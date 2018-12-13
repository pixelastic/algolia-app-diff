import algolia from 'algoliasearch';
import path from 'path';
import fs from 'fs';
import dotenv from 'dotenv';
import { _, chalk, dayjs, pMap, spinner, firost } from 'golgoth';
dotenv.config();

const module = {
  configs: {
    mesos: {
      appId: process.env.MESOS_APP_ID,
      apiKey: process.env.MESOS_API_KEY,
      client: null,
    },
    kubernetes: {
      appId: process.env.KUBERNETES_APP_ID,
      apiKey: process.env.KUBERNETES_API_KEY,
      client: null,
    },
  },

  /**
   * Return an Algolia client for a given app.
   * @param {String} appName Name of the app, as defined in configs
   * @returns {Object} Algolia client object
   **/
  getClient(appName) {
    if (!this.configs[appName].client) {
      this.configs[appName].client = algolia(
        this.configs[appName].appId,
        this.configs[appName].apiKey
      );
    }
    return this.configs[appName].client;
  },

  /**
   * Download the list of indices of a given app to disk.
   * List contains recordCount, dataSize, fileSize and lastUpdate for each
   * record
   * @param {String} appName Name of the app, as defined in configs
   * @returns {Void}
   **/
  async downloadIndexList(appName) {
    const filePath = `./dist/apps/${appName}.json`;
    if (await firost.isFile(filePath)) {
      console.info(`✔ List of indices for ${appName} found in cache.`);
      return await firost.readJson(filePath);
    }
    const response = await this.getClient(appName).listIndexes();

    const indexData = _.chain(response)
      .get('items')
      .map(index => ({
        name: index.name,
        recordCount: index.entries,
        dataSize: index.dataSize,
        fileSize: index.fileSize,
        lastUpdate: index.updatedAt,
      }))
      .reject(index => _.endsWith(index.name, '_tmp'))
      .value();

    console.info(`Saving list of indices for ${appName} to cache`);

    await firost.writeJson(`./dist/apps/${appName}.json`, indexData);
    return indexData;
  },
  /**
   * Download an index with all its records to disk
   * The record list is normalized, objectID are removed, objects are always
   * returned in the same order, and thus can be diffed accross appas
   * @param {String} appName Name of the app, as defined in configs
   * @param {String} indexName Name of the index
   * @returns {Void}
   **/
  async getRecordsFromIndex(appName, indexName) {
    return await new Promise(async (resolve, reject) => {
      const client = await this.getClient(appName);
      const index = client.initIndex(indexName);
      const browser = index.browseAll('', {
        distinct: false,
        attributesToRetrieve: '*',
        hitsPerPage: 1000,
      });
      let result = [];
      browser.on('result', content => {
        result = result.concat(content.hits);
      });

      browser.on('end', () => {
        // Sort results, without their objectID. Makes diffing easier.
        const sortedResults = _.chain(result)
          .map(record => _.omit(record, ['objectID']))
          .sortBy([
            'url',
            'hierarchy.lvl0',
            'hierarchy.lvl1',
            'hierarchy.lvl2',
            'hierarchy.lvl3',
            'hierarchy.lvl4',
            'hierarchy.lvl5',
            'hierarchy.lvl6',
            'weight.position',
          ])
          .value();
        resolve(sortedResults);
      });

      browser.on('error', error => {
        reject(error);
      });
    });
  },

  /**
   * Returns all indices that have a different size between mesos and kubernetes
   * Requires you to have downloaded the list of indices on disk first
   * @returns {Void}
   **/
  async getIndicesWithDifferences() {
    const mesosIndices = await this.downloadIndexList('mesos');
    const kubernetesIndices = await this.downloadIndexList('kubernetes');
    function convertList(list) {
      return _.transform(
        list,
        (result, index) => {
          // eslint-disable-next-line no-param-reassign
          result[index.name] = index.dataSize;
        },
        {}
      );
    }

    const mesosList = convertList(mesosIndices);
    const kubernetesList = convertList(kubernetesIndices);

    return _.chain(mesosList)
      .map((size, name) => {
        const kubernetesSize = kubernetesList[name];
        if (kubernetesSize !== size) {
          return name;
        }
        return null;
      })
      .compact()
      .value();
  },

  /**
   * Compare list of indices for mesos and k8s and download on disk the indices
   * that don't have the same size
   * @returns {Void}
   **/
  async downloadIndicesWithDifferences() {
    const indices = await this.getIndicesWithDifferences();
    console.info(`Found ${indices.length} indices whose size does not match`);
    console.info('Downloading thoses indices to disk...');

    const progress = spinner(indices.length * 2);

    await pMap(
      indices,
      async indexName => {
        const apps = _.keys(this.configs);
        return await pMap(
          apps,
          async appName => {
            const filePath = `./dist/indices/${indexName}-${appName}.json`;
            if (await firost.isFile(filePath)) {
              progress.tick(`[${appName}] ${indexName}`);
              return;
            }

            try {
              const records = await this.getRecordsFromIndex(
                appName,
                indexName
              );
              await firost.writeJson(filePath, records);
              progress.tick(`[${appName}] ${indexName}`);
            } catch (error) {
              await firost.write(filePath, `ERROR: ${error.message}`);
              progress.tick(
                `[${appName}] ${chalk.red(indexName)} (${error.message})`
              );
            }
          },
          { concurrency: 1 }
        );
      },
      { concurrency: 1 }
    );

    progress.succeed(`All ${indices.length} indices downloaded on both apps`);
  },

  /**
   * Return the list of indices that haven't been updated in the past month.
   * Note: This is not very reliable in the case of DocSearch as even failed
   * crawls will have userData updated in the settings
   * @param {String} appName Name of the app, as defined in configs
   * @returns {Void}
   **/
  async getStaleIndices(appName) {
    const indices = await firost.readJson(`./dist/apps/${appName}.json`);
    return _.chain(indices)
      .sortBy(['name'])
      .filter(index => {
        const lastUpdate = dayjs(index.lastUpdate);
        const diff = dayjs().diff(lastUpdate, 'day');
        return diff >= 30;
      })
      .value();
  },

  /**
   * Return the list of indices that are different on mesos and kubernetes
   * @returns {Array}
   **/
  async getDifferentIndices() {
    const rawList = await firost.glob('./dist/indices/*.json');
    return (
      _.chain(rawList)
        // Transform into object of type/indexName
        .map(filepath => {
          const basename = path.basename(filepath, '.json');
          const split = basename.split('-');
          const type = _.last(split);
          const indexName = _.dropRight(split, 1).join('-');
          return {
            type,
            indexName,
          };
        })
        // Set each index into a key for kubernetes/mesos
        .transform(
          (result, value) => {
            result[value.type].push(value.indexName);
          },
          { kubernetes: [], mesos: [] }
        )
        // Remove indices with errors
        .thru(indices => {
          const results = [];
          _.each(indices.kubernetes, indexName => {
            if (!indexName) {
              return;
            }
            const kubeSize = fs.statSync(
              `./dist/indices/${indexName}-kubernetes.json`
            );
            const mesosSize = fs.statSync(
              `./dist/indices/${indexName}-mesos.json`
            );
            if (kubeSize.size < 100 || kubeSize === mesosSize) {
              return;
            }
            results.push(indexName);
          });

          return results;
        })
        .value()
    );
  },
};

(async function() {
  await module.downloadIndicesWithDifferences();
  const diff = await module.getDifferentIndices();
  console.info(
    'The following indices have different content on mesos and kubernetes:'
  );
  console.info(diff);
})();
