import { Storage } from "./storage";

export class CacheHandler {
  private initialized: boolean;
  private storage: Storage;
  private cache: Cache;

  public constructor(storage: Storage) {
    this.initialized = false;
    this.storage = storage;
    this.cache = {
      versions: {
        database: "",
        data: 0,
        messages: 0,
        results: 0,
        sponsors: 0,
      },
      data: {},
      messages: [],
      results: [],
      sponsors: {},
    };
  }

  public async synchronize(): Promise<void> {
    const databaseVersion = await this.storage.getLatestFolderName();

    if (!databaseVersion) {
      console.warn("No database found");
      return;
    }

    const [
      dataVersion,
      messagesVersion,
      resultsVersion,
      sponsorsVersion,
    ] = await Promise.all(
      ["databases", "messages", "results", "sponsors"].map(folder =>
        this.getVersionFromStorage(databaseVersion, folder)
      )
    );

    const { versions } = this.cache;

    const [data, messages, results, sponsors] = await Promise.all([
      this.getObjectFromStorage(
        databaseVersion,
        "databases",
        dataVersion,
        versions.data,
        this.cache.data
      ),
      this.getArraysFromStorage(
        databaseVersion,
        "messages",
        messagesVersion,
        versions.messages,
        this.cache.messages
      ),
      this.getArraysFromStorage(
        databaseVersion,
        "results",
        resultsVersion,
        versions.results,
        this.cache.results
      ),
      this.getObjectFromStorage(
        databaseVersion,
        "sponsors",
        sponsorsVersion,
        versions.sponsors,
        this.cache.sponsors
      ),
    ]);

    this.cache = {
      versions: {
        database: databaseVersion,
        data: dataVersion,
        messages: messagesVersion,
        results: resultsVersion,
        sponsors: sponsorsVersion,
      },
      data,
      messages,
      results,
      sponsors,
    };

    this.initialized = true;
  }

  /**
   * Returns all data in the cache newer than `previousVersions`.
   * @param previousVersions
   */
  public getNewerData(previousVersions: Versions): Cache {
    if (!this.initialized) {
      throw new Error("Cache is not initialized yet");
    }

    const { versions, data, results, messages, sponsors } = this.cache;

    // The client had a completely outdated database:
    // Return the whole new cache.
    if (previousVersions.database.localeCompare(versions.database) < 0) {
      return this.cache;
    }

    // Return newer data for each property separately.
    return {
      versions,
      data: this.getObjectNewerThan(previousVersions.data, versions.data, data),
      messages: this.getArrayNewerThan(previousVersions.messages, messages),
      results: this.getArrayNewerThan(previousVersions.results, results),
      sponsors: this.getObjectNewerThan(
        previousVersions.sponsors,
        versions.sponsors,
        sponsors
      ),
    };
  }

  private async getVersionFromStorage(
    databaseVersion: string,
    folder: string
  ): Promise<number> {
    const fileName = await this.storage.getLatestFileName(
      `${databaseVersion}/${folder}`
    );
    return fileName || 0;
  }

  private async getObjectFromStorage(
    databaseVersion: string,
    folder: string,
    newVersion: number,
    currentVersion: number,
    currentData: object
  ): Promise<object> {
    // We already have the latest version: return that data.
    if (
      databaseVersion === this.cache.versions.database &&
      newVersion === currentVersion
    ) {
      return currentData;
    }

    // We need no data: return empty object
    if (newVersion === 0) {
      return {};
    }

    // Otherwise fetch and return the new data.
    return await this.storage.getObjectFile(
      `${databaseVersion}/${folder}`,
      newVersion
    );
  }

  private async getArraysFromStorage(
    databaseVersion: string,
    folder: string,
    newVersion: number,
    currentVersion: number,
    currentData: object[][]
  ): Promise<object[][]> {
    // We already have the latest version: return empty array.
    if (
      databaseVersion === this.cache.versions.database &&
      newVersion === currentVersion
    ) {
      return currentData;
    }

    // We need no data: return empty array
    if (newVersion === 0) {
      return [];
    }

    const newData = await this.storage.getArrayFiles(
      `${databaseVersion}/${folder}`,
      currentVersion,
      newVersion
    );

    // If we are still working on the same database: concatenate with existing data.
    // Otherwise: return the data for the new database only.
    return databaseVersion === this.cache.versions.database
      ? [...currentData, ...newData]
      : newData;
  }

  // Object data is either all or nothing. When newer: all, otherwise: nothing.
  private getObjectNewerThan(
    previousVersion: number,
    version: number,
    data: object
  ): object {
    return previousVersion < version ? data : {};
  }

  // Array data can be partial. When newer: return the new entries, otherwise: nothing.
  private getArrayNewerThan(
    previousVersion: number,
    data: object[][]
  ): object[][] {
    return previousVersion < data.length ? data.slice(previousVersion) : [];
  }
}

// All these versions are needed to support custom caching because regular caching is too mainstream.
export interface Versions {
  database: string;
  data: number;
  messages: number;
  results: number;
  sponsors: number;
}

export interface Cache {
  versions: Versions;

  // All these properties are objects because we don't really care.
  // Someone with access rights uploads data to S3 and we serve it to the clients.
  // We could try to validate it but it doesn't bring much value at this point.
  data: object;
  messages: object[][];
  results: object[][];
  sponsors: object;
}