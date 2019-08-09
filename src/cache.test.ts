import { Cache, CacheHandler, Versions } from "./cache";
import { Storage } from "./storage";

describe("getNewerData", () => {
  describe("when the cache is not yet synchronized", () => {
    const storageContent = undefined;

    const cacheHandler = new CacheHandler(createStorage(storageContent));

    it("throws an error when getting data", () => {
      expect(() => {
        cacheHandler.getNewerData({
          database: "something",
          data: 5,
          messages: 3,
          results: 4,
          sponsors: 8,
        });
      }).toThrow();
    });
  });

  describe("when storage has data", () => {
    const storageContent: StorageContent = {
      latestFolderName: "20190001",
      latestFileNames: {
        databases: 3,
        messages: undefined,
        results: 4,
        sponsors: 2,
      },
      data: {
        databases: { some: "data" },
        results: [
          [{ one: "one" }, { two: "two" }],
          [{ one: "one1" }],
          [{ two: "two2" }],
          [{ one: "one2" }],
        ],
        sponsors: { other: "data" },
      },
    };

    it("returns current versions and empty data when calling with current versions", async () => {
      const versions: Versions = {
        database: "20190001",
        data: 3,
        messages: 0,
        results: 4,
        sponsors: 2,
      };

      const expected: Cache = {
        versions,
        data: {},
        messages: [[]],
        results: [[]],
        sponsors: {},
      };

      const cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });

    it("returns full cache when calling with an old database version", async () => {
      const versions: Versions = {
        database: "20180004",
        data: 6,
        messages: 3,
        results: 2,
        sponsors: 8,
      };

      const expected: Cache = {
        versions: {
          database: "20190001",
          data: 3,
          messages: 0,
          results: 4,
          sponsors: 2,
        },
        data: { some: "data" },
        messages: [[]],
        results: [
          [{ one: "one" }, { two: "two" }],
          [{ one: "one1" }],
          [{ two: "two2" }],
          [{ one: "one2" }],
        ],
        sponsors: { other: "data" },
      };

      const cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });

    it("returns partial data when calling with older versions", async () => {
      const versions: Versions = {
        database: "20190001",
        data: 3,
        messages: 0,
        results: 2,
        sponsors: 1,
      };

      const expected: Cache = {
        versions: {
          database: "20190001",
          data: 3,
          messages: 0,
          results: 4,
          sponsors: 2,
        },
        data: {},
        messages: [[]],
        results: [[{ two: "two2" }], [{ one: "one2" }]],
        sponsors: { other: "data" },
      };

      const cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });
  });

  describe("when storage has a database but no data", () => {
    const storageContent: StorageContent = {
      latestFolderName: "20190001",
    };

    it("returns empty data", async () => {
      const versions: Versions = {
        database: "20180003",
        data: 9,
        messages: 2,
        results: 15,
        sponsors: 8,
      };

      const expected: Cache = {
        versions: {
          database: "20190001",
          data: 0,
          messages: 0,
          results: 0,
          sponsors: 0,
        },
        data: {},
        messages: [[]],
        results: [[]],
        sponsors: {},
      };

      const cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });
  });

  describe("when storage has no database", () => {
    const storageContent: StorageContent = {};

    it("throws an exception when getting data", async () => {
      const cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();

      expect(() => {
        cacheHandler.getNewerData({
          database: "something",
          data: 5,
          messages: 3,
          results: 4,
          sponsors: 8,
        });
      }).toThrow();
    });
  });
});

interface StorageContent {
  latestFolderName?: string;
  latestFileNames?: {
    databases?: number;
    messages?: number;
    results?: number;
    sponsors?: number;
  };
  data?: {
    databases?: object;
    messages?: object[][];
    results?: object[][];
    sponsors?: object;
  };
}

function createStorage(content?: StorageContent): Storage {
  return {
    getLatestFolderName: async () => content && content.latestFolderName,
    getLatestFileName: async (folder: string) => {
      if (!content || !content.latestFileNames) {
        return;
      }

      const subFolder = folder.split("/").pop();

      switch (subFolder) {
        case "databases":
          return content.latestFileNames.databases;
        case "messages":
          return content.latestFileNames.messages;
        case "results":
          return content.latestFileNames.results;
        case "sponsors":
          return content.latestFileNames.sponsors;
        default:
          return;
      }
    },
    getObjectFile: async (folder: string) => {
      const subFolder = folder.split("/").pop();
      if (content && content.data) {
        if (subFolder === "databases" && content.data.databases) {
          return content.data.databases;
        }
        if (subFolder === "sponsors" && content.data.sponsors) {
          return content.data.sponsors;
        }
      }
      throw new Error(`Unknown folder ${folder}`);
    },
    getArrayFiles: async (folder: string) => {
      const subFolder = folder.split("/").pop();
      if (content && content.data) {
        if (subFolder === "messages" && content.data.messages) {
          return content.data.messages;
        }
        if (subFolder === "results" && content.data.results) {
          return content.data.results;
        }
      }
      throw new Error(`Unknown folder ${folder}`);
    },
  };
}
