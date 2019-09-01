import { Cache, CacheHandler, Versions } from "./cache";
import { Storage } from "./storage";

describe("getNewerData", () => {
  describe("when the cache is not yet synchronized", () => {
    const cacheHandler = new CacheHandler(createStorage());

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
    let cacheHandler: CacheHandler;

    beforeEach(async () => {
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

      cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
    });

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
        messages: [],
        results: [],
        sponsors: {},
      };

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
        messages: [],
        results: [
          [{ one: "one" }, { two: "two" }],
          [{ one: "one1" }],
          [{ two: "two2" }],
          [{ one: "one2" }],
        ],
        sponsors: { other: "data" },
      };

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
        messages: [],
        results: [[{ two: "two2" }], [{ one: "one2" }]],
        sponsors: { other: "data" },
      };

      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });
  });

  describe("when storage has a database but no data", () => {
    let cacheHandler: CacheHandler;

    beforeEach(async () => {
      const storageContent: StorageContent = {
        latestFolderName: "20190001",
      };

      cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
    });

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
        messages: [],
        results: [],
        sponsors: {},
      };

      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });
  });

  describe("when storage has no database", () => {
    let cacheHandler: CacheHandler;

    beforeEach(async () => {
      const storageContent: StorageContent = {};

      cacheHandler = new CacheHandler(createStorage(storageContent));
      await cacheHandler.synchronize();
    });

    it("throws an exception when getting data", async () => {
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

describe("synchronize", () => {
  let storageContent: StorageContent;
  let cacheHandler: CacheHandler;

  beforeEach(async () => {
    storageContent = {
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

    cacheHandler = new CacheHandler(createStorage(storageContent));
    await cacheHandler.synchronize();
  });

  describe("when the data is updated", () => {
    beforeEach(async () => {
      // update underlying data
      storageContent.latestFileNames = {
        databases: 4, // increased
        messages: 3, // increased
        results: 4, // same
        sponsors: 2, // same
      };
      storageContent.data = {
        ...storageContent.data,
        databases: { some: "new data" },
        messages: [
          [{ message: "one a" }, { message: "one b" }],
          [{ message: "two" }],
          [{ message: "three" }],
        ],
      };

      await cacheHandler.synchronize();
    });

    it("contains the latest data", async () => {
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
          data: 4,
          messages: 3,
          results: 4,
          sponsors: 2,
        },
        data: { some: "new data" },
        messages: [
          [{ message: "one a" }, { message: "one b" }],
          [{ message: "two" }],
          [{ message: "three" }],
        ],
        results: [
          [{ one: "one" }, { two: "two" }],
          [{ one: "one1" }],
          [{ two: "two2" }],
          [{ one: "one2" }],
        ],
        sponsors: { other: "data" },
      };

      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
    });
  });

  describe("when the database version is increased", () => {
    beforeEach(async () => {
      // update underlying data
      storageContent.latestFolderName = "20190002";
      storageContent.latestFileNames = {
        databases: 1,
        messages: 1,
        results: 1,
        sponsors: 1,
      };
      storageContent.data = {
        databases: { some: "new data" },
        messages: [[{ message: "one a" }, { message: "one b" }]],
        results: [[{ some: "thing" }]],
        sponsors: { some: "sponsors" },
      };

      await cacheHandler.synchronize();
    });

    it("contains the latest data", async () => {
      const versions: Versions = {
        database: "20180004",
        data: 6,
        messages: 3,
        results: 2,
        sponsors: 8,
      };

      const expected: Cache = {
        versions: {
          database: "20190002",
          data: 1,
          messages: 1,
          results: 1,
          sponsors: 1,
        },
        data: { some: "new data" },
        messages: [[{ message: "one a" }, { message: "one b" }]],
        results: [[{ some: "thing" }]],
        sponsors: { some: "sponsors" },
      };

      const actual = cacheHandler.getNewerData(versions);

      expect(actual).toEqual(expected);
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
    getLatestFileName: async (_: string, subFolder: string) => {
      if (!content || !content.latestFileNames) {
        return;
      }

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
    getObjectFile: async (folder: string, subFolder: string) => {
      if (content && content.data) {
        if (subFolder === "databases" && content.data.databases) {
          return content.data.databases;
        }
        if (subFolder === "sponsors" && content.data.sponsors) {
          return content.data.sponsors;
        }
      }
      throw new Error(`Unknown folder ${folder}/${subFolder}`);
    },
    getArrayFiles: async (folder: string, subFolder: string) => {
      if (content && content.data) {
        if (subFolder === "messages" && content.data.messages) {
          return content.data.messages;
        }
        if (subFolder === "results" && content.data.results) {
          return content.data.results;
        }
      }
      throw new Error(`Unknown folder ${folder}/${subFolder}`);
    },
  };
}
