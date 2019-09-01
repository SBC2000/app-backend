import * as AWSMock from "mock-aws-s3";
import * as path from "path";

import { S3Storage, Storage } from "./storage";

beforeAll(() => {
  AWSMock.config.basePath = path.join(__dirname, "../data/buckets");
});

describe("when there is full data", () => {
  let storage: Storage;

  beforeEach(() => {
    storage = new S3Storage(new AWSMock.S3(), "full");
  });

  it("returns the latest database version", async () => {
    expect(await storage.getLatestFolderName()).toEqual("201900");
  });

  it("returns the latest data version", async () => {
    expect(await storage.getLatestFileName("201900", "databases")).toEqual(3);
  });

  it("returns the latest message version", async () => {
    expect(await storage.getLatestFileName("201900", "messages")).toEqual(1);
  });

  it("returns the latest result version", async () => {
    expect(await storage.getLatestFileName("201900", "results")).toEqual(2);
  });

  it("returns the latest sponsors version", async () => {
    expect(await storage.getLatestFileName("201900", "sponsors")).toEqual(2);
  });

  it("returns the latest data content", async () => {
    expect(await storage.getObjectFile("201900", "databases", 3)).toEqual({
      some: "data v3 2019",
    });
  });

  it("returns the latest message content", async () => {
    expect(await storage.getArrayFiles("201900", "messages", 0, 1)).toEqual([
      [{ message: "hello 2019" }],
    ]);
  });

  it("returns the latest results content", async () => {
    expect(await storage.getArrayFiles("201900", "results", 0, 2)).toEqual([
      [{ results: "1a 2019" }, { results: "1b 2019" }],
      [{ results: "2a 2019" }, { results: "2b 2019" }],
    ]);
  });

  it("returns the latest sponsors content", async () => {
    expect(await storage.getObjectFile("201900", "sponsors", 2)).toEqual({
      sponsors: "2 2019",
    });
  });

  it("returns a subset of messages", async () => {
    expect(await storage.getArrayFiles("201900", "messages", 1, 1)).toEqual([]);
  });

  it("returns a subset of results", async () => {
    expect(await storage.getArrayFiles("201900", "results", 1, 2)).toEqual([
      [{ results: "2a 2019" }, { results: "2b 2019" }],
    ]);
  });
});

describe("when there is some data", () => {
  let storage: Storage;

  beforeEach(() => {
    storage = new S3Storage(new AWSMock.S3(), "partial");
  });

  it("returns the latest database version", async () => {
    expect(await storage.getLatestFolderName()).toEqual("201900");
  });

  it("returns the latest data version", async () => {
    expect(await storage.getLatestFileName("201900", "databases")).toEqual(2);
  });

  it("returns the latest message version", async () => {
    expect(
      await storage.getLatestFileName("201900", "messages")
    ).toBeUndefined();
  });

  it("returns the latest result version", async () => {
    expect(await storage.getLatestFileName("201900", "results")).toEqual(1);
  });

  it("returns the latest sponsors version", async () => {
    expect(await storage.getLatestFileName("201900", "sponsors")).toEqual(1);
  });
});

describe("when there is no data", () => {
  let storage: Storage;

  beforeEach(() => {
    storage = new S3Storage(new AWSMock.S3(), "empty");
  });

  it("returns no folder", async () => {
    expect(await storage.getLatestFolderName()).toBeUndefined();
  });
});
