import { S3 } from "aws-sdk";

import { Logger } from "./logger";

export interface Storage {
  getLatestFolderName(): Promise<string | undefined>;
  getLatestFileName(
    folder: string,
    subFolder: string
  ): Promise<number | undefined>;
  getObjectFile(
    folder: string,
    subFolder: string,
    fileName: number
  ): Promise<object>;
  /**
   * Get all file contents between start and end. Not that contrary to common
   * practice start is excluded and end is included.
   * @param folder
   * @param subFolder
   * @param start
   * @param end
   */
  getArrayFiles(
    folder: string,
    subFolder: string,
    start: number,
    end: number
  ): Promise<object[][]>;
}

export interface WritableStorage extends Storage {
  createFolder(folder: string): Promise<void>;
  createSubFolder(folder: string, subFolder: string): Promise<void>;
  createFile(
    folder: string,
    subFolder: string,
    fileName: number,
    data: string
  ): Promise<void>;
}

// Naming convention: all json files are 4-digit numbers
const fileNameRegex = /.*\/(\d{4}).json/;

export interface S3Config {
  accessKeyId: string;
  secretAccessKey: string;
  s3Bucket: string;
}

export function createS3Storage(
  { accessKeyId, secretAccessKey, s3Bucket }: S3Config,
  logger: Logger
): S3Storage {
  return new S3Storage(
    new S3({ accessKeyId, secretAccessKey }),
    s3Bucket,
    logger
  );
}

export class S3Storage implements WritableStorage {
  private connection: S3;
  private bucketName: string;
  private logger: Logger;

  public constructor(connection: S3, bucketName: string, logger: Logger) {
    this.connection = connection;
    this.bucketName = bucketName;
    this.logger = logger;
  }

  public async getLatestFolderName(): Promise<string | undefined> {
    const directories = await this.listDirectories();
    return directories.pop();
  }

  public async getLatestFileName(
    folder: string,
    subFolder: string
  ): Promise<number | undefined> {
    const fileNames = await this.listFiles(`${folder}/${subFolder}`);
    if (fileNames.length === 0) {
      return;
    }

    for (const fileName of fileNames.reverse()) {
      const match = fileName.match(fileNameRegex);
      if (match) {
        return parseInt(match[1], 10);
      }
    }

    return;
  }

  public async getObjectFile(
    folder: string,
    subFolder: string,
    fileName: number,
    // We ignore parse errors for array data because otherwise we could never
    // recover from corrupted data. For object data we can just upload a new
    // version but for arrays we would retry loading until parsing succeeds.
    // Yes, this is a hack, but that applies to most of this project.
    ignoreParseErrors = false
  ): Promise<object> {
    const fullFileName = `0000${fileName}`.slice(-4);

    const object = await this.connection
      .getObject({
        Bucket: this.bucketName,
        Key: `${folder}/${subFolder}/${fullFileName}.json`,
      })
      .promise();

    if (!object.Body) {
      return {};
    }

    try {
      return JSON.parse(object.Body.toString());
    } catch (error: unknown) {
      if (ignoreParseErrors) {
        return {};
      }
      throw error;
    }
  }

  public async getArrayFiles(
    folder: string,
    subFolder: string,
    start: number,
    end: number
  ): Promise<object[][]> {
    const files = await Promise.all(
      [...Array(end - start).keys()]
        .map((x) => x + start + 1)
        .map((fileName) =>
          this.getObjectFile(folder, subFolder, fileName, true)
        )
    );

    return files
      .map((x) => (Array.isArray(x) ? (x as object[]) : undefined))
      .filter(isDefined);
  }

  public async createFolder(folder: string): Promise<void> {
    const params = {
      Bucket: this.bucketName,
      Key: `${folder}/`,
    };

    this.logger.debug(`Put object: ${JSON.stringify(params, null, 2)}`);

    await this.connection.putObject(params).promise();
  }

  public async createSubFolder(
    folder: string,
    subFolder: string
  ): Promise<void> {
    const params = {
      Bucket: this.bucketName,
      Key: `${folder}/${subFolder}/`,
    };

    this.logger.debug(`Put object: ${JSON.stringify(params, null, 2)}`);

    await this.connection.putObject(params).promise();
  }

  public async createFile(
    folder: string,
    subFolder: string,
    fileName: number,
    data: string
  ): Promise<void> {
    const fullFileName = `000${fileName}`.slice(-4);

    const params = {
      Bucket: this.bucketName,
      Key: `${folder}/${subFolder}/${fullFileName}.json`,
      ContentType: "application/json",
      Body: data,
    };

    this.logger.debug(
      `Put object: ${JSON.stringify({ ...params, Body: "<DATA>" }, null, 2)}`
    );

    await this.connection.putObject(params).promise();
  }

  private async listDirectories(prefix?: string): Promise<string[]> {
    const objects = await this.listObjects(prefix, "/");
    return objects
      .map((object) =>
        (object.CommonPrefixes || [])
          .map((x) => x.Prefix)
          .filter(isDefined)
          .map((x) => x.split("/")[0])
      )
      .reduce((result, list) => [...result, ...list], []);
  }

  private async listFiles(prefix?: string): Promise<string[]> {
    const objects = await this.listObjects(prefix);
    return objects
      .map((object) =>
        (object.Contents || []).map((x) => x.Key).filter(isDefined)
      )
      .reduce((result, list) => [...result, ...list], []);
  }

  private async listObjects(
    prefix?: string,
    delimiter?: string
  ): Promise<S3.ListObjectsOutput[]> {
    const results: S3.ListObjectsOutput[] = [];

    let isTruncated = true;
    let marker: string | undefined;

    while (isTruncated) {
      const params = {
        Bucket: this.bucketName,
        Delimiter: delimiter,
        Prefix: prefix,
        Marker: marker,
        MaxKeys: 20,
      };

      this.logger.debug(`List objects: ${JSON.stringify(params, null, 2)}`);

      const result = await this.connection.listObjects(params).promise();

      results.push(result);

      isTruncated = result.IsTruncated || false;
      // S3 only returns NextMarker when delimiter is specified.
      // Otherwise, use the last content key.
      marker = delimiter
        ? result.NextMarker
        : result.Contents &&
          (result.Contents.length > 0 || undefined) &&
          result.Contents[result.Contents.length - 1].Key;
    }

    return results;
  }
}

function isDefined<T>(t: T | undefined): t is T {
  return t !== undefined;
}
