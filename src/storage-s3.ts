import { S3 } from "aws-sdk";

import { Logger } from "./logger";
import { isDefined, StorageBase } from "./storage";

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

export class S3Storage extends StorageBase {
  private connection: S3;
  private bucketName: string;
  private logger: Logger;

  public constructor(connection: S3, bucketName: string, logger: Logger) {
    super();

    this.connection = connection;
    this.bucketName = bucketName;
    this.logger = logger;
  }

  public async createFolder(folder: string): Promise<void> {
    const params = {
      Bucket: this.bucketName,
      Key: `${folder}/`,
    };

    this.logger.debug(`Put object: ${JSON.stringify(params, null, 2)}`);

    await this.connection.putObject(params).promise();
  }

  protected async listDirectories(prefix?: string): Promise<string[]> {
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

  protected async listFiles(prefix?: string): Promise<string[]> {
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

  protected async readFileContents(
    filePath: string
  ): Promise<Buffer | undefined> {
    const object = await this.connection
      .getObject({
        Bucket: this.bucketName,
        Key: filePath,
      })
      .promise();

    return object.Body as Buffer | undefined;
  }

  protected async writeFileContents(
    filePath: string,
    contents: string
  ): Promise<void> {
    const params = {
      Bucket: this.bucketName,
      Key: filePath,
      ContentType: "application/json",
      Body: contents,
    };

    this.logger.debug(
      `Put object: ${JSON.stringify({ ...params, Body: "<DATA>" }, null, 2)}`
    );

    await this.connection.putObject(params).promise();
  }
}
