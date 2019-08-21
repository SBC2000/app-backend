import { S3 } from "aws-sdk";

export interface Storage {
  getLatestFolderName(): Promise<string | undefined>;
  getLatestFileName(folder: string): Promise<number | undefined>;
  getObjectFile(folder: string, fileName: number): Promise<object>;
  /**
   * Get all file contents between start and end. Not that contrary to common
   * practice start is excluded and end is included.
   * @param folder
   * @param start
   * @param end
   */
  getArrayFiles(
    folder: string,
    start: number,
    end: number
  ): Promise<object[][]>;
}

// Naming convention: all json files are 4-digit numbers
const fileNameRegex = /.*\/(\d{4}).json/;

export interface S3Config {
  accessKeyId: string;
  secretAccessKey: string;
  s3Bucket: string;
}

export class S3Storage implements Storage {
  private bucketName: string;
  private connection: S3;

  public constructor({ accessKeyId, secretAccessKey, s3Bucket }: S3Config) {
    this.bucketName = s3Bucket;
    this.connection = new S3({ accessKeyId, secretAccessKey });
  }

  public async getLatestFolderName(): Promise<string | undefined> {
    const directories = await this.listDirectories();
    return directories.pop();
  }

  public async getLatestFileName(folder: string): Promise<number | undefined> {
    const fileNames = await this.listFiles(folder);
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
    fileName: number
  ): Promise<object> {
    const fullFileName = `0000${fileName}`.slice(-4);

    const object = await this.connection
      .getObject({
        Bucket: this.bucketName,
        Key: `${folder}/${fullFileName}.json`,
      })
      .promise();

    if (!object.Body) {
      return {};
    }

    return JSON.parse(object.Body.toString());
  }

  public async getArrayFiles(
    folder: string,
    start: number,
    end: number
  ): Promise<object[][]> {
    const files = await Promise.all(
      [...Array(end - start).keys()]
        .map(x => x + start + 1)
        .map(fileName => this.getObjectFile(folder, fileName))
    );

    return files
      .map(x => (Array.isArray(x) ? (x as object[]) : undefined))
      .filter(isDefined);
  }

  private async listDirectories(prefix?: string): Promise<string[]> {
    const objects = await this.listObjects(prefix, "/");
    return objects
      .map(object =>
        (object.CommonPrefixes || []).map(x => x.Prefix).filter(isDefined)
      )
      .reduce((result, list) => [...result, ...list], []);
  }

  private async listFiles(prefix?: string): Promise<string[]> {
    const objects = await this.listObjects(prefix);
    return objects
      .map(object => (object.Contents || []).map(x => x.Key).filter(isDefined))
      .reduce((result, list) => [...result, ...list], []);
  }

  private async listObjects(
    prefix?: string,
    delimiter?: string
  ): Promise<S3.ListObjectsOutput[]> {
    const results: S3.ListObjectsOutput[] = [];

    do {
      const marker =
        results.length > 0 ? results[results.length - 1].NextMarker : undefined;

      results.push(
        await this.connection
          .listObjects({
            Bucket: this.bucketName,
            Delimiter: delimiter,
            Prefix: prefix,
            Marker: marker,
            MaxKeys: 20,
          })
          .promise()
      );
    } while (results[results.length - 1].IsTruncated);

    return results;
  }
}

function isDefined<T>(t: T | undefined): t is T {
  return t !== undefined;
}
