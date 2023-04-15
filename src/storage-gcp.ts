import {
  Bucket,
  GetFilesOptions,
  GetFilesResponse,
  Storage as StorageSdk,
} from "@google-cloud/storage";

import { Logger } from "./logger";
import { StorageBase, WritableStorage } from "./storage";

export interface GcpConfig {
  bucketName: string;
}

export function createGcpStorage(
  { bucketName }: GcpConfig,
  logger: Logger
): WritableStorage {
  return new GcpStorage(new StorageSdk().bucket(bucketName), logger);
}

export class GcpStorage extends StorageBase {
  private bucket: Bucket;
  private logger: Logger;

  public constructor(bucket: Bucket, logger: Logger) {
    super();

    this.bucket = bucket;
    this.logger = logger;
  }

  public async createFolder(folder: string): Promise<void> {
    const filePath = `${folder}/`;

    this.logger.debug(`Create folder: ${filePath}`);

    await this.bucket
      .file(filePath)
      .save("", { contentType: "application/x-www-form-urlencoded" });
  }

  protected listDirectories(prefix?: string): Promise<string[]> {
    return this.listObjects(prefix, "/", ([, , { prefixes }]) =>
      prefixes.map((prefix: string) =>
        prefix.endsWith("/") ? prefix.slice(0, -1) : prefix
      )
    );
  }

  protected listFiles(prefix?: string): Promise<string[]> {
    return this.listObjects(prefix, undefined, ([files]) =>
      files.map((file) => file.name)
    );
  }

  private async listObjects<T>(
    prefix: string | undefined,
    delimiter: string | undefined,
    handleResponse: (response: GetFilesResponse) => T[]
  ): Promise<T[]> {
    const result: T[] = [];

    let query: GetFilesOptions = {
      delimiter,
      includeTrailingDelimiter: !!delimiter,
      prefix,
      autoPaginate: false,
      maxResults: 20,
    };

    while (query) {
      this.logger.debug(`Get files: ${JSON.stringify(query, null, 2)}`);

      const response = await this.bucket.getFiles(query);

      result.push(...handleResponse(response));
      query = response[1];
    }

    return result;
  }

  protected async readFileContents(
    filePath: string
  ): Promise<Buffer | undefined> {
    const [buffer] = await this.bucket.file(filePath).download();

    return buffer;
  }

  protected async writeFileContents(
    filePath: string,
    contents: string
  ): Promise<void> {
    this.logger.debug(`Create file: ${filePath}`);

    await this.bucket
      .file(filePath)
      .save(contents, { contentType: "application/json" });
  }
}
