import {
  Bucket,
  File,
  GetFilesOptions,
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

  protected async listDirectories(prefix?: string): Promise<string[]> {
    const objects = await this.listObjects(prefix, "/");
    return objects.map((object) => object.name).map((x) => x.split("/")[0]);
  }

  protected async listFiles(prefix?: string): Promise<string[]> {
    const objects = await this.listObjects(prefix);
    return objects.map((object) => object.name);
  }

  protected async listObjects(
    prefix?: string,
    delimiter?: string
  ): Promise<File[]> {
    const result: File[] = [];

    let query: GetFilesOptions = {
      delimiter,
      includeTrailingDelimiter: !!delimiter,
      prefix,
      autoPaginate: false,
      maxResults: 20,
    };

    while (query) {
      this.logger.debug(`Get files: ${JSON.stringify(query, null, 2)}`);

      const [files, nextQuery] = await this.bucket.getFiles(query);

      result.push(...files);
      query = {
        ...query,
        ...nextQuery,
      };
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
