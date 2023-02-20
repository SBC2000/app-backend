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

export abstract class StorageBase implements WritableStorage {
  public async getLatestFolderName(): Promise<string | undefined> {
    const directories = await this.listDirectories();
    return directories.pop();
  }

  public async getLatestFileName(
    folder: string,
    subFolder: string
  ): Promise<number | undefined> {
    const fileNames = await this.listFiles(`${folder}/${subFolder}`);

    return fileNames
      .map((fileName) => fileName.match(fileNameRegex)?.[1])
      .filter(isDefined)
      .map((match) => parseInt(match, 10))
      .filter((n) => !isNaN(n))
      .sort((a, b) => b - a)[0];
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

    const buffer = await this.readFileContents(
      `${folder}/${subFolder}/${fullFileName}.json`
    );

    if (!buffer) {
      return {};
    }

    try {
      return JSON.parse(buffer.toString("utf-8"));
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
      [...Array(end - start)]
        .map((_, x) => x + start + 1)
        .map((fileName) =>
          this.getObjectFile(folder, subFolder, fileName, true)
        )
    );

    return files
      .map((x) => (Array.isArray(x) ? (x as object[]) : undefined))
      .filter(isDefined);
  }

  public async createSubFolder(
    folder: string,
    subFolder: string
  ): Promise<void> {
    return this.createFolder(`${folder}/${subFolder}`);
  }

  public async createFile(
    folder: string,
    subFolder: string,
    fileName: number,
    data: string
  ): Promise<void> {
    const fullFileName = `000${fileName}`.slice(-4);

    await this.writeFileContents(
      `${folder}/${subFolder}/${fullFileName}.json`,
      data
    );
  }

  public abstract createFolder(folder: string): Promise<void>;

  protected abstract listDirectories(prefix?: string): Promise<string[]>;

  protected abstract listFiles(prefix?: string): Promise<string[]>;

  protected abstract readFileContents(
    filePath: string
  ): Promise<Buffer | undefined>;

  protected abstract writeFileContents(
    filePath: string,
    contents: string
  ): Promise<void>;
}

export function isDefined<T>(t: T | undefined): t is T {
  return t !== undefined;
}
