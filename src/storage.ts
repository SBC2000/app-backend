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
