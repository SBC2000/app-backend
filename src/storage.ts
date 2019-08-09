export interface Storage {
  getLatestFolderName(): Promise<string | undefined>;
  getLatestFileName(folder: string): Promise<number | undefined>;
  getObjectFile(folder: string, fileName: number): Promise<object>;
  getArrayFiles(
    folder: string,
    start: number,
    end: number
  ): Promise<object[][]>;
}
