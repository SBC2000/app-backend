import express from "express";
import { createProxyMiddleware } from "http-proxy-middleware";

import { CacheHandler, Versions } from "./cache";
import { Logger } from "./logger";
import { WritableStorage } from "./storage";

export class Server {
  private cacheHandler: CacheHandler;
  private storage: WritableStorage;
  private password: string;
  private frontendUrl: string;
  private logger: Logger;

  private throttle = false;

  public constructor(
    storage: WritableStorage,
    password: string,
    frontendUrl: string,
    logger: Logger
  ) {
    this.cacheHandler = new CacheHandler(storage, logger);
    this.storage = storage;
    this.password = password;
    this.frontendUrl = frontendUrl;
    this.logger = logger;
  }

  public async createApp(): Promise<express.Express> {
    await this.cacheHandler.synchronize();

    const app = express();
    app.use(express.urlencoded({ extended: true }));
    app.use(express.json());

    app.get("/getData.php", this.getData);
    app.post("/upload.php", this.upload);
    app.post("/createNewVersion", this.createNewVersion);
    app.post("/synchronize", this.synchronize);

    app.use(
      "/",
      createProxyMiddleware({
        target: this.frontendUrl,
        changeOrigin: true,
        // map anything that is not a file to index.html
        pathRewrite: (path) => (/\.[^\\]+$/.test(path) ? path : "index.html"),
      })
    );

    return app;
  }

  private getData = (
    request: express.Request<unknown, unknown, unknown, VersionsQuery>,
    response: express.Response
  ): void => {
    this.logger.debug(
      `GET getData.php:\n${JSON.stringify(request.query, null, 2)}`
    );

    const previousVersions = this.parseVersions(request.query);
    if (!previousVersions) {
      response.sendStatus(400);
      return;
    }

    try {
      const cache = this.cacheHandler.getNewerData(previousVersions);

      response.json({
        ...this.formatVersions(cache.versions, previousVersions.database),
        ...cache.data,
        ...cache.sponsors,
        messages: this.flatten(cache.messages),
        results: this.flatten(cache.results),
      });
    } catch {
      response.sendStatus(500);
    }
  };

  private upload = async (
    request: express.Request<unknown, unknown, UploadBody, unknown>,
    response: express.Response
  ): Promise<void> => {
    const body = { ...request.body };
    if (body.password) {
      body.password = "****";
    }

    this.logger.debug(`Upload: ${JSON.stringify(body, null, 2)}`);

    if (!request.body.password) {
      response.sendStatus(401);
      return;
    }

    if (request.body.password !== this.password) {
      response.sendStatus(403);
      return;
    }

    const type = request.body.type;

    // Note that the types are "database" and "message", so singular.
    // Consistency is overrated.
    if (
      !type ||
      !["database", "message", "results", "sponsors"].includes(type)
    ) {
      response.status(400).send("Unknown data type");
      return;
    }

    try {
      // There is a race condition here. If someone creates a new version
      // while uploading data, the data ends up in the wrong folder.
      // Fortunately, both requests are sent by the same person and he is
      // not that fast.
      const folder = await this.storage.getLatestFolderName();
      if (!folder) {
        response.status(500).send("No database to upload into");
        return;
      }

      this.logger.debug(`Upload database found: ${folder}`);

      // The upload script sends "almost json" but without the outer {} or [].
      // That was once handy in PHP-land (so merging objects and arrays can
      // be done using concatenating strings) but also was a very bad idea.
      const data =
        type === "database" || request.body.type === "sponsors"
          ? `{${request.body.data ?? ""}}`
          : `[${request.body.data ?? ""}]`;

      const subFolder =
        type === "database" || type === "message" ? `${type}s` : type;

      // Another race condition here: if someone calls this endpoint twice the
      // first file will be overwritten by the second. Same guy, still not that fast.
      const currentNumber =
        (await this.storage.getLatestFileName(folder, subFolder)) || 0;

      this.logger.debug(
        `Current version number for ${folder}/${subFolder}: ${currentNumber}`
      );

      await this.storage.createFile(folder, subFolder, currentNumber + 1, data);

      this.logger.debug(`Version ${currentNumber + 1} created`);

      // load the new data into cache
      await this.cacheHandler.synchronize();

      this.logger.debug("Upload synchronization done");

      response.sendStatus(200);
    } catch (error: unknown) {
      this.logger.error(`Failed to upload ${type}: ${error}`);
      response.sendStatus(500);
    }
  };

  private createNewVersion = async (
    request: express.Request<unknown, unknown, AuthenticatedBody, unknown>,
    response: express.Response
  ): Promise<void> => {
    const requestPassword = request.body.password;

    if (!requestPassword) {
      response.sendStatus(401);
      return;
    }

    if (requestPassword !== this.password) {
      response.sendStatus(403);
      return;
    }

    try {
      const currentVersion = await this.storage.getLatestFolderName();
      const newVersion = this.getNewVersion(currentVersion);

      await this.storage.createFolder(newVersion);
      await Promise.all(
        ["databases", "messages", "results", "sponsors"].map((subFolder) =>
          this.storage.createSubFolder(newVersion, subFolder)
        )
      );

      // load the new data into cache
      await this.cacheHandler.synchronize();
      response.sendStatus(200);
    } catch (error: unknown) {
      this.logger.error(`Create new version failed: ${error}`);
      response.sendStatus(500);
    }
  };

  private synchronize = async (
    request: express.Request,
    response: express.Response
  ): Promise<void> => {
    this.logger.debug("POST synchronize");

    if (this.throttle) {
      response.sendStatus(503);
      return;
    }

    try {
      // poor man's rate limiting, probably good enough in practice
      this.throttle = true;
      setTimeout(() => {
        this.throttle = false;
      }, 1000 * 60);

      await this.cacheHandler.synchronize();
      response.sendStatus(200);
    } catch {
      response.sendStatus(500);
    }
  };

  private parseVersions = (query: VersionsQuery): Versions | undefined => {
    const database = query.databaseversion;
    const data = this.maybeParseInt(query.dataversion);
    const messages = this.maybeParseInt(query.messageversion);
    const results = this.maybeParseInt(query.resultversion);
    const sponsors = this.maybeParseInt(query.sponsorsversion);

    if (
      database === undefined ||
      data === undefined ||
      messages === undefined ||
      results === undefined ||
      sponsors === undefined
    ) {
      return;
    }

    return { database, data, messages, results, sponsors };
  };

  private formatVersions = (
    versions: Versions,
    previousDatabaseVersion: string
  ): object => {
    // Note that the casing of the keys is different from the input and that
    // messages and results are in singular. This is not a typo.
    // Also, the newDatabaseVersion field name is pretty misleading.
    return {
      databaseVersion: versions.database,
      dataVersion: versions.data,
      messageVersion: versions.messages,
      resultVersion: versions.results,
      sponsorsVersion: versions.sponsors,
      newDatabaseVersion:
        versions.database === previousDatabaseVersion ? "false" : "true",
    };
  };

  // version numbers are the year plus a 2-digit order number
  private getNewVersion = (currentVersion: string | undefined): string => {
    const currentYear = `${new Date(Date.now()).getFullYear()}`;

    let versionNumber = 0;
    if (currentVersion && currentVersion.startsWith(currentYear)) {
      const currentVersionNumber = parseInt(currentVersion.substring(4), 10);
      if (currentVersionNumber >= 99) {
        throw new Error("Version number cannot be greater than 99");
      }
      versionNumber = currentVersionNumber + 1;
    }

    const versionNumberString = `0${versionNumber}`.slice(-2);

    return `${currentYear}${versionNumberString}`;
  };

  private flatten = <T>(x: T[][]): T[] => x.reduce((l, e) => [...l, ...e], []);

  private maybeParseInt = (s: string | undefined): number | undefined => {
    if (!s) {
      return;
    }

    const number = parseInt(s, 10);
    return isNaN(number) ? undefined : number;
  };
}

interface VersionsQuery {
  databaseversion?: string;
  dataversion?: string;
  messageversion?: string;
  resultversion?: string;
  sponsorsversion?: string;
}

interface AuthenticatedBody {
  password?: string;
}

interface UploadBody extends AuthenticatedBody {
  type?: "database" | "message" | "results" | "sponsors";
  data?: string;
}
