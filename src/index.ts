import * as bodyParser from "body-parser";
import * as express from "express";
import * as http from "http";

import { CacheHandler, Versions } from "./cache";
import { createConsoleLogger } from "./logger";
import { S3Config, createS3Storage } from "./storage";

const logger = createConsoleLogger();

const config = readConfig(process.env);

bootstrap(config);

setInterval(() => {
  try {
    http.request(`${config.baseUrl}/synchronize`);
  } catch (error) {
    logger.error(`Periodic synchronization failed: ${error}`);
  }
}, 1000 * 60 * 5);

async function bootstrap(config: Config): Promise<void> {
  logger.info("Application starting");

  const storage = createS3Storage(config.s3Config, logger);
  const cacheHandler = new CacheHandler(storage, logger);

  await cacheHandler.synchronize();

  const app = express();
  app.use(bodyParser.json());

  app.get("/getData.php", (req, res) => {
    logger.info(`GET getData.php:\n${JSON.stringify(req.query, null, 2)}`);

    const previousVersions = parseVersions(req.query);
    if (!previousVersions) {
      return res.sendStatus(400);
    }

    try {
      const cache = cacheHandler.getNewerData(previousVersions);

      return res.json({
        ...formatVersions(cache.versions, previousVersions.database),
        ...cache.data,
        ...cache.sponsors,
        messages: flatten(cache.messages),
        results: flatten(cache.results),
      });
    } catch {
      return res.sendStatus(500);
    }
  });

  app.post("/upload.php", async (req, res) => {
    if (!req.body.password) {
      return res.sendStatus(401);
    }

    if (req.body.password !== config.password) {
      return res.sendStatus(403);
    }

    const type = req.body.type;

    // Note that the types are "database" and "message", so singular.
    // Consistency is overrated.
    if (!["database", "message", "results", "sponsors"].includes(type)) {
      return res.status(400).send("Unknown data type");
    }

    try {
      // There is a race condition here. If someone creates a new version
      // while uploading data, the data ends up in the wrong folder.
      // Fortunately, both requests are send by the same person and he is
      // not that fast.
      const folder = await storage.getLatestFolderName();
      if (!folder) {
        return res.status(500).send("No database to upload into.");
      }

      // The upload script sends "almost json" but without the outer {} or [].
      // That was once handy in PHP-land (so merging objects and arrays can
      // be done using concatenating strings) but also was a very bad idea.
      const data =
        type === "database" || req.body.type === "sponsors"
          ? `{${req.body.data || ""}}`
          : `[${req.body.data || ""}]`;

      const subFolder =
        type === "database" || type === "message" ? `${type}s` : type;

      // Another race condition here: if someone calls this endpoint twice the
      // first file will be overwritten by the second. Same guy, still not that fast.
      const currentNumber =
        (await storage.getLatestFileName(folder, subFolder)) || 0;

      await storage.createFile(folder, subFolder, currentNumber + 1, data);
    } catch (error) {
      logger.error(`Failed to upload ${type}: ${error}`);
    }
  });

  app.post("/createNewVersion", async (req, res) => {
    if (!req.body.password) {
      return res.sendStatus(401);
    }

    if (req.body.password !== config.password) {
      return res.sendStatus(403);
    }

    try {
      const currentVersion = await storage.getLatestFolderName();
      const newVersion = getNewVersion(currentVersion);

      await storage.createFolder(newVersion);
      await Promise.all(
        ["databases", "messages", "results", "sponsors"].map(subFolder =>
          storage.createSubFolder(newVersion, subFolder)
        )
      );
    } catch (error) {
      logger.error(`Create new version failed: ${error}`);
    }
  });

  let throttle = false;
  app.post("/synchronize", async (_, res) => {
    logger.info("POST synchronize");

    if (throttle) {
      return res.sendStatus(503);
    }

    try {
      // poor man's rate limiting, probably good enough in practice
      throttle = true;
      setTimeout(() => {
        throttle = false;
      }, 1000 * 60);

      await cacheHandler.synchronize();
      res.sendStatus(200);
    } catch {
      res.sendStatus(500);
    }
  });

  app.listen(config.port, () => {
    logger.info(`App listening on port ${config.port}`);
  });
}

interface Config {
  port: string;
  baseUrl: string;
  password: string;
  s3Config: S3Config;
}

function readConfig(env: Record<string, string | undefined>): Config {
  const missing: string[] = [];
  const readEnvVar = (varName: string): string => {
    const envVar = env[varName];

    if (!envVar) {
      missing.push(varName);
      return "";
    }
    return envVar;
  };

  const config = {
    port: readEnvVar("PORT"),
    baseUrl: readEnvVar("BASE_URL"),
    password: readEnvVar("PASSWORD"),
    s3Config: {
      accessKeyId: readEnvVar("AWS_ACCESS_KEY_ID"),
      secretAccessKey: readEnvVar("AWS_SECRET_ACCESS_KEY"),
      s3Bucket: readEnvVar("AWS_S3_BUCKET"),
    },
  };

  if (missing.length > 0) {
    throw new Error(`Missing env vars: ${missing.join(", ")}`);
  }

  return config;
}

function parseVersions(
  query: Record<string, string | undefined>
): Versions | undefined {
  const database = query.databaseversion;
  const data = maybeParseInt(query.dataversion);
  const messages = maybeParseInt(query.messageversion);
  const results = maybeParseInt(query.resultversion);
  const sponsors = maybeParseInt(query.sponsorsversion);

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
}

function formatVersions(
  versions: Versions,
  previousDatabaseVersion: string
): object {
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
}

// version number are the year plus a 2-digit order number
function getNewVersion(currentVersion: string | undefined): string {
  const currentYear = `${new Date().getFullYear()}`;

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
}

function flatten<T>(x: T[][]): T[] {
  return x.reduce((l, e) => [...l, ...e], []);
}

function maybeParseInt(s: string | undefined): number | undefined {
  return s ? parseInt(s, 10) : undefined;
}
