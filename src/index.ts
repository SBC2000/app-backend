import * as express from "express";
import * as http from "http";

import { CacheHandler, Versions } from "./cache";
import { createConsoleLogger } from "./logger";
import { S3Config, createS3Storage } from "./storage";

const logger = createConsoleLogger();

const config = readConfig(process.env);

bootstrap(config);

setInterval(() => {
  http.request(`${config.baseUrl}/synchronize`);
}, 1000 * 60 * 5);

async function bootstrap(config: Config): Promise<void> {
  logger.info("Application starting");

  const cacheHandler = new CacheHandler(
    createS3Storage(config.s3Config, logger),
    logger
  );

  await cacheHandler.synchronize();

  const app = express();

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
  s3Config: S3Config;
}

function readConfig(env: Record<string, string | undefined>): Config {
  const port = env.PORT;
  const baseUrl = env.BASE_URL;
  const accessKeyId = env.AWS_ACCESS_KEY_ID;
  const secretAccessKey = env.AWS_SECRET_ACCESS_KEY;
  const s3Bucket = env.AWS_S3_BUCKET;

  if (!port || !baseUrl || !accessKeyId || !secretAccessKey || !s3Bucket) {
    const missing = [
      "PORT",
      "BASEURL",
      "AWS_ACCESS_KEY_ID",
      "AWS_SECRET_ACCESS_KEY",
      "AWS_S3_BUCKET",
    ]
      .filter(key => !env[key])
      .join(", ");

    logger.error(missing);

    throw new Error(`Missing env vars: ${missing}`);
  }

  return {
    port,
    baseUrl,
    s3Config: { accessKeyId, secretAccessKey, s3Bucket },
  };
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

function flatten<T>(x: T[][]): T[] {
  return x.reduce((l, e) => [...l, ...e], []);
}

function maybeParseInt(s: string | undefined): number | undefined {
  return s ? parseInt(s, 10) : undefined;
}
