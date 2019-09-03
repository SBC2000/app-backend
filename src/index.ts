import * as http from "http";

import { createConsoleLogger } from "./logger";
import { Server } from "./server";
import { S3Config, createS3Storage } from "./storage";

const logger = createConsoleLogger();
const config = readConfig(process.env);
const storage = createS3Storage(config.s3Config, logger);

async function bootstrap(): Promise<void> {
  logger.info("Application starting");

  const server = new Server(storage, config.password, logger);

  const app = await server.createApp();

  setInterval(async () => {
    try {
      await new Promise((resolve, reject) => {
        const request = http.request(
          `${config.baseUrl}/synchronize`,
          result => {
            const status = result.statusCode;
            if (status && status >= 200 && status <= 299) {
              resolve();
            } else {
              reject(status);
            }
          }
        );

        request.on("error", error => reject(error));
        request.end();
      });
    } catch (error) {
      logger.error(`Periodic synchronization failed: ${error}`);
    }
  }, 1000 * 60 * 5);

  app.listen(config.port, () => {
    logger.info(`App listening on port ${config.port}`);
  });
}

bootstrap();

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
