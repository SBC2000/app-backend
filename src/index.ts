import { createLogger, LogLevel } from "./logger";
import { Server } from "./server";
import { createGcpStorage, GcpConfig } from "./storage-gcp";
import { S3Config, createS3Storage } from "./storage-s3";

const config = readConfig(process.env);
const logger = createLogger(config.logLevel);
const storage = (() => {
  switch (config.storageConfig.type) {
    case "aws":
      return createS3Storage(config.storageConfig.config, logger);
    case "gcp":
      return createGcpStorage(config.storageConfig.config, logger);
  }
})();

async function bootstrap(): Promise<void> {
  logger.info("Application starting");

  const server = new Server(
    storage,
    config.password,
    config.frontendUrl,
    logger
  );

  const app = await server.createApp();

  app.listen(config.port, () => {
    logger.info(`App listening on port ${config.port}`);
  });
}

bootstrap();

interface Config {
  port: string;
  password: string;
  frontendUrl: string;
  logLevel: LogLevel;
  storageConfig: StorageConfig;
}

type StorageConfig =
  | { type: "aws"; config: S3Config }
  | { type: "gcp"; config: GcpConfig };

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

  const logLevelString = readEnvVar("LOG_LEVEL");
  const logLevel: LogLevel =
    logLevelString === "debug" ||
    logLevelString === "info" ||
    logLevelString === "warning" ||
    logLevelString === "error"
      ? logLevelString
      : "info";

  const storageConfig: StorageConfig = (() => {
    const storageType = readEnvVar("STORAGE_TYPE");
    switch (storageType) {
      case "aws":
        return {
          type: "aws",
          config: {
            accessKeyId: readEnvVar("AWS_ACCESS_KEY_ID"),
            secretAccessKey: readEnvVar("AWS_SECRET_ACCESS_KEY"),
            s3Bucket: readEnvVar("AWS_S3_BUCKET"),
          },
        };
      case "gcp":
        return {
          type: "gcp",
          config: {
            bucketName: readEnvVar("GCP_BUCKET_NAME"),
          },
        };
      default:
        throw new Error(`Invalid STORAGE_TYPE: ${storageType}`);
    }
  })();

  const config = {
    port: readEnvVar("PORT"),
    password: readEnvVar("PASSWORD"),
    frontendUrl: readEnvVar("FRONTEND_URL"),
    logLevel,
    storageConfig,
  };

  if (missing.length > 0) {
    throw new Error(`Missing env vars: ${missing.join(", ")}`);
  }

  return config;
}
