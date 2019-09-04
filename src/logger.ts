export interface Logger {
  debug(message: string): void;
  info(message: string): void;
  warning(message: string): void;
  error(message: string): void;
}

export type LogLevel = "debug" | "info" | "warning" | "error";

export const createLogger = (level: LogLevel): Logger => ({
  debug: (message: string) => {
    if (level === "debug") {
      console.log(message);
    }
  },
  info: (message: string) => {
    if (level === "debug" || level === "info") {
      console.log(message);
    }
  },
  warning: (message: string) => {
    if (level !== "error") {
      console.warn(message);
    }
  },
  error: (message: string) => {
    console.error(message);
  },
});
