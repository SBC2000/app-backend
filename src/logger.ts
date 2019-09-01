export interface Logger {
  info(message: string): void;
  warning(message: string): void;
  error(message: string): void;
}

export const createConsoleLogger = (): Logger => ({
  info: (message: string) => {
    console.log(message);
  },
  warning: (message: string) => {
    console.warn(message);
  },
  error: (message: string) => {
    console.error(message);
  },
});

export const createDummyLogger = (): Logger => ({
  info: () => {},
  warning: () => {},
  error: () => {},
});
