import chalk from "chalk";

export interface OutputOptions {
  json?: boolean;
  quiet?: boolean;
}

export const success = (msg: string) => console.log(chalk.green("✓"), msg);
export const info = (msg: string) => console.log(chalk.blue("ℹ"), msg);
export const warn = (msg: string) => console.log(chalk.yellow("⚠"), msg);
export const error = (msg: string) => console.error(chalk.red("✗"), msg);

export const bold = (s: string) => chalk.bold(s);
export const dim = (s: string) => chalk.dim(s);
export const cmd = (s: string) => chalk.cyan(s);

export const bullet = (msg: string) => console.log(chalk.green("●"), msg);
export const bulletDim = (msg: string) => console.log(chalk.dim("●"), msg);

export const hint = (msg: string) => console.log(chalk.dim(`  ${msg}`));
export const nextStep = (command: string) => console.log(`  ${cmd(command)}`);

export const header = (title: string) => {
  console.log();
  console.log(chalk.bold(title));
  console.log();
};

export function jsonOutput(data: object): void {
  console.log(JSON.stringify(data, null, 2));
}

export function output(
  options: OutputOptions,
  handlers: {
    json?: () => object;
    quiet?: () => void;
    human: () => void;
  },
): void {
  if (options.json && handlers.json) {
    jsonOutput(handlers.json());
  } else if (options.quiet && handlers.quiet) {
    handlers.quiet();
  } else {
    handlers.human();
  }
}
