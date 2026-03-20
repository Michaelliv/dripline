import { initStore } from "../store.js";
import type { OutputOptions } from "../utils/output.js";
import { cmd, hint, output, success } from "../utils/output.js";

const COLLECTIONS = ["plugins", "connections"];

export async function init(
  _args: string[],
  options: OutputOptions,
): Promise<void> {
  const root = initStore(COLLECTIONS);

  output(options, {
    json: () => ({ success: true, path: root }),
    human: () => {
      success(`Initialized .dripline/ in ${process.cwd()}`);
      hint("Next: add a connection");
      console.log(`  ${cmd("dripline connection add gh --plugin github --set token=ghp_xxx")}`);
    },
  });
}
