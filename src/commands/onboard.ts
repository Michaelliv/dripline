import { existsSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { OutputOptions } from "../utils/output.js";
import { output, success } from "../utils/output.js";

const INSTRUCTIONS = `
<dripline>
Use \`dripline\` to query cloud APIs using SQL. Config lives in \`.dripline/config.json\`.

<commands>
- \`dripline query "<sql>"\` - Run a SQL query (alias: \`dripline q\`)
- \`dripline\` or \`dripline repl\` - Interactive SQL shell
- \`dripline init\` - Initialize .dripline/ directory
- \`dripline onboard\` - Add these instructions to CLAUDE.md
</commands>

<tables>
- \`github_repos\` - WHERE owner = 'x' (required)
- \`github_issues\` - WHERE owner = 'x' AND repo = 'y' (required)
- \`github_pull_requests\` - WHERE owner = 'x' AND repo = 'y' (required)
- \`github_stargazers\` - WHERE owner = 'x' AND repo = 'y' (required)
</tables>

<rules>
- ALWAYS use \`--json\` flag to get structured output for parsing
- Key columns (owner, repo) are required WHERE clauses — queries without them return empty
- Config: \`.dripline/config.json\` for API tokens and rate limits
</rules>
</dripline>
`.trim();

const MARKER = "<dripline>";

export async function onboard(
  _args: string[],
  options: OutputOptions,
): Promise<void> {
  const cwd = process.cwd();
  const claudeMd = join(cwd, "CLAUDE.md");
  const agentsMd = join(cwd, "AGENTS.md");

  let targetFile: string;
  if (existsSync(claudeMd)) {
    targetFile = claudeMd;
  } else if (existsSync(agentsMd)) {
    targetFile = agentsMd;
  } else {
    targetFile = claudeMd;
  }

  let existingContent = "";
  if (existsSync(targetFile)) {
    existingContent = readFileSync(targetFile, "utf-8");
  }

  if (existingContent.includes(MARKER)) {
    output(options, {
      json: () => ({
        success: true,
        file: targetFile,
        message: "already_onboarded",
      }),
      human: () => success(`Already onboarded (${targetFile})`),
    });
    return;
  }

  if (existingContent) {
    writeFileSync(
      targetFile,
      `${existingContent.trimEnd()}\n\n${INSTRUCTIONS}\n`,
    );
  } else {
    writeFileSync(targetFile, `${INSTRUCTIONS}\n`);
  }

  output(options, {
    json: () => ({ success: true, file: targetFile }),
    human: () => success(`Added dripline instructions to ${targetFile}`),
  });
}
