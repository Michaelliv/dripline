export function formatTable(
  rows: Record<string, any>[],
  options?: { maxWidth?: number },
): string {
  if (rows.length === 0) return "No results.";

  const maxWidth = options?.maxWidth ?? process.stdout.columns ?? 120;
  const keys = Object.keys(rows[0]);

  // Calculate column widths
  const widths: Record<string, number> = {};
  for (const k of keys) {
    widths[k] = k.length;
  }
  for (const row of rows) {
    for (const k of keys) {
      const val = formatValue(row[k]);
      widths[k] = Math.max(widths[k], val.length);
    }
  }

  // Cap widths to fit terminal
  const totalBorder = keys.length + 1; // │ between each + edges
  const totalPad = keys.length * 2; // 1 space each side
  const available = maxWidth - totalBorder - totalPad;
  const totalWidth = Object.values(widths).reduce((a, b) => a + b, 0);

  if (totalWidth > available && available > 0) {
    const ratio = available / totalWidth;
    for (const k of keys) {
      widths[k] = Math.max(3, Math.floor(widths[k] * ratio));
    }
  }

  const lines: string[] = [];

  // Top border
  lines.push(
    `┌${keys.map((k) => "─".repeat(widths[k] + 2)).join("┬")}┐`,
  );

  // Header
  lines.push(
    `│${keys.map((k) => ` ${pad(k, widths[k])} `).join("│")}│`,
  );

  // Header separator
  lines.push(
    `├${keys.map((k) => "─".repeat(widths[k] + 2)).join("┼")}┤`,
  );

  // Data rows
  for (const row of rows) {
    lines.push(
      `│${keys.map((k) => ` ${pad(truncate(formatValue(row[k]), widths[k]), widths[k])} `).join("│")}│`,
    );
  }

  // Bottom border
  lines.push(
    `└${keys.map((k) => "─".repeat(widths[k] + 2)).join("┴")}┘`,
  );

  lines.push(`\n${rows.length} row${rows.length === 1 ? "" : "s"}.`);

  return lines.join("\n");
}

function formatValue(v: any): string {
  if (v === null || v === undefined) return "<null>";
  if (typeof v === "boolean") return v ? "true" : "false";
  return String(v);
}

function pad(s: string, width: number): string {
  return s.padEnd(width);
}

function truncate(s: string, maxLen: number): string {
  if (s.length <= maxLen) return s;
  return `${s.slice(0, maxLen - 1)}…`;
}
