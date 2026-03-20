export function formatJson(rows: any[]): string {
  return JSON.stringify(rows, null, 2);
}

export function formatCsv(rows: any[]): string {
  if (rows.length === 0) return "";
  const keys = Object.keys(rows[0]);
  const lines: string[] = [keys.map(csvEscape).join(",")];
  for (const row of rows) {
    lines.push(keys.map((k) => csvEscape(row[k])).join(","));
  }
  return lines.join("\n");
}

function csvEscape(v: any): string {
  if (v === null || v === undefined) return "";
  const s = String(v);
  if (s.includes(",") || s.includes('"') || s.includes("\n")) {
    return `"${s.replace(/"/g, '""')}"`;
  }
  return s;
}

export function formatLine(rows: any[]): string {
  if (rows.length === 0) return "No results.";
  const keys = Object.keys(rows[0]);
  const maxKeyLen = Math.max(...keys.map((k) => k.length));
  const blocks: string[] = [];
  for (const row of rows) {
    const lines: string[] = [];
    for (const k of keys) {
      const val = row[k] === null || row[k] === undefined ? "<null>" : String(row[k]);
      lines.push(`${k.padEnd(maxKeyLen)} | ${val}`);
    }
    blocks.push(lines.join("\n"));
  }
  return blocks.join("\n\n");
}
