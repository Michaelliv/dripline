/** Shorten an absolute path by replacing the home directory prefix with ~. */
export function shortenPath(p: unknown, homeDir?: string | null): string {
  if (typeof p !== "string") return "";
  if (homeDir) {
    const normalizedPath = p.replace(/\\/g, "/");
    const normalizedHome = homeDir.replace(/\\/g, "/");
    if (normalizedPath === normalizedHome) return "~";
    if (normalizedPath.startsWith(`${normalizedHome}/`)) {
      return `~/${normalizedPath.slice(normalizedHome.length + 1)}`;
    }
  }
  return p;
}
