import Database from "better-sqlite3";
import { execSync } from "node:child_process";

const db = new Database(":memory:");

// Plugin 1: GitHub repos (real API call)
db.table("github_repos", {
  columns: ["id", "name", "full_name", "description", "stargazers_count", "language", "html_url"],
  parameters: ["owner"],
  *rows(owner: string) {
    if (!owner) throw new Error("owner parameter is required: WHERE owner = 'someone'");
    console.log(`[github_repos] fetching repos for ${owner}...`);
    
    const res = execSync(`curl -s "https://api.github.com/users/${owner}/repos?per_page=100"`);
    const repos = JSON.parse(res.toString());
    
    for (const r of repos) {
      yield {
        id: r.id,
        name: r.name,
        full_name: r.full_name,
        description: r.description || "",
        stargazers_count: r.stargazers_count,
        language: r.language || "",
        html_url: r.html_url,
      };
    }
  },
});

// Plugin 2: Static process list (local data source)
db.table("processes", {
  columns: ["pid", "name", "cpu", "mem"],
  *rows() {
    console.log("[processes] fetching...");
    const res = execSync("ps aux");
    const lines = res.toString().split("\n").slice(1).filter(Boolean);
    for (const line of lines) {
      const parts = line.trim().split(/\s+/);
      yield {
        pid: parseInt(parts[1]),
        name: parts[10] || parts[0],
        cpu: parseFloat(parts[2]),
        mem: parseFloat(parts[3]),
      };
    }
  },
});

console.log("\n=== Test 1: GitHub repos for Michaelliv, sorted by stars ===");
const repos = db.prepare(`
  SELECT name, stargazers_count, language 
  FROM github_repos 
  WHERE owner = 'Michaelliv' 
  ORDER BY stargazers_count DESC 
  LIMIT 10
`).all();
console.table(repos);

console.log("\n=== Test 2: Top 5 CPU-hungry processes ===");
const procs = db.prepare(`
  SELECT pid, name, cpu, mem 
  FROM processes 
  ORDER BY cpu DESC 
  LIMIT 5
`).all();
console.table(procs);

console.log("\n=== Test 3: JOIN - repos with language stats ===");
const langStats = db.prepare(`
  SELECT language, COUNT(*) as count, SUM(stargazers_count) as total_stars
  FROM github_repos 
  WHERE owner = 'Michaelliv' AND language != ''
  GROUP BY language 
  ORDER BY count DESC
`).all();
console.table(langStats);

console.log("\n✅ All smoke tests passed");
