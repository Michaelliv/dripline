import {
  existsSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { nanoid } from "nanoid";

const DATA_DIR = ".dripline";

export function findRoot(): string | null {
  let dir = process.cwd();
  while (true) {
    if (existsSync(join(dir, DATA_DIR))) {
      return join(dir, DATA_DIR);
    }
    const parent = join(dir, "..");
    if (parent === dir) return null;
    dir = parent;
  }
}

export function requireRoot(): string {
  const root = findRoot();
  if (!root) {
    console.error("Not a dripline project. Run: dripline init");
    process.exit(1);
  }
  return root;
}

export function initStore(collections: string[]): string {
  const root = join(process.cwd(), DATA_DIR);
  if (existsSync(root)) return root;
  mkdirSync(root, { recursive: true });
  for (const col of collections) {
    mkdirSync(join(root, col), { recursive: true });
  }
  return root;
}

export function newId(): string {
  return nanoid(8);
}

export function readAll<T>(root: string, collection: string): T[] {
  const dir = join(root, collection);
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => JSON.parse(readFileSync(join(dir, f), "utf-8")));
}

export function readOne<T>(
  root: string,
  collection: string,
  id: string,
): T | null {
  const filePath = join(root, collection, `${id}.json`);
  if (!existsSync(filePath)) return null;
  return JSON.parse(readFileSync(filePath, "utf-8"));
}

export function writeRecord<T extends { id: string }>(
  root: string,
  collection: string,
  record: T,
): void {
  const dir = join(root, collection);
  if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
  writeFileSync(
    join(dir, `${record.id}.json`),
    `${JSON.stringify(record, null, 2)}\n`,
  );
}

export function deleteRecord(
  root: string,
  collection: string,
  id: string,
): boolean {
  const filePath = join(root, collection, `${id}.json`);
  if (!existsSync(filePath)) return false;
  unlinkSync(filePath);
  return true;
}
