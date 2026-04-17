import type {
  ColumnDef,
  GetFunc,
  HydrateFunc,
  KeyColumn,
  ListFunc,
  PluginDef,
  TableDef,
} from "./types.js";

export interface SchemaField {
  type: "string" | "number" | "boolean";
  required?: boolean;
  description?: string;
  default?: any;
  env?: string;
}

export interface TableDefinition {
  columns: ColumnDef[];
  keyColumns?: KeyColumn[];
  /** Row identity columns for deduplication during sync. */
  primaryKey?: string[];
  /** Default params for sync() — used when caller doesn't provide params for this table. */
  syncParams?: Record<string, any>;
  /** Column name used as high-water mark for incremental sync. Type inferred from columns[]. */
  cursor?: string;
  list: ListFunc;
  get?: GetFunc;
  hydrate?: Record<string, HydrateFunc>;
  description?: string;
}

export interface DriplinePluginAPI {
  registerTable(name: string, def: TableDefinition): void;
  setConnectionSchema(schema: Record<string, SchemaField>): void;
  setName(name: string): void;
  setVersion(version: string): void;
  onInit(fn: (config: Record<string, any>) => void): void;
  log: {
    info(msg: string): void;
    warn(msg: string): void;
    error(msg: string): void;
  };
}

export type PluginFunction = (api: DriplinePluginAPI) => void;

export function createPluginAPI(pluginId: string): {
  api: DriplinePluginAPI;
  resolve: () => PluginDef;
} {
  let name = pluginId;
  let version = "0.0.0";
  const tables: TableDef[] = [];
  let connectionConfigSchema: PluginDef["connectionConfigSchema"];
  const initHooks: Array<(config: Record<string, any>) => void> = [];

  const api: DriplinePluginAPI = {
    setName(n: string) {
      name = n;
    },
    setVersion(v: string) {
      version = v;
    },
    registerTable(tableName: string, def: TableDefinition) {
      tables.push({ name: tableName, ...def });
    },
    setConnectionSchema(schema: Record<string, SchemaField>) {
      connectionConfigSchema = {};
      for (const [key, field] of Object.entries(schema)) {
        connectionConfigSchema[key] = { ...field };
      }
    },
    onInit(fn) {
      initHooks.push(fn);
    },
    log: {
      info(msg: string) {
        console.log(`[${name}] ${msg}`);
      },
      warn(msg: string) {
        console.warn(`[${name}] ${msg}`);
      },
      error(msg: string) {
        console.error(`[${name}] ${msg}`);
      },
    },
  };

  function resolve(): PluginDef {
    const plugin: PluginDef & {
      _initHooks?: Array<(config: Record<string, any>) => void>;
    } = {
      name,
      version,
      tables,
      connectionConfigSchema,
    };
    if (initHooks.length > 0) {
      plugin._initHooks = initHooks;
    }
    return plugin;
  }

  return { api, resolve };
}

export function isPluginFunction(val: any): val is PluginFunction {
  return typeof val === "function";
}

export function resolvePluginExport(
  exported: PluginFunction | PluginDef,
  pluginId: string,
): PluginDef {
  if (isPluginFunction(exported)) {
    const { api, resolve } = createPluginAPI(pluginId);
    exported(api);
    return resolve();
  }
  if (exported && typeof exported === "object" && "tables" in exported) {
    return exported as PluginDef;
  }
  throw new Error(
    `Invalid plugin export from "${pluginId}": expected a function or { name, tables } object`,
  );
}
