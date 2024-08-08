import { BigQuery, TableField } from "@google-cloud/bigquery";
import { MongoClient, ServerApiVersion } from "mongodb";
import dotenv from "dotenv";
import { TableName } from "./constants";

dotenv.config();

type EventDataSchema = {
  table?: TableName;
  method: "sync" | "export";
  options?: Record<string, unknown>;
};

export const env = process.env as {
  // SYNC_METHOD?: string;
  SYNC_TABLE?: string;
  SYNC_OPTIONS?: string;
  METHOD?: string;
  ANALYTICS_DB_URI: string;
  BIGQUERY_DATASET: string;
  OPENEXCHANGERATES_APP_ID: string;
};

export function safeJson(data: string) {
  try {
    return JSON.parse(data);
  } catch {
    return undefined;
  }
}

export const jobParams = (): EventDataSchema => ({
  table: env.SYNC_TABLE as unknown as TableName,
  method: env.METHOD as unknown as EventDataSchema["method"] ?? "sync",
  options: safeJson(env.SYNC_OPTIONS ?? "") ?? undefined,
});

export const mongoConnect = async (dbName: string = "askable") => {
  if (!env.ANALYTICS_DB_URI) {
    throw new Error("ANALYTICS_DB_URI is not set");
  }

  const client = new MongoClient(env.ANALYTICS_DB_URI, {
    serverApi: {
      version: ServerApiVersion.v1,
      strict: true,
      deprecationErrors: true,
    },
  });

  await client.connect();
  return {
    client,
    db: await client.db(dbName),
  };
};

export const safeMapLookup = (
  map: Record<string | number, string>,
  key: string | number
) => {
  if (typeof key === "string" || typeof key === "number") {
    return map?.[key] ?? null;
  }
  return null;
};

export const tmpTableName = (table: TableName) => `${table}_tmp_merge`;

type TableMetadata = {
  tableReference: { projectId?: string; datasetId?: string; tableId?: string };
  streamingBuffer?: {
    estimatedRows: string;
    estimatedBytes: string;
    oldestEntryTime: string;
  };
  schema?: { fields?: TableField[] };
};

export const bqTableMeta = async (
  client: BigQuery,
  table: TableName | string
): Promise<TableMetadata> => {
  const { BIGQUERY_DATASET } = env;
  const response = await client
    .dataset(BIGQUERY_DATASET)
    .table(table)
    .getMetadata();

  return response[0] as TableMetadata;
};

export const dayDiffMs = (days: number) =>
  Date.now() - days * 24 * 60 * 60 * 1000;

export const round = (v: number | null | undefined, decimals: number = 0) => {
  if (typeof v !== "number") {
    return null;
  }
  return Math.round(v * 10 ** decimals) / 10 ** decimals;
}