import { BigQuery, TableField } from "@google-cloud/bigquery";
import { CloudEvent } from "@google-cloud/functions-framework";
import { MongoClient, ServerApiVersion } from "mongodb";

import { TableName } from "./types";

require("dotenv").config();

type EventDataSchema = {
  table?: TableName;
  method?: "sync";
  options?: Record<string, unknown>;
};

export const decodeEventData = (
  event: CloudEvent<string>,
  defaultValue: EventDataSchema = {}
) => {
  if (process.env.MOCK_EVENT_DATA) {
    try {
      return JSON.parse(process.env.MOCK_EVENT_DATA) as EventDataSchema;
    } catch (e) {}
  }
  if (process.env.ENV === 'dev' && typeof event?.query === 'object') {
    const query = event.query as unknown as EventDataSchema;
    return {
      table: query?.table,
      method: query?.method
    };
  }

  const data = event.data;

  if (!data) {
    return defaultValue;
  }
  try {
    const decoded = JSON.parse(
      Buffer.from(data, "base64").toString()
    ) as EventDataSchema;
    return decoded;
  } catch (e) {
    return defaultValue;
  }
};

export const env = process.env as {
  ANALYTICS_DB_URI: string;
  BIGQUERY_DATASET: string;
  OPENEXCHANGERATES_APP_ID: string;
};

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
