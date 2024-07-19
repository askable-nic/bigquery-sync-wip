import { MongoClient, ServerApiVersion } from "mongodb";

import { TableName } from "./types";

require("dotenv").config();

type EventDataSchema = {
  table?: TableName;
  method?: "sync" | "merge";
};

export const decodeEventData = (
  data?: string,
  defaultValue: EventDataSchema = {}
) => {
  if (process.env.MOCK_EVENT_DATA) {
    try {
      return JSON.parse(process.env.MOCK_EVENT_DATA) as EventDataSchema;
    } catch (e) {}
  }
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
