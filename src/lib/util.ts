import { AggregationCursor, MongoClient, ServerApiVersion } from "mongodb";
import { TableName } from "./types";
require("dotenv").config();

type EventDataSchema = {
  table?: TableName;
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

export const saveAggregationResults = async (
  cursor: AggregationCursor,
  table: string
): Promise<{ created: 0; modified: 0 }> => {
  const rows = await cursor.toArray();
  rows.forEach((row) => {
    console.log(JSON.stringify(row));
  });
  return { created: 0, modified: 0 }; // return number
};

export const env = process.env as {
  ANALYTICS_DB_URI: string;
};

export const mongoDb = async (dbName: string = "askable") => {
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
  return await client.db(dbName);
};
