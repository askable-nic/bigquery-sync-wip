import { MongoClient, ServerApiVersion, Document } from "mongodb";
import { BigQuery } from "@google-cloud/bigquery";
import { TableName } from "./types";
import { BigqueryTable } from "./tables";
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

export const savePipelineToTable = async (
  pipeline: Document[],
  collection: string,
  table: BigqueryTable
): Promise<{ created: 0; modified: 0 }> => {
  const { BIGQUERY_DATASET } = env;

  const { db, client: mongoClient } = await mongoConnect();
  const bigqueryClient = new BigQuery();

  // if merge table has records, fail the job

  const cursor = db
    .collection(collection)
    .aggregate(pipeline, { readPreference: "secondaryPreferred" });

  const rows = await cursor.toArray();
  console.log(`Found ${rows.length} records`);

  // insert batches of records into tmpMergeTable
  /*
  INSERT ${BIGQUERY_DATASET}.${table.mergeTableName} (...columns)
  VALUES (...row1), (...row2), ..., (...rowN)
  */

  console.log('writing to bigquery...', BIGQUERY_DATASET, table.mergeTableName, rows.length);
  const tmpInsertResult = await bigqueryClient
    .dataset(BIGQUERY_DATASET)
    .table(table.mergeTableName)
    .insert(rows);
  console.log(tmpInsertResult);

  await cursor.close();
  await mongoClient.close();

  // merge tmpMergeTable into table
  /*
  MERGE ${BIGQUERY_DATASET}.${table.name} T
  USING ${BIGQUERY_DATASET}.${table.mergeTableName} S
  ON T.ID = S.ID
  WHEN MATCHED THEN
    UPDATE SET T.col1 = S.col1, T.col2 = S.col2, ...
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (ID, col1, col2, ...)
    VALUES (S.ID, S.col1, S.col2, ...)
  WHEN NOT MATCHED BY SOURCE THEN
    DELETE
  */

  // truncate tmpMergeTable

  return { created: 0, modified: 0 }; // return number
};
