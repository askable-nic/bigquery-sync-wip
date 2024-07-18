import {
  BigQuery,
  InsertRowsResponse,
  TableField,
} from "@google-cloud/bigquery";
import { Document, MongoClient, ServerApiVersion } from "mongodb";

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
export class BigqueryTable {
  name: string;
  mergeTableName: string;
  idColumn: string;
  schema: TableField[] = [];
  private schemaFieldNames: string[] = [];
  // private schemaFieldDefinitions: Record<string, TableField> = {};
  constructor(name: string, idColumn: string, schemaJson: unknown) {
    this.name = name;
    this.mergeTableName = `${name}_tmp_merge`;
    this.idColumn = idColumn;
    this.schema = schemaJson as TableField[];
    this.schemaFieldNames = this.schema
      .filter((field) => field.name)
      .map((field) => field.name!);
    // this.schema.forEach((field) => {
    //   if (field.name) {
    //     this.schemaFieldNames.push(field.name);
    //     this.schemaFieldDefinitions[field.name] = field;
    //   }
    // });
  }

  get columns() {
    return this.schemaFieldNames;
  }
}

export const savePipelineToTable = async (
  pipeline: Document[],
  collection: string,
  table: BigqueryTable
): Promise<{ created: 0; modified: 0 }> => {
  const { BIGQUERY_DATASET } = env;

  const { db, client: mongoClient } = await mongoConnect();
  const bigqueryClient = new BigQuery();

  // TODO: move to the end
  await bigqueryClient
    .dataset(BIGQUERY_DATASET)
    .table(table.mergeTableName)
    .query(`TRUNCATE TABLE ${BIGQUERY_DATASET}.${table.mergeTableName}`);

  // if merge table has records, fail the job

  const cursor = db
    .collection(collection)
    .aggregate(pipeline, { readPreference: "secondaryPreferred" });

  let insertDocsBatch: Document[] = [];
  const insertChunkSize = 5;
  let totalRows = 0;
  const batchPromises: Promise<InsertRowsResponse>[] = [];
  const queueInsertBatch = async () => {
    if (insertDocsBatch.length === 0) {
      return;
    }
    batchPromises.push(
      bigqueryClient
        .dataset(BIGQUERY_DATASET)
        .table(table.mergeTableName)
        .insert(insertDocsBatch)
    );
    insertDocsBatch = [];
  };
  for await (const row of cursor) {
    totalRows += 1;
    insertDocsBatch.push(row);
    if (insertDocsBatch.length >= insertChunkSize) {
      queueInsertBatch();
    }
  }
  queueInsertBatch();
  console.log("total rows", totalRows);
  await Promise.all(batchPromises);

  // Finished inserting rows, can close cursor and mongo client
  await cursor.close();
  await mongoClient.close();

  // const rows = await cursor.toArray();
  // console.log(`Found ${rows.length} records`);

  // insert batches of records into tmpMergeTable
  /*
  INSERT ${BIGQUERY_DATASET}.${table.mergeTableName} (...columns)
  VALUES (...row1), (...row2), ..., (...rowN)
  */

  // console.log('writing to bigquery...', BIGQUERY_DATASET, table.mergeTableName, rows.length);
  // const tmpInsertResult = await bigqueryClient
  //   .dataset(BIGQUERY_DATASET)
  //   .table(table.mergeTableName)
  //   .insert(rows);
  // console.log(tmpInsertResult);

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
