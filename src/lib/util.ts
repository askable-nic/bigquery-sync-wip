import { BigQuery, TableField } from "@google-cloud/bigquery";
import { adapt, managedwriter } from "@google-cloud/bigquery-storage";
import { JSONObject } from "@google-cloud/bigquery-storage/build/src/managedwriter/json_writer";
import { PendingWrite } from "@google-cloud/bigquery-storage/build/src/managedwriter/pending_write";
import { Document, MongoClient, ServerApiVersion } from "mongodb";

import { TableName } from "./types";

const { WriterClient, JSONWriter } = managedwriter;

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
  const startTime = Date.now();
  const timeElapsed = () => `${((Date.now() - startTime) / 1000).toFixed(1)}s`;
  const { BIGQUERY_DATASET } = env;

  const { db, client: mongoClient } = await mongoConnect();
  const bigqueryClient = new BigQuery();

  const [tableMetaData] = await bigqueryClient
    .dataset(BIGQUERY_DATASET)
    .table(table.mergeTableName)
    .getMetadata();

  const { projectId, datasetId, tableId } =
    tableMetaData.tableReference as Record<string, string>;

  // if merge table has records, fail the job
  // TODO: move to end
  console.log("Truncating table...");
  await bigqueryClient
    .dataset(BIGQUERY_DATASET)
    .table(table.mergeTableName)
    .query(`TRUNCATE TABLE ${BIGQUERY_DATASET}.${table.mergeTableName}`)
    .catch((e) => {
      console.warn("Failed to truncate table", e);
    });
  console.log(timeElapsed(), "Done");

  const cursor = db
    .collection(collection)
    .aggregate(pipeline, { readPreference: "secondaryPreferred" });

  const destinationTable = `projects/${projectId}/datasets/${datasetId}/tables/${tableId}`;
  const streamType = managedwriter.PendingStream;
  const writeClient = new WriterClient({ projectId });

  try {
    const writeStream = await writeClient.createWriteStreamFullResponse({
      streamType,
      destinationTable,
    });
    if (!writeStream.name || !writeStream.tableSchema) {
      throw new Error("Stream ID invalid");
    }
    console.log(`Stream created: ${writeStream.name}`);

    const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(
      writeStream.tableSchema,
      "root"
    );

    // console.log("Proto descriptor: ", protoDescriptor);

    const connection = await writeClient.createStreamConnection({
      streamId: writeStream.name,
    });

    // console.log("Connection created: ", connection);

    const writer = new JSONWriter({
      connection,
      protoDescriptor,
    });

    // console.log("Writer created: ", writer);

    const writePromises: ReturnType<PendingWrite["getResult"]>[] = [];

    let totalRows = 0;
    let appendRowBatch: JSONObject[] = [];
    let pwOffset = 0;

    const loggingInterval = setInterval(() => {
      console.log({
        timeElapsed: timeElapsed(),
        rowsIterated: totalRows,
        appendRowBatch: appendRowBatch.length,
        pwOffset,
        writePromisesTotal: writePromises.length,
      });
    }, 3000);

    for await (const row of cursor) {
      totalRows += 1;
      appendRowBatch.push(row);
      if (appendRowBatch.length >= 1000) {
        const pw = writer.appendRows(appendRowBatch, pwOffset);
        pwOffset += appendRowBatch.length;
        writePromises.push(pw.getResult());
        appendRowBatch = [];
      }
    }
    if (appendRowBatch.length) {
      const pw = writer.appendRows(appendRowBatch, pwOffset);
      writePromises.push(pw.getResult());
    }

    console.log(timeElapsed(), `Rows pushed: ${totalRows}`);

    await Promise.all(writePromises);

    clearInterval(loggingInterval);

    const rowCount = (await connection.finalize())?.rowCount;
    console.log(timeElapsed(), `Connection row count: ${rowCount}`);

    const response = await writeClient.batchCommitWriteStream({
      parent: destinationTable,
      writeStreams: [writeStream.name],
    });

    console.log(timeElapsed(), response);
  } catch (err) {
    console.log(err);
  } finally {
    writeClient.close();
  }

  // Finished inserting rows, can close cursor and mongo client
  await cursor.close();
  await mongoClient.close();

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
