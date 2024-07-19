import { BigQuery, TableField } from "@google-cloud/bigquery";
import { adapt, managedwriter } from "@google-cloud/bigquery-storage";
import { JSONObject } from "@google-cloud/bigquery-storage/build/src/managedwriter/json_writer";
import { PendingWrite } from "@google-cloud/bigquery-storage/build/src/managedwriter/pending_write";
import { StreamConnection } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_connection";
import { WriteStream } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_types";
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

export type BigqueryTableSyncOptions = {
  name: string;
  idField: string;
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

class BqDataSync {
  startTime: number = 0;

  tableName: string;
  idField: string;
  mergeTableName: string;

  _metadata?: {
    projectId: string;
    datasetId: string;
    tableId: string;
    fields: TableField[];
  };

  client: BigQuery;
  destinationTable?: string;
  writeClient?: managedwriter.WriterClient;
  writeStream?: WriteStream;
  connection?: StreamConnection;
  writer?: managedwriter.JSONWriter;
  writePromises: ReturnType<PendingWrite["getResult"]>[] = [];
  pwOffset = 0;

  ready = false;
  loggerInterval: NodeJS.Timeout | null = null;

  constructor(table: BigqueryTableSyncOptions) {
    this.startTime = Date.now();

    (this.tableName = table.name),
      (this.idField = table.idField),
      (this.mergeTableName = `${table.name}_tmp_merge`),
      (this.client = new BigQuery());
  }

  async init() {
    const { BIGQUERY_DATASET } = env;
    const { WriterClient, JSONWriter } = managedwriter;

    const [tableMetaData] = await this.client
      .dataset(BIGQUERY_DATASET)
      .table(this.mergeTableName)
      .getMetadata();

    this._metadata = {
      projectId: tableMetaData.tableReference?.projectId as string,
      datasetId: tableMetaData.tableReference?.datasetId as string,
      tableId: tableMetaData.tableReference?.tableId as string,
      fields: (tableMetaData?.schema?.fields ?? []) as TableField[],
    };

    // TODO: fail if merge table has rows (update in progress)

    this.destinationTable = `projects/${this.projectId}/datasets/${this.datasetId}/tables/${this.tableId}`;
    const streamType = managedwriter.PendingStream;
    this.writeClient = new WriterClient({ projectId: this.projectId });

    this.writeStream = await this.writeClient.createWriteStreamFullResponse({
      streamType,
      destinationTable: this.destinationTable,
    });
    if (!this.writeStream.name || !this.writeStream.tableSchema) {
      throw new Error("Stream ID invalid");
    }
    console.log(`Stream created: ${this.writeStream.name}`);

    const protoDescriptor = adapt.convertStorageSchemaToProto2Descriptor(
      this.writeStream.tableSchema,
      "root"
    );

    // console.log("Proto descriptor: ", protoDescriptor);

    this.connection = await this.writeClient.createStreamConnection({
      streamId: this.writeStream.name,
    });

    // console.log("Connection created: ", connection);

    this.writer = new JSONWriter({
      connection: this.connection,
      protoDescriptor,
    });

    this.ready = true;
  }

  startLogging(timeout: number, extra?: () => Record<string, any>) {
    this.stopLogging();
    this.loggerInterval = setInterval(() => {
      console.log({
        timeElapsed: this.timeElapsed,
        pwOffset: this.pwOffset,
        writePromisesTotal: this.writePromises.length,
        ...extra?.(),
      });
    }, timeout);
  }
  stopLogging() {
    if (this.loggerInterval) {
      clearInterval(this.loggerInterval);
    }
  }

  writeBatch(rows: JSONObject[]) {
    if (!this.ready || !this.writer) {
      throw new Error("Not initialized");
    }
    const pw = this.writer.appendRows(rows, this.pwOffset);
    this.pwOffset += rows.length;
    this.writePromises.push(pw.getResult());
  }

  async commitWrites() {
    if (
      !this.ready ||
      !this.connection ||
      !this.writeClient ||
      !this.destinationTable ||
      !this.writeStream?.name
    ) {
      throw new Error("Not initialized");
    }
    await Promise.all(this.writePromises);
    this.writePromises = [];

    const rowCount = (await this.connection.finalize())?.rowCount;
    console.log(this.timeElapsed, `Connection row count: ${rowCount}`);

    const response = await this.writeClient.batchCommitWriteStream({
      parent: this.destinationTable,
      writeStreams: [this.writeStream.name],
    });

    console.log(this.timeElapsed, response);
  }

  async truncateMergeTable() {
    // TODO: make sure the buffer is clear:
    /* (TRUNCATE DML statement over table operations_data_warehouse_test.credit_activity_tmp_merge would affect rows in the streaming buffer, which is not supported) */
    console.log("Truncating table...");
    await this.client
      .dataset(this.datasetId)
      .table(this.mergeTableName)
      .query(`TRUNCATE TABLE ${this.datasetId}.${this.mergeTableName}`)
      .catch((e) => {
        console.warn("Failed to truncate table", e);
      });
    console.log(this.timeElapsed, "Done");
  }

  async mergeTmpTable() {
    // TODO: make sure the buffer is clear:
    /* (TRUNCATE DML statement over table operations_data_warehouse_test.credit_activity_tmp_merge would affect rows in the streaming buffer, which is not supported) */
  }

  async cleanup() {
    this.stopLogging();
    this.writeClient?.close();
  }

  get timeElapsed() {
    return `${((Date.now() - this.startTime) / 1000).toFixed(1)}s`;
  }
  get projectId() {
    if (!this._metadata?.projectId) {
      throw new Error("projectId is not set");
    }
    return this._metadata.projectId;
  }
  get datasetId() {
    if (!this._metadata?.datasetId) {
      throw new Error("datasetId is not set");
    }
    return this._metadata.datasetId;
  }
  get tableId() {
    if (!this._metadata?.tableId) {
      throw new Error("tableId is not set");
    }
    return this._metadata.tableId;
  }
  get fields() {
    if (!this._metadata?.fields) {
      throw new Error("fields is not set");
    }
    return this._metadata.fields;
  }
}

export const savePipelineToTable = async (
  pipeline: Document[],
  collection: string,
  table: BigqueryTableSyncOptions
): Promise<{ created: 0; modified: 0 }> => {
  const dataSync = new BqDataSync(table);
  await dataSync.init();

  const { db, client: mongoClient } = await mongoConnect();
  const cursor = db
    .collection(collection)
    .aggregate(pipeline, { readPreference: "secondaryPreferred" });

  await dataSync.truncateMergeTable(); // TODO: move to end

  let totalRows = 0;
  let appendRowBatch: JSONObject[] = [];

  dataSync.startLogging(500, () => ({
    totalRows,
    appendRowBatch: appendRowBatch.length,
  }));

  for await (const row of cursor) {
    totalRows += 1;
    appendRowBatch.push(row);
    if (appendRowBatch.length >= 1000) {
      dataSync.writeBatch(appendRowBatch);
      appendRowBatch = [];
    }
  }
  if (appendRowBatch.length) {
    dataSync.writeBatch(appendRowBatch);
  }

  await dataSync.commitWrites();

  // Finished inserting rows, can close cursor and mongo client
  await cursor.close();
  await mongoClient.close();

  await dataSync.cleanup();

  // const mergeQuery = [
  //   `MERGE ${BIGQUERY_DATASET}.${table.name} T`,
  //   `USING ${BIGQUERY_DATASET}.${mergeTableName} S`,

  // ].join('\n')

  // merge tmpMergeTable into table
  /*
  MERGE ${BIGQUERY_DATASET}.${table.name} T
  USING ${BIGQUERY_DATASET}.${mergeTableName} S
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
