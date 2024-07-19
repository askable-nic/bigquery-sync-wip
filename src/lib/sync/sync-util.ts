import { BigQuery, TableField } from "@google-cloud/bigquery";
import { adapt, managedwriter } from "@google-cloud/bigquery-storage";
import { JSONObject } from "@google-cloud/bigquery-storage/build/src/managedwriter/json_writer";
import { PendingWrite } from "@google-cloud/bigquery-storage/build/src/managedwriter/pending_write";
import { StreamConnection } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_connection";
import { WriteStream } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_types";
import { Document } from "mongodb";

import { env, mergeTableName, mongoConnect } from "../util";
import { TableName } from "../types";

class BqDataSync {
  startTime: number = 0;

  tableName: string;
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

  constructor(tableName: TableName) {
    this.startTime = Date.now();

    this.tableName = tableName;
    this.mergeTableName = mergeTableName(tableName);
    this.client = new BigQuery();
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

    const connectionFinalizeResult = await this.connection.finalize();
    // console.log(this.timeElapsed, `Connection row count: ${rowCount}`);
    console.log(
      this.timeElapsed,
      "Connection finalized",
      connectionFinalizeResult
    );

    // const finalizeResponse = await this.writeClient.finalizeWriteStream({
    //   name: this.writeStream.name,
    // });

    // console.log(this.timeElapsed, "Write stream finalized", finalizeResponse);

    const commitResponse = await this.writeClient.batchCommitWriteStream({
      parent: this.destinationTable,
      writeStreams: [this.writeStream.name],
    });

    console.log(this.timeElapsed, "Write stream committed", commitResponse);
  }

  async truncateMergeTable() {
    // TODO: make sure the buffer is clear:
    /* (TRUNCATE DML statement over table operations_data_warehouse_test.credit_activity_tmp_merge would affect rows in the streaming buffer, which is not supported) */
    console.log(this.timeElapsed, "Truncating table...");
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
    if (this.writeClient) {
      this.writeClient.close();
    }
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
}

export const syncPipelineToMergeTable = async (
  pipeline: Document[],
  collection: string,
  table: TableName
): Promise<{ created: 0; modified: 0 }> => {
  const dataSync = new BqDataSync(table);

  const { db, client: mongoClient } = await mongoConnect();
  const cursor = db
    .collection(collection)
    .aggregate(pipeline, { readPreference: "secondaryPreferred" });

  try {
    await dataSync.init();

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

    dataSync.stopLogging();
  } finally {
    await dataSync.cleanup();
    await cursor.close();
    await mongoClient.close();
  }

  return { created: 0, modified: 0 }; // return number
};
