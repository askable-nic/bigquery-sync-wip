import { BigQuery, TableField } from "@google-cloud/bigquery";
import { adapt, managedwriter } from "@google-cloud/bigquery-storage";
import { JSONObject } from "@google-cloud/bigquery-storage/build/src/managedwriter/json_writer";
import { PendingWrite } from "@google-cloud/bigquery-storage/build/src/managedwriter/pending_write";
import { StreamConnection } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_connection";
import { WriteStream } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_types";
import { Document, FindCursor } from "mongodb";

import { env, mergeTableName, mongoConnect } from "../util";
import { TableName } from "../types";
import { idFieldName } from "../constants";

type BqDataSyncOptions = {
  batchSize?: number;
};

class BqDataSync {
  startTime: number = 0;

  tableName: string;
  mergeTableName: string;
  idField: string;

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

  batchSize: number;

  ready = false;
  loggerInterval: NodeJS.Timeout | null = null;

  constructor(tableName: TableName, options: BqDataSyncOptions = {}) {
    this.startTime = Date.now();
    this.tableName = tableName;
    this.mergeTableName = mergeTableName(tableName);
    this.idField = idFieldName[tableName];
    if (!this.idField) {
      throw new Error(`Missing/Invalid ID field for table ${tableName}`);
    }
    this.client = new BigQuery();

    this.batchSize = options.batchSize ?? 1000;
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

    const existingMergeTableRows = await this.countRows(this.mergeTableName);

    console.log(this.timeElapsed, {existingMergeTableRows});

    if (existingMergeTableRows && existingMergeTableRows > 0) {
      throw new Error(
        `Merge table ${this.mergeTableName} is not empty (${existingMergeTableRows} rows)`
      );
    }

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
    console.log(this.timeElapsed, await this.countAllTableRows());
  }

  // async truncateMergeTable() {
  //   // TODO: make sure the buffer is clear:
  //   /* (TRUNCATE DML statement over table operations_data_warehouse_test.credit_activity_tmp_merge would affect rows in the streaming buffer, which is not supported) */
  //   console.log(this.timeElapsed, "Truncating table...");
  //   await this.client
  //     .dataset(this.datasetId)
  //     .table(this.mergeTableName)
  //     .query(`TRUNCATE TABLE ${this.datasetId}.${this.mergeTableName}`)
  //     .catch((e) => {
  //       console.warn("Failed to truncate table", e);
  //     });
  //   console.log(this.timeElapsed, "Done");
  // }

  get mergeStatement() {
    if (!this.datasetId) {
      throw new Error("Metadata not set");
    }
    return [
      `MERGE \`${this.datasetId}.${this.tableName}\` T`,
      // `USING \`${BIGQUERY_DATASET}.${mergeTableName(table)}\` S`,
      `USING (SELECT * FROM \`${this.datasetId}.${this.mergeTableName}\` QUALIFY ROW_NUMBER() OVER (PARTITION BY ID) = 1) S`,
      `ON T.\`${this.idField}\` = S.\`${this.idField}\``,
      // Exists in both tables
      `WHEN MATCHED THEN`,
      `UPDATE SET `,
      this.fields
        .map((field) => `T.\`${field.name}\` = S.\`${field.name}\``)
        .join(", "),
      // Exists in source but not target
      `WHEN NOT MATCHED BY TARGET THEN`,
      `INSERT (${this.fields.map((field) => `\`${field.name}\``).join(", ")})`,
      `VALUES (${this.fields
        .map((field) => `S.\`${field.name}\``)
        .join(", ")})`,
      // Exists in target but not source
      `WHEN NOT MATCHED BY SOURCE THEN DELETE`,
    ].join(" ");
  }

  async mergeTmpTable() {
    if (!this.ready) {
      throw new Error("Not initialized");
    }

    console.log(this.timeElapsed, "Merging tmp table...");

    await this.client.query(this.mergeStatement);

    console.log(this.timeElapsed, "Merge complete");
    console.log(this.timeElapsed, await this.countAllTableRows());

    return true;

    // if (dmlStats) {
    //   return {
    //     inserted: dmlStats?.insertedRowCount
    //       ? Number(dmlStats?.insertedRowCount)
    //       : undefined,
    //     deleted: dmlStats?.deletedRowCount
    //       ? Number(dmlStats?.deletedRowCount)
    //       : undefined,
    //     updated: dmlStats?.updatedRowCount
    //       ? Number(dmlStats?.updatedRowCount)
    //       : undefined,
    //   };
    // } else {
    //   console.log(this.timeElapsed, "Merge resolved without stats");
    //   console.log(queryResponse);
    // }

    // if (queryResponse?.dmlStats) {
    //   console.log(this.timeElapsed, "Merge complete");
    // }

    // TODO: ~make sure the buffer is clear~
    /* (TRUNCATE DML statement over table operations_data_warehouse_test.credit_activity_tmp_merge would affect rows in the streaming buffer, which is not supported) */
  }

  async deleteTmpData() {
    if (!this.datasetId) {
      throw new Error("Not initialized");
    }

    console.log(this.timeElapsed, "Deleting tmp data...");

    await this.client.query(
      `DELETE FROM ${this.datasetId}.${this.mergeTableName} WHERE TRUE`
    );
  }

  async countRows(table: string) {
    if (!this.datasetId) {
      throw new Error("Not initialized");
    }

    const result = await this.client.query(
      `SELECT COUNT(*) as count FROM ${this.datasetId!}.${table}`
    );
    return result?.[0]?.[0]?.count as number | undefined;
  }

  async countAllTableRows() {
    const [mergeTable, mainTable] = await Promise.all([
      this.countRows(this.mergeTableName),
      this.countRows(this.tableName),
    ]);

    return {
      [this.mergeTableName]: mergeTable,
      [this.tableName]: mainTable,
    };
  }

  async closeStream() {
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
  get fields() {
    if (!this._metadata?.fields) {
      throw new Error("fields is not set");
    }
    return this._metadata.fields;
  }
}

type SyncResult =
  | {
      merge?: { inserted?: number; deleted?: number; updated?: number };
    }
  | boolean;

export const syncPipelineToMergeTable = async (
  pipeline: Document[],
  collection: string,
  table: TableName
): Promise<SyncResult> => {
  const dataSync = new BqDataSync(table, { batchSize: 2000 });

  const { db, client: mongoClient } = await mongoConnect();
  const cursor = db
    .collection(collection)
    .aggregate(pipeline, { readPreference: "secondaryPreferred" });

  try {
    await dataSync.init();

    let totalRows = 0;
    let appendRowBatch: JSONObject[] = [];

    dataSync.startLogging(5000, () => ({
      function: 'syncPipelineToMergeTable',
      table,
      totalRows,
      appendRowBatch: appendRowBatch.length,
    }));

    console.log(dataSync.timeElapsed, "Start paging the cursor...");

    for await (const document of cursor) {
      totalRows += 1;
      appendRowBatch.push(document);
      if (appendRowBatch.length >= dataSync.batchSize) {
        dataSync.writeBatch(appendRowBatch);
        appendRowBatch = [];
      }
    }

    console.log(dataSync.timeElapsed, "Finished paging the cursor", totalRows);

    if (appendRowBatch.length) {
      dataSync.writeBatch(appendRowBatch);
    }

    await dataSync.commitWrites();

    await dataSync.mergeTmpTable();

    return true;
  } catch (e) {
    throw e;
  } finally {
    dataSync.stopLogging();
    await Promise.all([
      cursor.close(),
      mongoClient.close(),
      dataSync.closeStream(),
      dataSync.deleteTmpData().then(() => dataSync.countAllTableRows()),
    ]);
  }

  return false;
};

export const syncFindToMergeTable = async (
  cursor: FindCursor,
  transform: (document: Document) => JSONObject,
  table: TableName,
): Promise<SyncResult> => {
  
  const dataSync = new BqDataSync(table, { batchSize: 2000 });
  try {
    await dataSync.init();

    console.log(dataSync.timeElapsed, await dataSync.countAllTableRows());

    let totalRows = 0;
    let appendRowBatch: JSONObject[] = [];

    dataSync.startLogging(5000, () => ({
      function: 'syncFindToMergeTable',
      table,
      totalRows,
      appendRowBatch: appendRowBatch.length,
    }));

    console.log(dataSync.timeElapsed, "Start paging the cursor...");

    for await (const document of cursor) {
      totalRows += 1;
      appendRowBatch.push(transform(document));
      if (appendRowBatch.length >= dataSync.batchSize) {
        dataSync.writeBatch(appendRowBatch);
        appendRowBatch = [];
      }
    }

    console.log(dataSync.timeElapsed, "Finished paging the cursor", totalRows);

    if (appendRowBatch.length) {
      dataSync.writeBatch(appendRowBatch);
    }

    await dataSync.commitWrites();
    await dataSync.mergeTmpTable();

    return true;
  } catch (e) {
    throw e;
  } finally {
    dataSync.stopLogging();
    await Promise.all([
      cursor.close(),
      dataSync.closeStream(),
      dataSync.deleteTmpData().then(() => dataSync.countAllTableRows()),
    ]);
  }

  return false;
}