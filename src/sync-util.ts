import { BigQuery, TableField } from "@google-cloud/bigquery";
import { adapt, managedwriter } from "@google-cloud/bigquery-storage";
import { JSONObject } from "@google-cloud/bigquery-storage/build/src/managedwriter/json_writer";
import { PendingWrite } from "@google-cloud/bigquery-storage/build/src/managedwriter/pending_write";
import { StreamConnection } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_connection";
import { WriteStream } from "@google-cloud/bigquery-storage/build/src/managedwriter/stream_types";
import { Document, FindCursor } from "mongodb";

import { env, tmpTableName } from "./util";

import { TableName, tableUtilColumns } from "./constants";

type BqDataSyncOptions = {
  batchSize?: number;
  useTmpTable?: boolean;
};

class BqDataSync {
  startTime: number = 0;

  tableName: string;
  writeStreamTableName: string;
  idField: string | null = "ID";

  _metadata?: {
    projectId: string;
    datasetId: string;
    tableId: string;
    fields: TableField[];
  };

  stats: SyncResultStats = {};

  hasUuidField = false;
  hasSyncTimeField = false;

  client: BigQuery;
  destinationTable?: string;
  writeClient?: managedwriter.WriterClient;
  writeStream?: WriteStream;
  connection?: StreamConnection;
  writer?: managedwriter.JSONWriter;
  writePromises: ReturnType<PendingWrite["getResult"]>[] = [];
  pwOffset = 0;

  writeBatchSize: number;
  useTmpTable: boolean;

  ready = false;
  loggerInterval: NodeJS.Timeout | null = null;

  uuidsSet = false;

  constructor(tableName: TableName, options: BqDataSyncOptions = {}) {
    this.startTime = Date.now();

    this.writeBatchSize = options.batchSize ?? 1000;
    this.useTmpTable = options.useTmpTable ?? false;

    this.tableName = tableName; // main table to end up with the data
    this.writeStreamTableName = this.useTmpTable
      ? tmpTableName(tableName)
      : tableName; // table to stream data into, which may be a tmp table

    this.client = new BigQuery();
  }

  async init() {
    const { BIGQUERY_DATASET } = env;
    const { WriterClient, JSONWriter } = managedwriter;

    const [tableMetaData] = await this.client
      .dataset(BIGQUERY_DATASET)
      .table(this.writeStreamTableName)
      .getMetadata();

    this._metadata = {
      projectId: tableMetaData.tableReference?.projectId as string,
      datasetId: tableMetaData.tableReference?.datasetId as string,
      tableId: tableMetaData.tableReference?.tableId as string,
      fields: (tableMetaData?.schema?.fields ?? []) as TableField[],
    };

    if (this.useTmpTable) {
      const existingTmpTableRows = await this.countRows(
        this.writeStreamTableName
      );

      console.log(this.timeElapsed, { existingTmpTableRows });

      if (existingTmpTableRows && existingTmpTableRows > 0) {
        throw new Error(
          `Merge table ${this.writeStreamTableName} is not empty (${existingTmpTableRows} rows)`
        );
      }
    }

    this.hasUuidField = !!this._metadata.fields.find(
      (field) => field.name === tableUtilColumns.uuid
    );
    this.hasSyncTimeField = !!this._metadata.fields.find(
      (field) => field.name === tableUtilColumns.syncTime
    );
    if (!this._metadata.fields.find((field) => field.name === this.idField)) {
      this.idField = null;
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

  startLogging(timeout: number, extra?: () => Record<string, unknown>) {
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

  async appendRow(pw: PendingWrite) {
    try {
      const result = await pw.getResult();
      if (result.error) {
        console.error(result.error);
        console.error(result.rowErrors);
        throw new Error(
          `[${result.error?.code || "?"}] ${
            result.error?.message || "Unknown error"
          }`
        );
      }
      return result;
    } catch (e) {
      console.error("Error appending rows", e);
      process.exit(1);
    }
  }

  writeBatch(rows: JSONObject[]) {
    if (!this.ready || !this.writer) {
      throw new Error("Not initialized");
    }
    const pw = this.writer.appendRows(rows, this.pwOffset);
    this.pwOffset += rows.length;
    this.writePromises.push(this.appendRow(pw));
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

    if (connectionFinalizeResult?.rowCount) {
      this.stats.writeStreamInserts = Number(connectionFinalizeResult.rowCount);
    }

    // const finalizeResponse = await this.writeClient.finalizeWriteStream({
    //   name: this.writeStream.name,
    // });

    // console.log(this.timeElapsed, "Write stream finalized", finalizeResponse);

    const commitResponse = await this.writeClient.batchCommitWriteStream({
      parent: this.destinationTable,
      writeStreams: [this.writeStream.name],
    });

    console.log(this.timeElapsed, "Write stream committed", commitResponse);

    await this.setUuids();

    console.log(this.timeElapsed, await this.countAllTableRows());
  }

  async setUuids() {
    if (!this.fields) {
      throw new Error("Not initialized");
    }
    if (
      !this.idField ||
      !this.hasUuidField ||
      !this.hasSyncTimeField ||
      this.uuidsSet
    ) {
      return;
    }
    console.log(this.timeElapsed, "Setting UUIDs...");
    const [, , result] = await this.client.query(
      `UPDATE \`${this.datasetId}.${this.writeStreamTableName}\` SET \`${
        tableUtilColumns.uuid
      }\` = GENERATE_UUID(), \`${
        tableUtilColumns.syncTime
      }\` = TIMESTAMP('${new Date(this.startTime).toJSON()}') WHERE \`${
        tableUtilColumns.uuid
      }\` IS NULL OR \`${tableUtilColumns.syncTime}\` IS NULL;`
    );
    this.uuidsSet = true;
    console.log(this.timeElapsed, "UUIDs set", {
      numDmlAffectedRows: result?.numDmlAffectedRows,
      dmlStats: (result as unknown as { dmlStats: unknown })?.dmlStats,
    });
  }

  async mergeTmpTable() {
    if (!this.ready) {
      throw new Error("Not initialized");
    }
    if (!this.idField) {
      throw new Error("Can only merge tables with an ID fieldx");
    }

    console.log(this.timeElapsed, "Merging tmp table...");

    const mergeStatement = [
      `MERGE \`${this.datasetId}.${this.tableName}\` T`,
      // `USING \`${BIGQUERY_DATASET}.${tmpTableName(table)}\` S`,
      `USING (SELECT * FROM \`${this.datasetId}.${this.writeStreamTableName}\` QUALIFY ROW_NUMBER() OVER (PARTITION BY ID) = 1) S`,
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

    await this.client.query(mergeStatement);

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
    if (!this.useTmpTable) {
      return;
    }
    if (!this.datasetId) {
      throw new Error("Not initialized");
    }

    console.log(this.timeElapsed, "Deleting tmp data...");

    await this.client.query(
      `DELETE FROM ${this.datasetId}.${this.writeStreamTableName} WHERE TRUE`
    );
  }

  async dedupeWriteTable() {
    if (!this.uuidsSet) {
      throw new Error("UUIDs have not been set");
    }
    if (!this.idField) {
      throw new Error("Can only dedupe tables with an ID field");
    }
    console.log(this.timeElapsed, "Deduping table...");
    const queryStatement = [
      `DELETE FROM \`${this.datasetId}.${this.writeStreamTableName}\``,
      `WHERE \`${tableUtilColumns.uuid}\` IN`,
      "(",
      `SELECT \`${tableUtilColumns.uuid}\``,
      `FROM \`${this.datasetId}.${this.writeStreamTableName}\``,
      "WHERE ID IN",
      "(",
      `SELECT ID FROM \`${this.datasetId}.${
        this.writeStreamTableName
      }\` WHERE \`${tableUtilColumns.syncTime}\` = TIMESTAMP('${new Date(
        this.startTime
      ).toJSON()}')`,
      ")",
      `QUALIFY ROW_NUMBER() OVER (PARTITION BY \`${this.idField}\` ORDER BY \`${tableUtilColumns.syncTime}\` DESC) > 1`,
      ")",
    ].join(" ");
    const [, , result] = await this.client.query(queryStatement);

    console.log(this.timeElapsed, "Table deduped", {
      numDmlAffectedRows: result?.numDmlAffectedRows,
      dmlStats: (result as unknown as { dmlStats: unknown })?.dmlStats,
    });

    if (result?.numDmlAffectedRows) {
      this.stats.dedupedRows = Number(result.numDmlAffectedRows);
    }

    console.log(this.timeElapsed, await this.countAllTableRows());
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
    const tableNames = [this.tableName, this.writeStreamTableName].reduce(
      (acc, cur) => {
        if (acc.includes(cur)) {
          return acc;
        }
        return [...acc, cur];
      },
      [] as string[]
    );

    const counts = await Promise.all(
      tableNames.map((table) =>
        this.countRows(table).then(
          (count) => [table, count] as [string, number | undefined]
        )
      )
    );

    return counts.reduce((acc, [table, count]) => {
      return { ...acc, [table]: count };
    }, {});
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

type SyncResultStats = Record<string, number | undefined>;
export type SyncResult =
  | { success: false; error?: unknown }
  | { success: true; stats: SyncResultStats };

export const syncQueryToTable = async (
  cursor: FindCursor,
  transform: (document: Document) => JSONObject | undefined,
  table: TableName
): Promise<SyncResult> => {
  const dataSync = new BqDataSync(table, {
    batchSize: 2000,
    useTmpTable: false,
  });
  try {
    await dataSync.init();

    dataSync.stats.initialTableSize = await dataSync.countRows(table);
    console.log(dataSync.timeElapsed, await dataSync.countAllTableRows());

    let totalRows = 0;
    let appendRowBatch: JSONObject[] = [];

    dataSync.startLogging(5000, () => ({
      function: "syncFindToTable",
      table,
      totalRows,
      appendRowBatch: appendRowBatch.length,
    }));

    console.log(dataSync.timeElapsed, "Start paging the cursor...");

    for await (const document of cursor) {
      totalRows += 1;
      const row = transform(document);
      if (!row) {
        continue;
      }
      appendRowBatch.push(row);
      if (appendRowBatch.length >= dataSync.writeBatchSize) {
        dataSync.writeBatch(appendRowBatch);
        appendRowBatch = [];
      }
    }

    console.log(dataSync.timeElapsed, "Finished paging the cursor", totalRows);
    dataSync.stats.queriedRows = totalRows;

    if (appendRowBatch.length) {
      dataSync.writeBatch(appendRowBatch);
    }

    await dataSync.commitWrites();

    await dataSync.dedupeWriteTable().catch((e) => {
      console.warn("Failed to dedupe table", e);
    });

    dataSync.stats.finalTableSize = await dataSync.countRows(table);

    return { success: true, stats: dataSync.stats };
  } catch (e) {
    console.error("Error syncing to table", e);
    return { success: false, error: e };
  } finally {
    dataSync.stopLogging();
    await Promise.all([cursor.close(), dataSync.closeStream()]);
  }
};

export const syncRowsToTable = async (
  rows: JSONObject[],
  table: TableName
): Promise<SyncResult> => {
  const dataSync = new BqDataSync(table, {
    batchSize: 2000,
    useTmpTable: false,
  });
  try {
    await dataSync.init();

    dataSync.stats.initialTableSize = await dataSync.countRows(table);
    console.log(dataSync.timeElapsed, await dataSync.countAllTableRows());

    dataSync.startLogging(5000, () => ({
      function: "syncFindToTable",
      table,
    }));

    await dataSync.writeBatch(rows);
    await dataSync.commitWrites();

    await dataSync.dedupeWriteTable().catch((e) => {
      console.warn("Failed to dedupe table", e);
    });

    dataSync.stats.finalTableSize = await dataSync.countRows(table);

    return { success: true, stats: dataSync.stats };
  } catch (e) {
    console.error("Error syncing to table", e);
    return { success: false, error: e };
  } finally {
    dataSync.stopLogging();
    await dataSync.closeStream();
  }
};

// For pushing a small number of rows, with no dedupe logic
export async function pushRowsToTable(table: TableName, rows: JSONObject[]) {
  const dataSync = new BqDataSync(table);
  try {
    await dataSync.init();
    await dataSync.writeBatch(rows);
    await dataSync.commitWrites();
    return { success: true, stats: dataSync.stats };
  } catch (e) {
    console.error("Error syncing to table", e);
    return { success: false, error: e };
  } finally {
    await dataSync.closeStream();
  }
}
