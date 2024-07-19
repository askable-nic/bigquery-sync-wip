import { BigQuery, TableField } from "@google-cloud/bigquery";

import { TableName } from "./types";
import { env, mergeTableName } from "./util";
import { idFieldName } from "./constants";

// const mergeQuery = [
//   `MERGE ${BIGQUERY_DATASET}.${table.name} T`,
//   `USING ${BIGQUERY_DATASET}.${mergeTableName} S`,
// ].join('\n')

// merge tmpMergeTable into table
/*
    MERGE ${BIGQUERY_DATASET}.${table} T
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

type TableMetadata = {
  tableReference: { projectId?: string; datasetId?: string; tableId?: string };
  streamingBuffer?: {
    estimatedRows: string;
    estimatedBytes: string;
    oldestEntryTime: string;
  };
  schema?: { fields?: TableField[] };
};

const fetchMetadata = async (
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

export const mergeTableData = async (table: TableName) => {
  const { BIGQUERY_DATASET } = env;
  const client = new BigQuery();

  const [mainTableMetaData, tmpTableMetaData] = await Promise.all([
    fetchMetadata(client, table),
    fetchMetadata(client, mergeTableName(table)),
  ]);

  if (
    !(
      tmpTableMetaData.tableReference?.datasetId &&
      tmpTableMetaData.tableReference?.tableId &&
      tmpTableMetaData.streamingBuffer &&
      tmpTableMetaData.schema?.fields
    )
  ) {
    throw new Error(`Table metadata for ${mergeTableName(table)} not found`);
  }

  const fieldNames = tmpTableMetaData.schema.fields
    .filter((field) => field.name)
    .map((field) => field.name as string);

  const idField = idFieldName[table];
  if (!idField || !fieldNames.includes(idField)) {
    throw new Error(`Missing/Invalid ID field for table ${table}`);
  }

  const mergeStatement = [
    `MERGE ${BIGQUERY_DATASET}.${table} T`,
    // SELECT DISTINCT BY ${ID}
    `USING ${BIGQUERY_DATASET}.${mergeTableName} S`,
    `ON T.${idField} = S.${idField}`,
    // Exists in both tables
    `WHEN MATCHED THEN`,
    `UPDATE SET `,
    fieldNames.map((field) => `T.${field} = S.${field}`).join(", "),
    // Exists in source but not target
    `WHEN NOT MATCHED BY TARGET THEN`,
    `INSERT (${fieldNames.join(", ")})`,
    `VALUES (${fieldNames.map((field) => `S.${field}`).join(", ")})`,
    // Exists in target but not source
    `WHEN NOT MATCHED BY SOURCE THEN DELETE`,
  ].join(" ");

  console.log(mergeStatement);
};
