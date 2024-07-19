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