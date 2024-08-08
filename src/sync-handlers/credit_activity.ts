import { syncQueryToTable } from "../sync-util";
import { dayDiffMs, mongoConnect } from "../util";
import {
  creditActivityQuery,
  creditActivityTransform,
} from "../queries/credit_activity";

/*
Partitioned: _sync_time (HOUR)
Clustered by: Usage
*/

export const syncCreditActivity = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncQueryToTable(
    creditActivityQuery(db, dayDiffMs(1)),
    creditActivityTransform,
    "credit_activity"
  );

  await mongoClient.close();

  return syncResult;
};
