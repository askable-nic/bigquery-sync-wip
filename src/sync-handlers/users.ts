import { syncQueryToTable } from "../sync-util";
import { dayDiffMs, mongoConnect } from "../util";
import { usersQuery, usersTransform } from "../queries/users";

/*
Partitioned: _sync_time (HOUR)
Clustered by: Type, Country
*/

export const syncUsers = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncQueryToTable(
    usersQuery(db, dayDiffMs(1)),
    usersTransform,
    "users"
  );

  await mongoClient.close();

  return syncResult;
};
