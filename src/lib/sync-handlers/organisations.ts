import { syncToTable } from "../sync-util";
import { mongoConnect } from "../util";

/*
Partitioned: _sync_time (HOUR)
Clustered by: (none)
*/

export const syncOrgs = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncToTable(
    db.collection("organisations").find(
      {},
      {
        // sort: { _id: -1 },
        projection: {
          _id: 1,
          name: 1,
        },
      }
    ),
    (doc) => {
      return {
        ID: doc._id.toString(),
        Name: doc.name ?? null,
      };
    },
    "organisations"
  );

  await mongoClient.close();

  return syncResult;
};
