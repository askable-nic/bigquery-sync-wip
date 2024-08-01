import { projectStatusMap, projectTypeMap } from "../constants";
import { syncQueryToTable } from "../sync-util";
import { mongoConnect, safeMapLookup } from "../util";

/*
Partitioned: _sync_time (HOUR)
Clustered by: (none)
*/

export const syncProjects = async () => {
  const { db, client: mongoClient } = await mongoConnect();

  const syncResult = await syncQueryToTable(
    db.collection("project").find(
      { type: { $in: Object.keys(projectTypeMap).map(Number) } },
      {
        // sort: { _id: -1 },
        projection: {
          _id: 1,
          created: 1,
          updated: 1,
          name: 1,
          type: 1,
          status: 1,
        },
      }
    ),
    (doc) => {
      const created = doc.created
        ? new Date(doc.created)
        : doc._id.getTimestamp();

      return {
        ID: doc._id.toString(),
        Created: created,
        Updated: doc.updated ? new Date(doc.updated) : created,
        Name: doc.name ?? null,
        Type: safeMapLookup(projectTypeMap, doc.type) || "Other",
        Askable_Plus: doc.type === 1,
        Status: safeMapLookup(projectStatusMap, doc.status),
      };
    },
    "projects"
  );

  await mongoClient.close();

  return syncResult;
};
