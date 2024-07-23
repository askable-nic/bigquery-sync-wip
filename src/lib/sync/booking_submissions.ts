import { mongoConnect } from "../util";
import { syncFindToMergeTable, syncPipelineToMergeTable } from "./sync-util";

export const syncBookingSubmissions = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncFindToMergeTable(
    db.collection("booking_submission").find(
      {},
      {
        sort: { _id: -1 },
        limit: 200000,
        projection: {
          _id: 1,
          created: 1,
          eligibility: 1,
          _user_id: 1,
          _booking_id: 1,
          status: 1,
        },
      }
    ),
    (doc) => ({
      ID: doc._id.toString(),
      Date: doc.created ? new Date(doc.created) : doc._id.getTimestamp(),
      Eligibility: doc.eligibility ?? null,
      User_ID: doc._user_id ? doc._user_id.toString() : null,
      Study_ID: doc._booking_id ? doc._booking_id.toString() : null,
      Status: doc.status ?? null,
    }),
    "booking_submissions"
  );

  await mongoClient.close();

  return syncResult;
};
