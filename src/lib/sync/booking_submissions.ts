import { ObjectId } from "mongodb";
import { mongoConnect } from "../util";
import { syncToTable } from "./sync-util";

export const syncBookingSubmissions = async () => {
  const { db, client: mongoClient } = await mongoConnect();

  const updatedSince = Date.now() - 1000 * 3600 * 24 * 1; // 1 day

  var i = new ObjectId();
  i.getTimestamp();

  const syncResult = await syncToTable(
    db.collection("booking_submission").find(
      {
        $or: [
          { created: { $gt: updatedSince } },
          { updated: { $gt: updatedSince } },
          { status_updated: { $gt: updatedSince } },
        ],
      },
      {
        sort: { status_updated: -1, updated: 1, _id: -1 },
        projection: {
          _booking_id: 1,
          _id: 1,
          _user_id: 1,
          created: 1,
          eligibility: 1,
          status_updated: 1,
          status: 1,
          updated: 1,
        },
      }
    ),
    (doc) => {
      const lastUpdated = Math.max(
        ...[
          doc.created,
          doc.updated,
          doc.status_updated,
          doc._id.getTimestamp().valueOf(),
        ].filter((v) => typeof v === "number")
      );

      return {
        ID: doc._id.toString(),
        Date: doc.created ? new Date(doc.created) : doc._id.getTimestamp(),
        Updated: new Date(lastUpdated),
        Eligibility: doc.eligibility ?? null,
        User_ID: doc._user_id ? doc._user_id.toString() : null,
        Study_ID: doc._booking_id ? doc._booking_id.toString() : null,
        Status: doc.status ?? null,
      };
    },
    "booking_submissions"
  );

  await mongoClient.close();

  return syncResult;
};
