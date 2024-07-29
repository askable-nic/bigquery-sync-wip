import { syncToTable } from "../sync-util";
import { dayDiffMs, mongoConnect, safeMapLookup } from "../util";
import { creditTypeMap, refundTypeMap } from "../constants";

/*
Partitioned: _sync_time (HOUR)
Clustered by: Team_ID, Type
*/

export const syncCreditActivity = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncToTable(
    db.collection("credit_activity").find(
      { updated: { $gt: dayDiffMs(7) } },
      {
        sort: { _id: -1 },
        projection: {
          _admin_user_id: 1,
          _booking_id: 1,
          _from_team_id: 1,
          _id: 1,
          _legacy_id: 1,
          _project_id: 1,
          _team_id: 1,
          _to_team_id: 1,
          _transaction_id: 1,
          _user_id: 1,
          accounting_type: 1,
          amount: 1,
          comment: 1,
          created: 1,
          refund_type: 1,
          type: 1,
          updated: 1,
        },
      }
    ),
    (doc) => {
      const createdDate = doc.created
        ? new Date(doc.created)
        : doc._id.getTimestamp();
      const creditAmount = (() => {
        if (typeof doc.amount !== "number") {
          return null;
        }
        const amount: number =
          doc.accounting_type === 2 ? doc.amount * -1 : doc.amount;
        if (createdDate < new Date("2019-08-01T00:00:00+1000")) {
          return amount * 100;
        }
        return amount;
      })();
      return {
        ID: doc._id.toString(),
        Created: createdDate,
        Updated: doc.updated ? new Date(doc.updated) : createdDate,
        Team_ID: doc._team_id ? doc._team_id.toString() : null,
        User_ID: doc._user_id ? doc._user_id.toString() : null,
        Admin_User_ID: doc._admin_user_id
          ? doc._admin_user_id.toString()
          : null,
        Study_ID: doc._booking_id ? doc._booking_id.toString() : null,
        Project_ID: doc._project_id ? doc._project_id.toString() : null,
        Transaction_ID: doc._transaction_id
          ? doc._transaction_id.toString()
          : null,
        Transfer_From_Team_ID: doc._from_team_id
          ? doc._from_team_id.toString()
          : null,
        Transfer_To_Team_ID: doc._to_team_id
          ? doc._to_team_id.toString()
          : null,
        Credit_Amount: creditAmount,
        Type: safeMapLookup(creditTypeMap, doc.type),
        Credit_Refund_Type: safeMapLookup(refundTypeMap, doc.refund_type),
        Usage: [0, 1, 3, 4, 5, 6, 7, 8, 12].includes(doc.type),
        Legacy: !!doc._legacy_id,
        Comment: doc.comment ?? null,
      };
    },
    "credit_activity"
  );

  await mongoClient.close();

  return syncResult;
};
