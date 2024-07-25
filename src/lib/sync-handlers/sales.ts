import { syncToTable } from "../sync-util";
import { mongoConnect } from "../util";
import { transactionStatusMap, transactionTypeMap } from "../constants";

export const syncTransactions = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncToTable(
    db.collection("credit_activity").find(
      {},
      {
        // sort: { _id: -1 },
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
          comment : 1,
          created: 1,
          refund_type: 1,
          type: 1,
          updated: 1,
        }
      }
    ),
    (doc) => {
      const createdDate = doc.created ? new Date(doc.created) : doc._id.getTimestamp();
      return {
        ID: doc._id.toString(),
        Created: createdDate,
        Updated: doc.updated ? new Date(doc.updated) : createdDate,
        Team_ID: doc._team_id ? doc._team_id.toString() : null,
        User_ID: doc._user_id ? doc._user_id.toString() : null,
        Admin_User_ID: doc._admin_user_id ? doc._admin_user_id.toString() : null,
        Study_ID: doc._booking_id ? doc._booking_id.toString() : null,
        Amount: typeof doc.amount === 'number' ? (doc.accounting_type === 1 ? doc.amount : doc.amount * -1) : null,
        Currency: doc.currency ?? null,
        Transaction_Type: transactionTypeMap.find(([k]) => k === doc.type)?.[1] ?? null,
        Status: transactionStatusMap.find(([k]) => k === doc.status)?.[1] ?? null,
        Invoice_Number: doc.invoice?.number ?? null,
        Invoice_Credit_Quantity: doc.invoice?.credit_quantity ?? null,
        Legacy: !!doc._legacy_id,
        Comment: doc?.comment ?? null,
      }
    },
    "sales"
  );

  await mongoClient.close();

  return syncResult;
};
