import { transactionStatusMap, transactionTypeMap } from "../constants";
import { syncQueryToTable } from "../sync-util";
import { mongoConnect, round, safeMapLookup } from "../util";

/*
Partitioned: _sync_time (HOUR)
*/

export const syncSales = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncQueryToTable(
    db.collection("transactions").find(
      { type: { $in: [1, 3] } },
      {
        // sort: { _id: -1 },
        projection: {
          _admin_user_id: 1,
          _booking_id: 1,
          _id: 1,
          _legacy_id: 1,
          _team_id: 1,
          _user_id: 1,
          "invoice.credit_quantity": 1,
          "invoice.number": 1,
          accounting_type: 1,
          total_amount: 1,
          comment: 1,
          currency: 1,
          status: 1,
          type: 1,
          updated: 1,
        },
      }
    ),
    (doc) => {
      const createdDate = doc.created
        ? new Date(doc.created)
        : doc._id.getTimestamp();

      const amountTotal =
        typeof doc.total_amount === "number"
          ? doc.accounting_type === 1
            ? doc.total_amount
            : doc.total_amount * -1
          : null;

      const currency: string | null = doc.currency ?? null;

      if (amountTotal === null || !currency) {
        return undefined;
      }

      const amountExTax = (() => {
        if (currency === "AUD") {
          return amountTotal / 1.1; // 10% GST
        }
        if (currency === "GBP") {
          return amountTotal / 1.2; // 20% VAT
        }
        return amountTotal;
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
        Amount: round(amountExTax, 6),
        Amount_Inc_Tax: round(amountTotal, 6),
        // Amount_Ex_Tax: amountExTax,
        Currency: doc.currency ?? null,
        Transaction_Type: safeMapLookup(transactionTypeMap, doc.type),
        Status: safeMapLookup(transactionStatusMap, doc.status),
        Invoice_Number: doc.invoice?.number ?? null,
        Invoice_Credit_Quantity: doc.invoice?.credit_quantity ?? null,
        Legacy: !!doc._legacy_id,
        Comment: doc?.comment ?? null,
      };
    },
    "sales"
  );

  await mongoClient.close();

  return syncResult;
};
