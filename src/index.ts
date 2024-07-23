import { CloudEventFunction } from "@google-cloud/functions-framework";

import { decodeEventData } from "./lib/util";
import { syncCreditActivity as syncCreditActivityAggregate } from "./lib/sync/credit-activity-aggregate";
import { syncCreditActivity as syncCreditActivityFind } from "./lib/sync/credit-activity-find";
import { syncTransactions } from "./lib/sync/transactions";
import { syncBookingSubmissions } from "./lib/sync/booking_submissions";

export const handler: CloudEventFunction<string> = async (cloudEvent) => {
  const { method, table, options } = decodeEventData(cloudEvent);
  if (!method) {
    console.error("No method found in event data");
    return;
  }
  if (!table) {
    console.error("No table found in event data");
    return;
  }
  if (method === "sync") {
    try {
      const result = await (async () => {
        switch (table) {
          case "credit_activity":
            if (options?.queryOp === "aggregate") {
              return syncCreditActivityAggregate();
            } else {
              return syncCreditActivityFind();
            }
          case "transactions":
            return syncTransactions();
          case "booking_submissions":
            return syncBookingSubmissions();
          default:
            throw new Error(`Table ${table} is not handled`);
        }
      })();
      console.log(`Finished syncing ${table}: ${JSON.stringify(result)}`);
      return result;
    } catch (error) {
      console.error(error);
    }
  }
  console.error(`Unhandled method ${method}`);
};
