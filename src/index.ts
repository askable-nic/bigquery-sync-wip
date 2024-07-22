import { CloudEventFunction } from "@google-cloud/functions-framework";

import { decodeEventData } from "./lib/util";
import { mergeTableData } from "./lib/merge";
import { syncCreditActivity } from "./lib/sync/credit-activity";
import { syncTransactions } from "./lib/sync/transactions";

export const handler: CloudEventFunction<string> = async (cloudEvent) => {
  const { method, table } = decodeEventData(cloudEvent?.data);
  if (!method) {
    console.error("No method found in event data");
    return;
  }
  if (!table) {
    console.error("No table found in event data");
    return;
  }
  if (method === "merge") {
    return mergeTableData(table)
      .catch((err) => {
        console.error(err);
      })
      .then((result) => {
        console.log(`Finished merging ${table}: ${JSON.stringify(result)}`);
        return result;
      });
  }
  if (method === "sync") {
    try {
      const result = await (async () => {
        switch (table) {
          case "credit_activity":
            return syncCreditActivity();
          case "transactions":
            return syncTransactions();
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
