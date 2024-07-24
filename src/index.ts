import { CloudEventFunction } from "@google-cloud/functions-framework";

import { decodeEventData } from "./lib/util";
import { syncCreditActivity as syncCreditActivityAggregate } from "./lib/sync/credit-activity-aggregate";
import { syncCreditActivity as syncCreditActivityFind } from "./lib/sync/credit-activity-find";
import { syncTransactions } from "./lib/sync/transactions";
import { syncBookingSubmissions } from "./lib/sync/booking_submissions";
import { syncTeams } from "./lib/sync/teams";
import { pushExchangeRateData } from "./lib/sync/exchange-rate-data";

import { TableName } from "./lib/types";

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
    const syncHandlers: Record<TableName, () => Promise<unknown>> = {
      credit_activity: syncCreditActivityFind,
      transactions: syncTransactions,
      booking_submissions: syncBookingSubmissions,
      teams: syncTeams,
      exchange_rate_data: pushExchangeRateData,
    };
    try {
      const result = await (async () => {
        if (!syncHandlers[table]) {
          throw new Error(`Table ${table} is not handled`);
        }
        return await syncHandlers[table]();
      })();
      console.log(`Finished syncing ${table}: ${JSON.stringify(result)}`);
      return result;
    } catch (error) {
      console.error(error);
    }
  }
  console.error(`Unhandled method ${method}`);
};
