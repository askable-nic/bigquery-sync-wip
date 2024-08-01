import { CloudEventFunction } from "@google-cloud/functions-framework";

import { decodeEventData } from "./lib/util";

// import { syncBookingSubmissions } from "./lib/sync-handlers/booking_submissions";
import { syncCreditActivity } from "./lib/sync-handlers/credit_activity";
import { syncExchangeRateData } from "./lib/sync-handlers/exchange_rates";
import { syncOrgs } from "./lib/sync-handlers/organisations";
import { syncProjects } from "./lib/sync-handlers/projects";
import { syncSales } from "./lib/sync-handlers/sales";
import { syncStudies } from "./lib/sync-handlers/studies";
import { syncTeams } from "./lib/sync-handlers/teams";
import { syncUsers } from "./lib/sync-handlers/users";

import type { TableName } from "./lib/constants";
import type { SyncResult } from "./lib/sync-util";

export const handler: CloudEventFunction<string> = async (cloudEvent) => {
  const { method, table } = decodeEventData(cloudEvent);
  if (!method) {
    console.error("No method found in event data");
    return;
  }
  if (!table) {
    console.error("No table found in event data");
    return;
  }
  if (method === "sync") {
    const syncHandlers: Record<TableName, () => Promise<SyncResult>> = {
      // booking_submissions: syncBookingSubmissions,
      credit_activity: syncCreditActivity,
      exchange_rates: syncExchangeRateData,
      organisations: syncOrgs,
      projects: syncProjects,
      sales: syncSales,
      studies: syncStudies,
      teams: syncTeams,
      users: syncUsers,
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
