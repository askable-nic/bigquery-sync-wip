import { jobParams } from "./util";

// import { syncBookingSubmissions } from "./lib/sync-handlers/booking_submissions";
import { syncCreditActivity } from "./sync-handlers/credit_activity";
import { syncExchangeRateData } from "./sync-handlers/exchange_rates";
import { syncOrgs } from "./sync-handlers/organisations";
import { syncProjects } from "./sync-handlers/projects";
import { syncSales } from "./sync-handlers/sales";
import { syncStudies } from "./sync-handlers/studies";
import { syncTeams } from "./sync-handlers/teams";
import { syncUsers } from "./sync-handlers/users";

import type { TableName } from "./constants";
import type { SyncResult } from "./sync-util";

export const main = async () => {
  const { table } = jobParams();
  if (!table) {
    console.log(process.env); // TODO: remove
    console.log(process.argv); // TODO: remove
    throw new Error("Table name is required");
  }
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
  const result = await (async () => {
    if (!syncHandlers[table]) {
      throw new Error(`Table ${table} is not handled`);
    }
    return await syncHandlers[table]();
  })();
  console.log(`Finished syncing ${table}: ${JSON.stringify(result)}`);
  return result;
};

main()
  .then((result) => {
    console.log("Sync complete", result);
    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1); // Retry Job Task by exiting the process
  });
