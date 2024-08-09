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

import { exportCreditActivity } from "./export-handlers/credit_activity";
import { exportUsers } from "./export-handlers/users";

import type { TableName } from "./constants";
import type { SyncResult } from "./sync-util";
import { ExportResult } from "./export-util";

export const main = async () => {
  const { table, method } = jobParams();
  if (!table) {
    console.log(process.env); // TODO: remove
    console.log(process.argv); // TODO: remove
    throw new Error("Table name is required");
  }
  switch (method) {
    case "sync": {
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
    }
    case "export": {
      const exportHandlers: Partial<
        Record<TableName, () => Promise<ExportResult>>
      > = {
        credit_activity: exportCreditActivity,
        users: exportUsers,
      };
      const result = await (async () => {
        if (!exportHandlers[table]) {
          throw new Error(`Table ${table} is not handled`);
        }
        return await exportHandlers[table]();
      })();
      console.log(`Finished exporting ${table}`);
      return result;
    }
    default:
      throw new Error(`Unknown method: ${method}`);
  }
};

main()
  .then((result) => {
    if(result.success) {
      console.log("Job complete", result);
      process.exit(0);
    } else {
      console.error("Job failed", result);
      process.exit(1);
    }
  })
  .catch((err) => {
    console.error(err);
    process.exit(1); // Retry Job Task by exiting the process
  });
