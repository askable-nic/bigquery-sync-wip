import { CloudEventFunction } from "@google-cloud/functions-framework";

import { decodeEventData } from "./lib/util";
import { syncCreditActivity } from "./lib/credit_activity/credit_activity";

export const handler: CloudEventFunction<string> = async (cloudEvent) => {
  const { table } = decodeEventData(cloudEvent?.data);
  if (!table) {
    console.error("No table found in event data");
    return;
  }
  try {
    const result = await (async () => {
      switch (table) {
        case "credit_activity":
          return syncCreditActivity();
        default:
          throw new Error(`Table ${table} is not handled`);
      }
    })();
    console.log(`Finished syncing ${table}: ${JSON.stringify(result)}`);
  } catch (error) {
    console.error(error);
  }
};
