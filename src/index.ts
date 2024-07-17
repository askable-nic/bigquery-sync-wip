import { CloudEventFunction } from "@google-cloud/functions-framework";
import { syncData } from "./lib/data-syncs";

import { decodeEventData } from "./lib/util";

export const handler: CloudEventFunction<string> = async (cloudEvent) => {
  const { table } = decodeEventData(cloudEvent?.data);
  if (!table) {
    console.error('No table found in event data');
    return;
  }
  try {
    const result = await syncData(table);
    console.log(`Finished syncing ${table}: ${JSON.stringify(result)}`);
  } catch (error) {
    console.error(error);
  }
};
