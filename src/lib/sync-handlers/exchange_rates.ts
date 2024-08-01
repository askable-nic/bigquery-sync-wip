import axios from "axios";
import { env } from "../util";
import { syncRowsToTable } from "../sync-util";
import { JSONObject } from "@google-cloud/bigquery-storage/build/src/managedwriter/json_writer";

/*
Partitioned: Timestamp (DAY)
Clustered by: Base
*/

export const syncExchangeRateData = async () => {
  const { data } = await axios.get(
    `https://openexchangerates.org/api/latest.json?app_id=${env.OPENEXCHANGERATES_APP_ID}`
  );

  if (!data?.timestamp || !data?.base || !data?.rates) {
    throw new Error("Invalid response from OpenExchangeRates API");
  }

  if (data.base !== "USD") {
    // TODO: convert to USD base and continue
    throw new Error("Unexpected base currency from OpenExchangeRates API");
  }

  const rates: Record<string, number> = data.rates;

  const date = new Date(data.timestamp * 1000);
  // const date = new Date("2024-07-30");
  const dateString = date.toISOString().split("T")[0];

  const rows = Object.entries(rates).map(([Currency, Rate_USD]) => {
    if (
      typeof Currency !== "string" ||
      typeof Rate_USD !== "number" ||
      !(Rate_USD > 0)
    ) {
      return undefined;
    }
    return {
      ID: `${dateString}_${Currency}`,
      Timestamp: date,
      Currency,
      Rate_To_USD: 1 / Rate_USD,
      Rate_To_AUD: rates.AUD ? rates.AUD / Rate_USD : null,
      Rate_To_GBP: rates.GBP ? rates.GBP / Rate_USD : null,
    };
  });

  const syncResult = await syncRowsToTable(
    rows.filter(Boolean) as JSONObject[],
    "exchange_rates"
  );

  return syncResult;
};
