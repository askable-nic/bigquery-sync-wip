import axios from "axios";
import { env } from "../util";
import { pushRowsToTable } from "../sync-util";

export const pushExchangeRateData = async () => {
  const { data } = await axios.get(
    `https://openexchangerates.org/api/latest.json?app_id=${env.OPENEXCHANGERATES_APP_ID}`
  );

  if (!data?.timestamp || !data?.base || !data?.rates) {
    throw new Error("Invalid response from OpenExchangeRates API");
  }

  const rows = [
    {
      Timestamp: new Date(data.timestamp * 1000),
      Base: data.base,
      Rates: JSON.stringify(data.rates),
    },
  ];

  const syncResult = await pushRowsToTable("exchange_rate_data", rows);

  return syncResult;
};
