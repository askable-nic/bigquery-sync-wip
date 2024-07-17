import { syncCreditActivity } from "./credit-activity";
import { TableName } from "../types";

export const syncData = async (table: TableName) => {
  switch (table) {
    case "credit_activity":
      return syncCreditActivity();
    default:
      throw new Error(`Table ${table} is not handled`);
  }
};
