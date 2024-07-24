import { TableName } from "./types";

export const creditTypeMap = [
  [0, "Manual adjustment"],
  [1, "Credit usage"],
  [2, "Credit purchase"],
  [3, "Refund"],
  [4, "Refund"],
  [5, "Refund"],
  [6, "Refund"],
  [7, "Refund"],
  [8, "Booking requirement adjustment"],
  [9, "Free / promotional credits"],
  [10, "Expiring credits"],
  [11, "Credit transfer"],
  [12, "Non-booking usage"],
];

export const refundTypeMap = [
  [3, "No-show"],
  [4, "Unfulfilled"],
  [5, "Booking cancelled / rejected"],
  [6, "Bad participant"],
  [7, "Other"],
];

export const studyTypeMap = [
  [1, "In-person"],
  [2, "Remote"],
  [3, "Online task"],
];

export const transactionTypeMap = [
  [1, "Credit sale"],
  [3, "Credit refund"],
];

export const transactionStatusMap = [
  [1, "Paid"],
  [2, "Pending payment"],
  [3, "Cancelled"],
];

export const idFieldName: Record<TableName, string> = {
  credit_activity: "ID",
  transactions: "ID",
  exchange_rate_data: "Date_Key",
  booking_submissions: "ID",
  teams: "ID",
};

export const tableUtilColumns = {
  uuid: '_uuid',
  syncTime: '_sync_time',
};
