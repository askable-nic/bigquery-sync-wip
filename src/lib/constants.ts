export type TableName =
  | "credit_activity"
  | "exchange_rates"
  | "organisations"
  | "projects"
  | "sales"
  | "studies"
  | "teams"
  | "users";

export const creditTypeMap = {
  0: "Manual adjustment",
  1: "Credit usage",
  2: "Credit purchase",
  3: "Refund",
  4: "Refund",
  5: "Refund",
  6: "Refund",
  7: "Refund",
  8: "Booking requirement adjustment",
  9: "Free / promotional credits",
  10: "Expiring credits",
  11: "Credit transfer",
  12: "Non-booking usage",
};

export const refundTypeMap = {
  3: "No-show",
  4: "Unfulfilled",
  5: "Booking cancelled / rejected",
  6: "Bad participant",
  7: "Other",
};

export const transactionTypeMap = {
  1: "Credit sale",
  3: "Credit refund",
};

export const transactionStatusMap = {
  1: "Paid",
  2: "Pending payment",
  3: "Cancelled",
};

export const studyStatusMap = {
  0: "Draft",
  1: "Active",
  3: "In review",
  4: "Rejected",
  5: "Completed",
  7: "Archived",
};

export const studyTypeMap = {
  1: "In-person",
  2: "Remote",
  3: "Online task",
};

export const studyVideoToolMap = {
  askableLive: "Askable Sessions",
  ciscoWebex: "Webex",
  googleMeet: "Google Meet",
  hearsay: "Hearsay",
  lookback: "Lookback",
  loop11: "Loop11",
  microsoftTeams: "Microsoft Teams",
  skype: "Skype",
  validately: "Validately",
  zoom: "Zoom",
  other: "Other",
};

export const studyOnlineTaskToolMap = {
  dscout: "dscout",
  googleForms: "Google Forms",
  lookback: "Lookback",
  loop11: "Loop11",
  maze: "Maze",
  optimalWorkshop: "Optimal Workshop",
  qualtrics: "Qualtrics",
  surveyGizmo: "SurveyGizmo",
  surveyMonkey: "SurveyMonkey",
  typeform: "Typeform",
  usabilityHub: "UsabilityHub",
  userZoom: "UserZoom",
  validately: "Validately",
  other: "Other",
};

export const idFieldName: Record<TableName, string | null> = {
  credit_activity: "ID",
  sales: "ID",
  exchange_rates: null,
  booking_submissions: "ID",
  teams: "ID",
  organisations: "ID",
};

export const tableUtilColumns = {
  uuid: "_uuid",
  syncTime: "_sync_time",
};
