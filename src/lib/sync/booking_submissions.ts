import { syncPipelineToMergeTable } from "./sync-util";

export const syncBookingSubmissions = async () => {
  const pipeline = [
    { $sort: { _id: -1 } },
    {
      $project: {
        _id: false,
        ID: { $toString: "$_id" },
        Date: { $toDate: { $ifNull: ["$created", "$_id"] } },
        Eligibility: "$eligibility",
        User_ID: { $toString: "$_user_id" },
        Study_ID: { $toString: "$_booking_id" },
        Status: "$status",
      } }
  ];

  return syncPipelineToMergeTable(pipeline, "booking_submission", "booking_submissions");
};
