import { saveAggregationResults, mongoDb } from "../util";
import { creditTypeMap, refundTypeMap, studyTypeMap } from "../enum-mappings";

export const syncCreditActivity = async () => {
  const db = await mongoDb();
  const cursor = db.collection("credit_activity").aggregate(
    [
      { $match: { type: { $in: creditTypeMap.map(([k]) => k).flat() } } },
      { $sort: { _id: -1 } },
      // LIMIT FOR TESTING
      { $limit: 3 },

      // DEFINE LOOKUPS
      {
        $lookup: {
          from: "teams",
          localField: "_team_id",
          foreignField: "_id",
          as: "team",
        },
      },
      { $unwind: "$team" },
      //// EXIT EARLY BASED ON TEAM LOOKUP
      { $match: { "team.settings.ignore_tracking": { $ne: true } } },

      {
        $lookup: {
          from: "booking",
          localField: "_booking_id",
          foreignField: "_id",
          as: "booking",
        },
      },
      { $unwind: { path: "$booking", preserveNullAndEmptyArrays: true } },
      //// EXIT EARLY BASED ON BOOKING LOOKUP
      { $match: { "booking.config.demo": { $ne: true } } },

      {
        $lookup: {
          from: "user",
          localField: "_user_id",
          foreignField: "_id",
          as: "user",
        },
      },
      {
        $lookup: {
          from: "user",
          localField: "_admin_user_id",
          foreignField: "_id",
          as: "admin_user",
        },
      },
      {
        $lookup: {
          from: "project",
          localField: "_project_id",
          foreignField: "_id",
          as: "project",
        },
      },
      {
        $lookup: {
          from: "organisations",
          as: "organisation",
          localField: "team._organisation_id",
          foreignField: "_id",
        },
      },
      { $unwind: { path: "$user", preserveNullAndEmptyArrays: true } },
      { $unwind: { path: "$admin_user", preserveNullAndEmptyArrays: true } },
      { $unwind: { path: "$project", preserveNullAndEmptyArrays: true } },
      { $unwind: { path: "$organisation", preserveNullAndEmptyArrays: true } },

      // HELPER FIELD FOR MISSING created DATE
      {
        $set: {
          created_date: {
            $toDate: { $ifNull: ["$created", "$_id"] },
          },
        },
      },

      // PROJECT TO MATCH THE BIGQUERY SCHEMA
      {
        $project: {
          _id: false,
          ID: { $toString: "$_id" },
          Created: {
            $dateToString: { date: "$created_date" },
          },
          Updated: {
            $dateToString: {
              date: {
                $cond: ["$updated", { $toDate: "$updated" }, "$created_date"],
              },
            },
          },
          Team_ID: { $cond: ["$_team_id", { $toString: "$_team_id" }, null] },
          Team_Name: "$team.name",
          Team_Operational_Office: "$team.operational_office",
          Organisation_ID: {
            $cond: [
              "$team._organisation_id",
              { $toString: "$team._organisation_id" },
              null,
            ],
          },
          Organisation_Name: "$organisation.name",
          Organisation_Label: {
            $cond: [
              "$organisation.name",
              {
                $concat: [
                  "$organisation.name",
                  " | ",
                  { $substr: [{ $toString: "$team._organisation_id" }, 18, 6] },
                ],
              },
              null,
            ],
          },
          Team_Label: {
            $cond: [
              "$organisation.name",
              {
                $concat: [
                  "$organisation.name",
                  " | ",
                  "$team.name",
                  " | ",
                  { $substr: [{ $toString: "$_team_id" }, 18, 6] },
                ],
              },
              {
                $concat: [
                  "$team.name",
                  " | ",
                  { $substr: [{ $toString: "$_team_id" }, 18, 6] },
                ],
              },
            ],
          },
          User_ID: { $cond: ["$_user_id", { $toString: "$_user_id" }, null] },
          User_Email: {
            $cond: ["$user.email", { $toString: "$user.email" }, null],
          },
          User_Name: {
            $trim: {
              input: {
                $concat: [
                  { $ifNull: ["$user.meta.identity.firstname", ""] },
                  " ",
                  { $ifNull: ["$user.meta.identity.lastname", ""] },
                ],
              },
            },
          },
          Admin_User_ID: {
            $cond: ["$_admin_user_id", { $toString: "$_admin_user_id" }, null],
          },
          Admin_User_Email: "$admin_user.email",
          Study_ID: {
            $cond: ["$_booking_id", { $toString: "$_booking_id" }, null],
          },
          Study_Type: {
            $switch: {
              branches: studyTypeMap.map(([k, v]) => ({
                case: { $eq: ["$booking.type", k] },
                then: v,
              })),
              default: null,
            },
          },
          Project_ID: {
            $cond: ["$_project_id", { $toString: "$_project_id" }, null],
          },
          Askable_Plus: {
            $or: [
              { $eq: ["$project.type", 1] },
              {
                $regexMatch: {
                  input: { $ifNull: ["$admin.emoji", ""] },
                  regex: /✝️/,
                },
              },
            ],
          },
          Transaction_ID: {
            $cond: [
              "$_transaction_id",
              { $toString: "$_transaction_id" },
              null,
            ],
          },
          Transfer_From_Team_ID: {
            $cond: ["$_from_team_id", { $toString: "$_from_team_id" }, null],
          },
          Transfer_To_Team_ID: {
            $cond: ["$_to_team_id", { $toString: "$_to_team_id" }, null],
          },
          Credit_Amount: {
            $multiply: [
              {
                $multiply: [
                  "$amount",
                  { $cond: [{ $eq: ["$accounting_type", 2] }, -1, 1] },
                ],
              },
              // x100 if date is before 2019-08-01T00:00:00+1000
              {
                $cond: [
                  {
                    $lt: [
                      "$created_date",
                      new Date("2019-08-01T00:00:00+1000"),
                    ],
                  },
                  100,
                  1,
                ],
              },
            ],
          },
          Type: {
            $switch: {
              branches: creditTypeMap.map(([k, v]) => ({
                case: { $eq: ["$type", k] },
                then: v,
              })),
              default: null,
            },
          },
          Credit_Refund_Type: {
            $switch: {
              branches: refundTypeMap.map(([k, v]) => ({
                case: { $eq: ["$refund_type", k] },
                then: v,
              })),
              default: null,
            },
          },
          Usage: { $in: ["$type", [0, 1, 3, 4, 5, 6, 7, 8, 12]] },
          Legacy: { $cond: ["$_legacy_id", true, false] },
          Comment: "$comment",
        },
      },
    ],
    { readPreference: "secondaryPreferred" }
  );
  return saveAggregationResults(cursor, "credit_activity");
};
