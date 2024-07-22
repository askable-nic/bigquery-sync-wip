import { syncPipelineToMergeTable } from "./sync-util";
import { transactionStatusMap, transactionTypeMap } from "../constants";

export const syncTransactions = async () => {
  const pipeline = [
    { $match: { type: { $in: transactionTypeMap.map(([k]) => k).flat() } } },
    { $sort: { _id: -1 } },

    {
      $lookup: {
        from: "teams",
        localField: "_team_id",
        foreignField: "_id",
        as: "team",
      },
    },
    { $unwind: "$team" },
    { $match: { "team.settings.ignore_tracking": { $ne: true } } },

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
        from: "booking",
        localField: "_booking_id",
        foreignField: "_id",
        as: "booking",
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
    // {
    //   $lookup: {
    //     from: "coupon",
    //     localField: "_coupon_id",
    //     foreignField: "_id",
    //     as: "coupon",
    //   },
    // },

    { $unwind: { path: "$user", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$admin_user", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$project", preserveNullAndEmptyArrays: true } },
    { $unwind: { path: "$organisation", preserveNullAndEmptyArrays: true } },
    // { $unwind: { path: "$coupon", preserveNullAndEmptyArrays: true } },

    {
      $set: {
        created_date: {
          $toDate: { $ifNull: ["$created", "$_id"] },
        },
      },
    },

    {
      $project: {
        _id: false,
        ID: { $toString: "$_id" },
        Created: "$created_date",
        Updated: {
          $cond: ["$updated", { $toDate: "$updated" }, "$created_date"],
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

        Amount: {
          $multiply: [
            "$total_amount",
            { $cond: [{ $eq: ["$accounting_type", 2] }, -1, 1] },
          ],
        },
        Currency: "$currency",
        Transaction_Type: {
          $switch: {
            branches: transactionTypeMap.map(([k, v]) => ({
              case: { $eq: ["$type", k] },
              then: v,
            })),
            default: null,
          },
        },
        Status: {
          $switch: {
            branches: transactionStatusMap.map(([k, v]) => ({
              case: { $eq: ["$status", k] },
              then: v,
            })),
            default: null,
          },
        },

        Invoice_Number: "$invoice.number",
        Invoice_Credit_Quantity: "$invoice.credit_quantity",

        Legacy: { $cond: ["$_legacy_id", true, false] },
        Comment: "$comment",
      },
    },
  ];

  return syncPipelineToMergeTable(pipeline, "transactions", "transactions");
};
