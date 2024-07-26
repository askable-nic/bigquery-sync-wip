import { Document, ObjectId } from "mongodb";
import { syncToTable } from "../sync-util";
import { mongoConnect, safeMapLookup } from "../util";
import {
  studyOnlineTaskToolMap,
  studyStatusMap,
  studyTypeMap,
  studyVideoToolMap,
} from "../constants";

/*
Partitioned: Created (DAY)
Clustered by: Status, Type, Askable_Plus
*/

const emojiSegmenter = new Intl.Segmenter("en", { granularity: "grapheme" });
const emojiNameMap: Record<string, string> = {
  /* ✅ */ "2705": "Check_Mark",
  /* 📵 */ "d83d-dcf5": "No_Phones",
  /* 💿 */ "d83d-dcbf": "CD",
  /* 🥵 */ "d83e-dd75": "Overheated_Face",
  /* ❗ */ "2757": "Exclamation_Mark",
  /* ❗️ */ "2757-fe0f": "Exclamation_Mark",
  /* 🧿 */ "d83e-ddff": "Nazar_Amulet",
  /* 📹 */ "d83d-dcf9": "Video_Camera",
  /* ✝️ */ "271d-fe0f": "Cross",
  /* 🚨 */ "d83d-dea8": "Flashing_Light",
  /* ⭐ */ "2b50": "Star",
  /* ⭐️ */ "2b50-fe0f": "Star",
};
const emojiLabels = (emojiTagString: string) =>
  Array.from(emojiSegmenter.segment(emojiTagString)).map((entry) => {
    const unicode = entry.segment
      .split("")
      .map((char) => char.charCodeAt(0).toString(16))
      .join("-");

    return emojiNameMap[unicode] ?? unicode;
  });

export const syncStudies = async () => {
  const { db, client: mongoClient } = await mongoConnect();
  const syncResult = await syncToTable(
    db.collection("booking").find(
      { "config.demo": { $ne: true }, status: { $in: [1, 3, 4, 5, 7] } },
      {
        // sort: { _id: -1 },
        projection: {
          _id: 1,
          _owner_id: 1,
          _project_id: 1,
          _team_id: 1,
          "admin.emoji": 1,
          "admin.tags.nufp": 1,
          "config.credits_per_participant": 1,
          "config.criteria.locations.bounds.country": 1,
          "config.criteria.locations.countries.country": 1,
          "config.criteria.locations.states.country": 1,
          "config.incentive": 1,
          "config.incentives": 1,
          "config.location.country": 1,
          "config.online_task.tool": 1,
          "config.recruitment.byo": 1,
          "config.remote.tool": 1,
          "config.session.duration": 1,
          "session.end": 1,
          "session.start": 1,
          "rating.overall": 1,
          approved_date: 1,
          confirmed_date: 1,
          created: 1,
          fulfilled_date: 1,
          name: 1,
          recruited_date: 1,
          status: 1,
          total_participants: 1,
          type: 1,
          updated: 1,
        },
      }
    ),
    (doc) => {
      const created = doc.created
        ? new Date(doc.created)
        : doc._id.getTimestamp();

      const sessionTimes = ((doc?.session ?? []) as Document[]).reduce(
        (acc: { first: number; last: number }, session) => {
          if (session?.start < acc.first) {
            acc.first = session.start;
          }
          if (session?.end > acc.last) {
            acc.last = session.end;
          }
          return acc;
        },
        { first: Infinity, last: 0 }
      );

      const locations = [] as Document[];
      if (doc?.config?.location?.country) {
        locations.push(doc.config.location);
      }
      try {
        locations.push(...(doc?.config?.criteria?.locations?.bounds ?? []));
        locations.push(...(doc?.config?.criteria?.locations?.states ?? []));
        locations.push(...(doc?.config?.criteria?.locations?.countries ?? []));
      } catch (e) {
        console.warn(`Invalid location data for study ${doc._id}`);
      }

      const incentives = [] as Document[];
      try {
        if (doc?.config?.incentives?.map) {
          incentives.push(
            ...doc.config.incentives
              .filter(
                (incentive: Document) =>
                  incentive.value && incentive.currency_code
              )
              .map((incentive: Document) => ({
                Currency: incentive.currency_code ?? null,
                Amount: incentive.value ?? null,
              }))
          );
        }
        if (doc?.config?.incentive?.value) {
          incentives.push({
            Currency: doc.config.incentive.currency_code ?? null,
            Amount: doc.config.incentive.value ?? null,
          });
        }
      } catch (e) {
        console.warn(`Invalid incentive data for study ${doc._id}`);
      }

      const emojiTagNames = emojiLabels(doc?.admin?.emoji ?? "");

      return {
        ID: doc._id.toString(),
        Created: created,
        Updated: doc.updated ? new Date(doc.updated) : created,
        Submitted: doc.confirmed_date ? new Date(doc.confirmed_date) : null,
        Approved: doc.approved_date ? new Date(doc.approved_date) : null,
        Fulfilled: doc.fulfilled_date ? new Date(doc.fulfilled_date) : null,
        Recruited: doc.recruited_date ? new Date(doc.recruited_date) : null,
        First_Session:
          sessionTimes.first < Infinity ? new Date(sessionTimes.first) : null,
        Last_Session:
          sessionTimes.last > 0 ? new Date(sessionTimes.last) : null,
        Name: doc.name ?? null,
        Team_ID: doc._team_id ? doc._team_id.toString() : null,
        Project_ID: doc._project_id ? doc._project_id.toString() : null,
        Owner_ID: doc._owner_id
          ? (doc._owner_id as ObjectId[])
              .filter(Boolean)
              .map((id) => id.toString())
          : null,
        Status: safeMapLookup(studyStatusMap, doc.status),
        Type: safeMapLookup(studyTypeMap, doc.type),
        Countries: Array.from(
          new Set(
            locations
              .map((loc) => loc.country)
              .filter((country) => typeof country === "string")
          )
        ),
        Incentives: incentives ?? [],
        Credits_Per_Participant: doc?.config?.credits_per_participant ?? null,
        Quota: doc?.total_participants ?? null,
        Rating: doc?.rating?.overall ?? null,
        Session_Duration: doc?.config?.session?.duration ?? null,
        BYO: !!doc?.config?.recruitment?.byo,
        Askable_Plus: emojiTagNames.includes(emojiNameMap["271d-fe0f"]),
        NUFP: !!doc?.admin?.tags?.nufp,
        Video_Tool: doc?.config?.remote?.tool
          ? safeMapLookup(studyVideoToolMap, doc.config.remote.tool)
          : null,
        Online_Task_Tool: doc?.config?.online_task?.tool
          ? safeMapLookup(studyOnlineTaskToolMap, doc.config.online_task.tool)
          : null,
        Emoji_Tags: emojiTagNames,
      };
    },
    "studies"
  );

  await mongoClient.close();

  return syncResult;
};
