import { Db, Document } from "mongodb";

export const usersQuery = (db: Db, lastUpdated?: number) => {
  return db.collection("user").find(
    {
      status: 1,
      ...lastUpdated && { updated: { $gt: lastUpdated } }
    },
    {
      // sort: { _id: -1 },
      projection: {
        _id: 1,
        "location.country": 1,
        "meta.identity.firstname": 1,
        "meta.identity.lastname": 1,
        blacklist: 1,
        created: 1,
        type: 1,
        updated: 1,
      },
    }
  );
};

export const usersTransform = (doc: Document) => {
  const createdDate = doc.created
    ? new Date(doc.created)
    : doc._id.getTimestamp();

  const type = doc.type?.participant
    ? "Participant"
    : doc.type?.researcher
    ? "Researcher"
    : doc.type?.client
    ? "Client"
    : null;

  if (!type) {
    return undefined;
  }

  return {
    ID: doc._id.toString(),
    Created: createdDate,
    Updated: doc.updated ? new Date(doc.updated) : createdDate,
    Type: type,
    Name: doc?.meta?.identity
      ? `${doc.meta.identity?.firstname} ${doc.meta.identity?.lastname}`.trim()
      : null,
    Country: doc?.location?.country ?? null,
    Participant_Blacklist: doc.type?.participant ? !!doc?.blacklist : null,
  };
};
