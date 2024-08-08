import { mongoConnect } from "../util";
import { usersQuery, usersTransform } from "../queries/users";
import { exportQueryToJsonl } from "../export-util";

export const exportUsers = async () => {
  const { db, client: mongoClient } = await mongoConnect();

  const exportResult = await exportQueryToJsonl(
    usersQuery(db),
    usersTransform,
    "users"
  );

  await mongoClient.close();

  return exportResult;
};
