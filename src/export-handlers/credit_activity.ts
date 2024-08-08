import { mongoConnect } from "../util";
import {
  creditActivityQuery,
  creditActivityTransform,
} from "../queries/credit_activity";
import { exportQueryToJsonl } from "../export-util";

export const exportCreditActivity = async () => {
  const { db, client: mongoClient } = await mongoConnect();

  const exportResult = await exportQueryToJsonl(
    creditActivityQuery(db),
    creditActivityTransform,
    "credit_activity"
  );

  await mongoClient.close();

  return exportResult;
};
