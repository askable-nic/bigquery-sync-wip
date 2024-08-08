import fs from "fs";
import path from "path";
import { TableName } from "./constants";
import { FindCursor } from "mongodb";

const EXPORT_DIR = "../export-data/";

const exportPath = (table: TableName) =>
  path.resolve(__dirname, EXPORT_DIR, `${table}.jsonl`);

const resetJsonl = (path: string) => {
  fs.writeFileSync(path, "");
};

const pushRowToJsonl = (path: string, row: object) => {
  fs.appendFileSync(path, JSON.stringify(row) + "\n");
};

export type ExportResult =
  | { success: false; error?: unknown }
  | { success: true; rows: number };

export const exportQueryToJsonl = async (
  cursor: FindCursor,
  transform: (document: Document) => object | undefined,
  table: TableName
): Promise<ExportResult> => {
  const startTime = Date.now();
  const timeElapsed = () => `${((Date.now() - startTime) / 1000).toFixed(1)}s`;
  let loggerInterval: NodeJS.Timeout | null = null;

  try {
    const tablePath = exportPath(table);
    resetJsonl(tablePath);
    let totalRows = 0;
    console.log("Start paging the cursor...");

    loggerInterval = setInterval(() => {
      console.log({
        timeElapsed: timeElapsed(),
        totalRows,
      });
    }, 5000);

    for await (const document of cursor) {
      totalRows += 1;
      const row = transform(document);
      if (!row) {
        continue;
      }
      pushRowToJsonl(tablePath, row);
    }

    console.log(timeElapsed(), "Finished paging the cursor", totalRows);

    return { success: true, rows: totalRows };
  } catch (e) {
    console.error("Error exporting table", e);
    return { success: false, error: e };
  } finally {
    if (loggerInterval) {
      clearInterval(loggerInterval);
    }
    await cursor.close();
  }
};
