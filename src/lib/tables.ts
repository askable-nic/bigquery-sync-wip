import { TableField } from "@google-cloud/bigquery";

// import schema files explicity to make sure they're copied in the build
import creditActivitySchema from "./bigquery_schema/credit_activity.json";

export class BigqueryTable {
  name: string;
  mergeTableName: string;
  idColumn: string;
  schema: TableField[] = [];
  private schemaFieldNames: string[] = [];
  // private schemaFieldDefinitions: Record<string, TableField> = {};
  constructor(name: string, idColumn: string, schemaJson: unknown) {
    this.name = name;
    this.mergeTableName = `${name}_tmp_merge`;
    this.idColumn = idColumn;
    this.schema = schemaJson as TableField[];
    this.schemaFieldNames = this.schema
      .filter((field) => field.name)
      .map((field) => field.name!);
    // this.schema.forEach((field) => {
    //   if (field.name) {
    //     this.schemaFieldNames.push(field.name);
    //     this.schemaFieldDefinitions[field.name] = field;
    //   }
    // });
  }

  get columns() {
    return this.schemaFieldNames;
  }
}

export const CreditActivity = new BigqueryTable(
  "credit_activity",
  "ID",
  creditActivitySchema,
);
