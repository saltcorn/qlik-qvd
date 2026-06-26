const File = require("@saltcorn/data/models/file");
const Table = require("@saltcorn/data/models/table");
const Field = require("@saltcorn/data/models/field");
const { getState } = require("@saltcorn/data/db/state");

const { QvdDataFrame, QvdFileReader } = require("qvd4js");

const numberFormatToType = (nf) => {
  if (nf.Type === "REAL") return { type: "Float" };
  if (nf.Type === "INTEGER") return { type: "Integer" };
  if (nf.Type === "TIME") return { type: "String" };
  if (nf.Type === "UNKNOWN") return { type: "String" };
  if (nf.Type === "DATE")
    return {
      type: "Date",
      attributes: nf.Fmt === "DD.MM.YYYY" ? { day_only: true } : {},
    };
  if (nf.Type === "TIMESTAMP")
    return {
      type: "Date",
    };
  throw new Error("Unknown NumberFormat: " + JSON.stringify(nf));
};

const QLIK_EPOCH_MS = Date.UTC(1899, 11, 30); // 1899-12-30 00:00:00 (month is 0-indexed)
const MS_PER_DAY = 86400000;

function fromQlik(serial, { roundToSeconds = true } = {}) {
  let ms = QLIK_EPOCH_MS + serial * MS_PER_DAY;
  if (roundToSeconds) ms = Math.round(ms / 1000) * 1000;
  return new Date(ms);
}

module.exports = {
  sc_plugin_api_version: 1,
  plugin_name: "qlik-qvd",
  functions: {
    import_qvd_file: {
      async run(file_name, table_name) {
        const file = await File.findOne({ filename: file_name });

        const reader = new QvdFileReader(file.location);
        const df = await reader.load();
        const qvdTableName = reader._header.QvdTableHeader.TableName;
        const fields =
          reader._header["QvdTableHeader"]["Fields"]["QvdFieldHeader"];

        let table = Table.findOne({ name: table_name || qvdTableName });
        let field_names = [];
        if (!table) {
          table = await Table.create(table_name || qvdTableName);

          for (const field of fields) {
            const fld = {
              table,
              label: field.FieldName,
              ...numberFormatToType(field.NumberFormat),
            };

            const f = await Field.create(fld);
            field_names.push(f.name);
          }
          await getState().refresh_tables();
        } else {
          field_names = fields.map((f) => Field.labelToName(f.FieldName));
        }
        const timeStampFields = new Set(
          table.fields
            .filter((f) => f.type?.name === "Date" && !f.attributes?.day_only)
            .map((f) => f.name),
        );

        for (const row of df.data) {
          const o = {};
          for (let index = 0; index < field_names.length; index++) {
            const fname = field_names[index];
            const val = row[index];
            if (timeStampFields.has(fname) && typeof val === "number")
              o[fname] = fromQlik(val);
            else o[fname] = val;
          }
          await table.insertRow(o);
        }
      },
      description: "Convert a list of JSON objects to a CSV string",
      isAsync: true,
      arguments: [
        { name: "file_name", type: "String", required: true },
        { name: "table_name", type: "String", required: false },
      ],
    },
    /*export_to_qvd_file: {
      async run(table_name, where, file_name) {},
      description: "Convert a list of JSON objects to a CSV string",
      isAsync: true,
      arguments: [
        { name: "table_name", type: "String" },
        { name: "where", type: "JSON" },
        { name: "file_name", type: "String" },
      ],
    },*/
  },
};
