const File = require("@saltcorn/data/models/file");
const Table = require("@saltcorn/data/models/table");
const Field = require("@saltcorn/data/models/field");
const { getState } = require("@saltcorn/data/db/state");

const { QvdDataFrame, QvdFileReader } = require("qvd4js");

const numberFormatToType = (nf) => {
  if (nf.Type === "REAL") return "Float";
  if (nf.Type === "INTEGER") return "Integer";
  if (nf.Type === "TIME") return "String";
  if (nf.Type === "UNKNOWN") return "String";
  throw new Error("Unknown NumberFormat: " + JSON.stringify(nf));
};

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
              type: numberFormatToType(field.NumberFormat),
              attributes: {},
            };

            const f = await Field.create(fld);
            field_names.push(f.name);
          }
          await getState.refresh_tables()
        } else {
          field_names = fields.map((f) => Field.labelToName(f.FieldName));
        }
        for (const row of df.data) {
          const o = {};
          for (let index = 0; index < field_names.length; index++) {
            o[field_names[index]] = row[index];
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
    export_to_qvd_file: {
      async run(table_name, where, file_name) {},
      description: "Convert a list of JSON objects to a CSV string",
      isAsync: true,
      arguments: [
        { name: "table_name", type: "String" },
        { name: "where", type: "JSON" },
        { name: "file_name", type: "String" },
      ],
    },
  },
};
