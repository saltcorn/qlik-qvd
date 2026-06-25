const File = require("@saltcorn/data/models/file");
const Table = require("@saltcorn/data/models/table");
const Field = require("@saltcorn/data/models/field");

const { readQvd, saveQvd } = require("qvdrs");
//const table = await readQvd("data.qvd");
module.exports = {
  sc_plugin_api_version: 1,
  plugin_name: "qlik-qvd",
  functions: {
    import_qvd_file: {
      async run(file_name, table_name) {
        const file = await File.findOne({ filename: file_name });

        const qvd = await readQvd(file.location);
        console.log("qvd qvd", qvd);
        console.log(`Rows: ${qvd.numRows}, Cols: ${qvd.numCols}`);
        console.log(`tableName: ${qvd.tableName}`);
        console.log(`Columns: ${qvd.columns}`);
      },
      description: "Convert a list of JSON objects to a CSV string",
      isAsync: true,
      arguments: [
        { name: "file_name", type: "String" },
        { name: "table_name", type: "String" },
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
