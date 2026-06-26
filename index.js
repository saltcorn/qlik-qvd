const File = require("@saltcorn/data/models/file");
const Table = require("@saltcorn/data/models/table");
const Field = require("@saltcorn/data/models/field");
const { getState } = require("@saltcorn/data/db/state");
const db = require("@saltcorn/data/db");

const { Readable } = require("stream");

const { QvdDataFrame, QvdFileReader } = require("qvd4js");

// Serialise a single value into a CSV cell suitable for a Postgres
// `COPY ... FROM STDIN CSV HEADER` ingest. An empty cell becomes NULL.
const csvCell = (v) => {
  if (v === null || v === undefined) return "";
  let s;
  if (v instanceof Date) s = v.toISOString();
  else s = String(v);
  if (/[",\n\r]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
  return s;
};

const numberFormatToType = (nf) => {
  if (nf.Type === "REAL") return { type: "Float" };
  if (nf.Type === "FIX") return { type: "Float" };
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

        // Convert the raw QVD value for a given target field to the value we
        // want stored, applying the Qlik-serial -> Date conversion for
        // timestamp fields (mirrors the row-by-row path below).
        const convertValue = (fname, val) => {
          if (timeStampFields.has(fname) && typeof val === "number")
            return fromQlik(val);
          return val;
        };

        // Fast path: bulk-load via Postgres COPY when the driver supports it.
        // db.copyFrom is only defined on the Postgres driver, not SQLite.
        if (db.copyFrom) {
          // Stream a CSV of the QVD data, using the plugin's field names as the
          // header, instead of issuing one insertRow per row.
          const csvStream = Readable.from(
            (function* () {
              yield field_names.map(csvCell).join(",") + "\n";
              for (const row of df.data) {
                const cells = new Array(field_names.length);
                for (let index = 0; index < field_names.length; index++) {
                  const fname = field_names[index];
                  cells[index] = csvCell(convertValue(fname, row[index]));
                }
                yield cells.join(",") + "\n";
              }
            })(),
          );

          const client = await db.getClient();
          try {
            await db.copyFrom(csvStream, table.name, field_names, client);
          } finally {
            await client.release(true);
          }
        } else {
          for (const row of df.data) {
            const o = {};
            for (let index = 0; index < field_names.length; index++) {
              const fname = field_names[index];
              o[fname] = convertValue(fname, row[index]);
            }
            await table.insertRow(o);
          }
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
