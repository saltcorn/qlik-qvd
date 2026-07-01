const fs = require("fs");
const File = require("@saltcorn/data/models/file");
const Table = require("@saltcorn/data/models/table");
const Field = require("@saltcorn/data/models/field");
const { getState } = require("@saltcorn/data/db/state");
const db = require("@saltcorn/data/db");

const { Readable } = require("stream");

// qvd4js is used only to read the QVD header and symbol table (small) so we can
// deduce field types. qvdrs (native Rust bindings) reads the data section into
// native memory and lets us stream it out a cell at a time, which keeps large
// files off the V8 heap and avoids the out-of-memory errors that qvd4js hit by
// materialising the entire data frame in JavaScript.
const { QvdFileReader } = require("qvd4js");
const { readQvd } = require("qvdrs");

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

// Resolve a single QVD symbol to the value qvd4js would actually place in the
// data frame: QvdSymbol.toPrimaryValue() prioritises the string representation,
// and QvdFileReader.load() then coerces numeric-looking strings to numbers.
const symbolValue = (symbol) => {
  const value = symbol.toPrimaryValue();
  if (typeof value === "string" && value !== "" && !isNaN(Number(value)))
    return Number(value);
  return value;
};

// Inspect every distinct symbol of a field to discover what it really holds.
// The QVD NumberFormat cannot be trusted on its own: a field tagged INTEGER may
// still contain pure strings (e.g. "KBS108244839"), so the symbol table is the
// authoritative source for the column type.
const scanSymbols = (symbols) => {
  let hasString = false;
  let hasNumber = false;
  let hasFloat = false;
  for (const symbol of symbols || []) {
    const value = symbolValue(symbol);
    if (value === null || value === undefined) continue;
    if (typeof value === "number") {
      hasNumber = true;
      if (!Number.isInteger(value)) hasFloat = true;
    } else {
      hasString = true;
    }
  }
  return { hasString, hasNumber, hasFloat };
};

const deduceFieldType = (field, symbols) => {
  const nf = field.NumberFormat;
  // Dates/times are identified through the number format only; in the symbol
  // table they appear as numeric serials or formatted strings.
  if (nf.Type === "TIME") return { type: "String" };
  if (nf.Type === "DATE")
    return {
      type: "Date",
      attributes: nf.Fmt === "DD.MM.YYYY" ? { day_only: true } : {},
    };
  if (nf.Type === "TIMESTAMP")
    return {
      type: "Date",
    };

  // For everything else, let the actual symbol contents decide between String,
  // Float and Integer rather than relying on the (unreliable) NumberFormat.
  const { hasString, hasNumber, hasFloat } = scanSymbols(symbols);
  if (hasString) return { type: "String" };
  if (hasFloat || nf.Type === "REAL" || nf.Type === "FIX")
    return { type: "Float" };
  if (hasNumber || nf.Type === "INTEGER") return { type: "Integer" };
  // No usable symbols (e.g. an all-null column): fall back to String.
  return { type: "String" };
};

const QLIK_EPOCH_MS = Date.UTC(1899, 11, 30); // 1899-12-30 00:00:00 (month is 0-indexed)
const MS_PER_DAY = 86400000;

function fromQlik(serial, { roundToSeconds = true } = {}) {
  let ms = QLIK_EPOCH_MS + serial * MS_PER_DAY;
  if (roundToSeconds) ms = Math.round(ms / 1000) * 1000;
  return new Date(ms);
}

// Read just the XML header and the symbol table of a QVD file using qvd4js,
// without ever loading or parsing the (potentially huge) index/data section.
// qvd4js' own load() reads the whole file into a buffer and then builds the
// entire data frame in memory; here we read only the bytes up to the start of
// the index table, which is enough to recover the field headers and symbols.
async function loadQvdMetadata(path) {
  const reader = new QvdFileReader(path);
  const fd = await fs.promises.open(path, "r");
  try {
    // The XML header lives at the start of the file, terminated by "\r\n\0".
    // Read forward in chunks until the delimiter is found.
    const HEADER_DELIMITER = "\r\n\0";
    const CHUNK = 1 << 16;
    let head = Buffer.alloc(0);
    let delimIdx = -1;
    while (delimIdx === -1) {
      const buf = Buffer.alloc(CHUNK);
      const { bytesRead } = await fd.read(buf, 0, CHUNK, head.length);
      if (bytesRead === 0) break;
      head = Buffer.concat([head, buf.subarray(0, bytesRead)]);
      delimIdx = head.indexOf(HEADER_DELIMITER);
    }
    if (delimIdx === -1)
      throw new Error("The QVD XML header could not be located.");

    // Parse the header from what we have read so far; this computes the offset
    // at which the index (data) table begins.
    reader._buffer = head;
    await reader._parseHeader();

    // The symbol table sits between the header and the index table. Read exactly
    // that region (bytes [0, indexTableOffset)) and parse the symbols, then drop
    // the buffer. The data section is never touched.
    const need = reader._indexTableOffset;
    const buffer = Buffer.alloc(need);
    await fd.read(buffer, 0, need, 0);
    reader._buffer = buffer;
    await reader._parseSymbolTable();
    reader._buffer = null;
  } finally {
    await fd.close();
  }
  return reader;
}

const unPctFieldName = (s) => s.replaceAll("%", "Pct");

module.exports = {
  sc_plugin_api_version: 1,
  plugin_name: "qlik-qvd",
  functions: {
    import_qvd_file: {
      async run(file_name, table_name) {
        const file = await File.findOne({ filename: file_name });

        // Field types come from the QVD header + symbol table only (cheap).
        const reader = await loadQvdMetadata(file.location);
        const qvdTableName = reader._header.QvdTableHeader.TableName;
        let fields =
          reader._header["QvdTableHeader"]["Fields"]["QvdFieldHeader"];
        // A QVD with a single field is parsed as an object, not an array.
        // Normalise so it lines up with reader._symbolTable, which is always an
        // array indexed by field position.
        if (!Array.isArray(fields)) fields = [fields];

        let table = Table.findOne({ name: table_name || qvdTableName });
        let field_names = [];
        if (!table) {
          table = await Table.create(table_name || qvdTableName);

          for (let i = 0; i < fields.length; i++) {
            const field = fields[i];
            const fld = {
              table,
              label: unPctFieldName(field.FieldName),
              ...deduceFieldType(field, reader._symbolTable[i]),
            };

            const f = await Field.create(fld);
            field_names.push(f.name);
          }
          await getState().refresh_tables();
        } else {
          field_names = fields.map((f) => Field.labelToName(unPctFieldName(f.FieldName)));
        }

        const typeByName = new Map(
          table.fields.map((f) => [f.name, f.type?.name]),
        );
        const timeStampFields = new Set(
          table.fields
            .filter((f) => f.type?.name === "Date" && !f.attributes?.day_only)
            .map((f) => f.name),
        );

        // qvdrs returns every value as its display string (numbers as numeric
        // strings, missing values as null). Convert that string to the value we
        // want stored: numeric serials in timestamp columns become Dates, and
        // numeric columns are coerced back to numbers; everything else is left
        // as the string (already-formatted dates, plain text, ...).
        const convertValue = (fname, val) => {
          if (val === null || val === undefined || val === "") return null;
          if (timeStampFields.has(fname)) {
            const n = Number(val);
            return isNaN(n) ? val : fromQlik(n);
          }
          const t = typeByName.get(fname);
          if (t === "Integer" || t === "Float") {
            const n = Number(val);
            return isNaN(n) ? val : n;
          }
          return val;
        };

        // Read the data section into native (Rust) memory. The whole table is
        // never materialised on the JavaScript heap; we pull it out one cell at
        // a time below.
        const qvdTable = await readQvd(file.location);
        const numRows = qvdTable.numRows;
        const qvdColumns = qvdTable.columns;
        // Map each target field to its column position in the QVD data.
        const colIndex = fields.map((f) => qvdColumns.indexOf(f.FieldName));

        // Fast path: bulk-load via Postgres COPY when the driver supports it.
        // db.copyFrom is only defined on the Postgres driver, not SQLite.
        if (db.copyFrom) {
          // Stream a CSV of the QVD data, using the plugin's field names as the
          // header, instead of issuing one insertRow per row. The generator is
          // pull-based, so only one row exists in memory at any time.
          const csvStream = Readable.from(
            (function* () {
              yield field_names.map(csvCell).join(",") + "\n";
              for (let r = 0; r < numRows; r++) {
                const cells = new Array(field_names.length);
                for (let i = 0; i < field_names.length; i++) {
                  const raw = qvdTable.get(r, colIndex[i]);
                  cells[i] = csvCell(convertValue(field_names[i], raw));
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
          for (let r = 0; r < numRows; r++) {
            const o = {};
            for (let i = 0; i < field_names.length; i++) {
              const raw = qvdTable.get(r, colIndex[i]);
              o[field_names[i]] = convertValue(field_names[i], raw);
            }
            await table.insertRow(o);
          }
        }
      },
      description: "Import a Qlik QVD file into a Saltcorn table",
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
