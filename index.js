const assert = require('assert');
const { promisify } = require('util');
const GoogleSpreadsheet = require('google-spreadsheet');
const debug = require('debug')('shitsu');
const { findIndex, pick, find, groupBy, map } = require('lodash');

async function createShitsu(sheetId, creds) {
  assert(sheetId);
  assert(creds);

  const doc = new GoogleSpreadsheet(sheetId);

  await promisify(doc.useServiceAccountAuth)(creds);

  const getInfoAsync = promisify(doc.getInfo);

  const forBook = async function(sheetTitle) {
    const { worksheets } = await getInfoAsync();

    const sheet = find(worksheets, { title: sheetTitle });

    if (!sheet) {
      throw new Error(
        `Sheet ${sheetTitle} not found in ${pick(worksheets, 'title').join()}`
      );
    }

    const getCellsAsync = promisify(sheet.getCells);

    const getHeaderFromCells = function(cells) {
      assert(cells.length > 0);

      const initial = {
        cell: null,
        values: [],
      };

      const reducer = (prev, cell) => {
        if (cell.row > 1) {
          return prev;
        }

        if (prev.cell) {
          assert(cell.col === prev.cell.col + 1);
        } else {
          assert(prev.cell === null && cell.col === 1);
        }

        const { value } = cell;
        assert(value.length > 0);

        return {
          cell,
          values: [...prev.values, value],
        };
      };

      const { values } = cells.reduce(reducer, initial);

      return values;
    };

    const fetchHeader = async function() {
      assert(sheet.rowCount > 0);
      assert(sheet.colCount > 0);

      const cells = await getCellsAsync({
        'min-row': 1,
        'max-row': 1,
        'min-col': 1,
        'max-col': sheet.colCount,
      });

      return getHeaderFromCells(cells);
    };

    const fetchEveryCell = () =>
      getCellsAsync({
        'min-row': 1,
        'max-row': sheet.rowCount,
        'min-col': 1,
        'max-col': sheet.colCount,
      });

    const fetch = async function() {
      if (sheet.rowCount === 1) {
        return [];
      }

      const cells = await fetchEveryCell();
      const header = getHeaderFromCells(cells);
      const cellsAfterHeader = cells.slice(header.length);

      // console.log(
      //   JSON.stringify(
      //     cells.map(_ => ({
      //       row: _.row,
      //       col: _.col,
      //       value: _.value,
      //     })),
      //     null,
      //     2
      //   )
      // );

      // return;

      const initial = {
        cell: null,
        rows: [],
        row: [],
      };

      console.log(header, header.length);
      console.log(cellsAfterHeader[0]);

      const reducer = (prev, cell, index, cells) => {
        if (prev.done) {
          return prev;
        }

        const isLastInRow = (cells[index + 1] || {}).row !== cell.row;

        const { value } = cell;

        console.log(cell.col, value);

        if (cell.col === 1 && !value.length) {
          return { ...prev, done: true };
        }

        if (!prev.cell) {
          assert(prev.cell === null && cell.col === 1 && cell.row === 2);
        }

        const nextRow = [...prev.row, value];

        return {
          cell,
          rows: isLastInRow ? [...prev.rows, nextRow] : prev.rows,
          row: isLastInRow ? [] : nextRow,
        };
      };

      const { rows: rowsAsArray } = cellsAfterHeader.reduce(reducer, initial);

      const rows = rowsAsArray.reduce(
        (rows, rowValues) => [
          ...rows,
          rowValues.reduce(
            (row, value, index) => ({
              ...row,
              [header[index]]: value,
            }),
            {}
          ),
        ],
        []
      );

      return { header, rows };
    };

    return { fetch, fetchHeader };
  };

  return {
    forBook,
  };
}

async function main() {
  const book = await createShitsu(
    process.env.SHEET_KEY,
    require('./creds.json')
  );

  const sheet = await book.forBook('Positions');

  const positions = await sheet.fetch();

  console.log(positions);
}

main().then(process.exit);
