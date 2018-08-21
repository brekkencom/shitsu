const assert = require('assert');
const { promisify } = require('util');
const GoogleSpreadsheet = require('google-spreadsheet');
const debug = require('debug')('shitsu');
const { findIndex, pick, find, groupBy, map, last } = require('lodash');

async function createShitsu(sheetId, creds) {
  assert(sheetId);
  assert(creds);

  const doc = new GoogleSpreadsheet(sheetId);

  await promisify(doc.useServiceAccountAuth)(creds);

  const getInfoAsync = promisify(doc.getInfo);

  const forBook = async function(sheetTitle) {
    const { worksheets, ...otherInfo } = await getInfoAsync();

    // console.log(worksheets, otherInfo);

    const sheet = find(worksheets, { title: sheetTitle });

    if (!sheet) {
      const titles = worksheets.map(_ => _.title).join();
      throw new Error(`Sheet ${sheetTitle} not found in ${titles}`);
    }

    const getCellsAsync = promisify(sheet.getCells);
    const bulkUpdateCellsAsync = promisify(sheet.bulkUpdateCells);

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

        assert(
          value.length > 0,
          `Cannot have blank cells. Cell #${cell.col} is blank`
        );

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
        // 'return-empty': true,
        // 'oncl'
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

      // console.log(header, header.length);
      // console.log(cellsAfterHeader[0]);

      const reducer = (prev, cell, index, cells) => {
        if (prev.done) {
          return prev;
        }

        const isLastInRow = (cells[index + 1] || {}).row !== cell.row;

        const { value } = cell;

        // console.log(cell.col, value);

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

    const updateRow = async (row, { replace = false } = {}) => {
      const cells = await fetchEveryCell();
      const header = getHeaderFromCells(cells);

      const keyName = header[0];
      assert(keyName, 'Key name missing from header;');

      const key = row[keyName];
      assert(key, `Key (${keyName}) is missing`);

      // Fetch every key
      const cellsInFirstColumn = await getCellsAsync({
        'min-row': 1,
        'max-row': sheet.rowCount,
        'min-col': 1,
        'max-col': 1,
      });

      // Find the row number of the key
      const keyCells = cellsInFirstColumn.filter(cell => {
        assert.equal(cell.col, 1);
        return cell.value === key;
      });

      assert(keyCells.length < 2, 'Duplicate keys is not supported');

      const [keyCell] = keyCells;

      assert(
        keyCell,
        `A row with ${keyName} equal to ${key} could not be  found`
      );

      // Extract the row
      const rowCells = await getCellsAsync({
        'min-row': keyCell.row,
        'max-row': keyCell.row,
        'min-col': 1,
        'max-col': sheet.colCount,
        'return-empty': true,
      });

      const cellsToUpdate = [];

      for (let headerIndex = 0; headerIndex < header.length; headerIndex++) {
        const headerKey = header[headerIndex];

        // Skip undefined keys unless replacing
        if (row[headerKey] === undefined && !replace) {
          continue;
        }

        // Do not update the row key cell
        if (headerIndex === 0) {
          continue;
        }

        const nextValueIsBlank =
          row[headerKey] === undefined || row[headerKey] === '';

        const nextValue = nextValueIsBlank ? '' : row[headerKey];

        const cell = rowCells.find(_ => _.col === headerIndex + 1);
        assert(cell, `Cell not found for ${headerKey}`);

        cell.value = nextValue;
        cellsToUpdate.push(cell);
      }

      await bulkUpdateCellsAsync(cellsToUpdate);
    };

    const insertRow = async row => {
      const cells = await fetchEveryCell();
      const header = getHeaderFromCells(cells);

      const keyName = header[0];
      assert(keyName, 'Key name missing from header;');

      const key = row[keyName];
      assert(key, `Key (${keyName}) is missing`);

      // Fetch every key
      const cellsInFirstColumn = await getCellsAsync({
        'min-row': 1,
        'max-row': sheet.rowCount,
        'min-col': 1,
        'max-col': 1,
      });

      const duplicateKeys = cellsInFirstColumn.filter(
        cell => cell.value === key
      );

      if (duplicateKeys.length) {
        throw new Error(
          `Key ${key} would collide with ${duplicateKeys.length} other keys`
        );
      }

      // TODO: Gap key cells
      const nextRowNumber = last(cellsInFirstColumn).row + 1;

      // Extract the row
      const rowCells = await getCellsAsync({
        'min-row': nextRowNumber,
        'max-row': nextRowNumber,
        'min-col': 1,
        'max-col': sheet.colCount,
        'return-empty': true,
      });

      const cellsToUpdate = [];

      // TODO: If a cell has no key, but has other values defined, it can get confusing

      for (let headerIndex = 0; headerIndex < header.length; headerIndex++) {
        const headerKey = header[headerIndex];

        const nextValueIsBlank =
          row[headerKey] === undefined || row[headerKey] === '';

        const nextValue = nextValueIsBlank ? '' : row[headerKey];

        const cell = rowCells.find(_ => _.col === headerIndex + 1);
        assert(cell, `Cell not found for ${headerKey}`);

        cell.value = nextValue;
        cellsToUpdate.push(cell);
      }

      await bulkUpdateCellsAsync(cellsToUpdate);
    };

    const clearIncludingKeyWithRowNumber = async function(rowNumber) {
      assert(rowNumber > 1);

      // Extract the row
      const rowCells = await getCellsAsync({
        'min-row': rowNumber,
        'max-row': rowNumber,
        'min-col': 1,
        'max-col': sheet.colCount,
      });

      if (!rowCells.length) {
        return;
      }

      rowCells.forEach(cell => (cell.value = ''));

      await bulkUpdateCellsAsync(rowCells);
    };

    const fetchRowNumbersFromKeys = async function(keys) {
      // Fetch every key
      const cellsInFirstColumn = await getCellsAsync({
        'min-row': 1,
        'max-row': sheet.rowCount,
        'min-col': 1,
        'max-col': 1,
      });

      const matchingCells = cellsInFirstColumn.filter(
        cell => console.log(cell.value, keys) || keys.includes(cell.value)
      );

      const matchingRowNumbers = matchingCells.map(_ => _.row);

      return matchingRowNumbers;
    };

    const clearIncludingKeyWithKey = async function(key) {
      const rowNumbers = await fetchRowNumbersFromKeys([key]);

      console.log({ rowNumbers });

      for (const rowNumber of rowNumbers) {
        await clearIncludingKeyWithRowNumber(rowNumber);
      }
    };

    const insertRows = async function(rows) {
      for (const row of rows) {
        await insertRow(row);
      }
    };

    return {
      fetch,
      fetchHeader,
      updateRow,
      insertRow,
      clearIncludingKeyWithKey,
      insertRows,
    };
  };

  return {
    forBook,
  };
}

module.exports = createShitsu;
