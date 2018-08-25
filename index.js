const assert = require('assert');
const { promisify } = require('util');
const GoogleSpreadsheet = require('google-spreadsheet');
const debug = require('debug')('shitsu');
const { findIndex, find, last, findLastIndex, uniq } = require('lodash');

async function createShitsu(sheetId, creds) {
  assert(sheetId, 'sheetId is required');
  assert(creds, 'creds is required');

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
      const lastHeaderCellIndex = findLastIndex(
        cells,
        cell => cell.row === 1 && cell.value.length
      );

      if (!~lastHeaderCellIndex) {
        throw new Error('There is no header');
      }

      const headers = new Array(cells[lastHeaderCellIndex].col);

      let cellIndex = lastHeaderCellIndex;

      for (
        let headerIndex = headers.length - 1;
        headerIndex >= 0;
        headerIndex--
      ) {
        const { col, row, value } = cells[cellIndex];

        assert.equal(row, 1);

        // Skip blanks (allows sparse)
        if (col - 1 < headerIndex) {
          headers[headerIndex] = '';
          continue;
        }

        headers[headerIndex] = value;
        cellIndex--;
      }

      const nonBlankHeaders = headers.filter(_ => _.length);

      if (nonBlankHeaders.length !== uniq(nonBlankHeaders).length) {
        // console.log(headers);
        // console.log(uniq(headers));
        throw new Error(`Header has duplicates`);
      }

      return headers;
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
      const firstNonHeaderCellIndex = findIndex(cells, cell => cell.row > 1);

      const rows = [];

      let rowN = 0;
      let colN = 0;
      let cellIndex = firstNonHeaderCellIndex;
      let row;
      let rowIsBlank;

      while (true) {
        const nextCell = cells[cellIndex];

        const isMatchingCell =
          nextCell && nextCell.col - 1 === colN && nextCell.row - 2 === rowN;

        // console.log({
        //   cellIndex,
        //   colN,
        //   rowN,
        //   isMatchingCell,
        //   nextCellCol: nextCell && nextCell.col,
        //   nextCellRow: nextCell && nextCell.row,
        // });

        if (colN === 0) {
          row = {};
          rowIsBlank = true;
        }

        const key = header[colN];
        assert(key);

        const value = isMatchingCell ? nextCell.value : '';

        if (value !== '') {
          rowIsBlank = false;
        }

        row[key] = value;

        if (isMatchingCell) {
          cellIndex++;
        }

        if (colN === header.length - 1) {
          rowN += 1;
          colN = 0;

          if (rowIsBlank) {
            // console.log('breaking because every cell is blank');
            break;
          }

          rows.push(row);
        } else {
          colN += 1;
        }
      }

      return { header, rows, cells };
    };

    const updateRow = async (criteria = {}, nextRow) => {
      const { rows, header } = await fetch();

      const rowMatchesCriteria = row =>
        Object.keys(criteria).every(key => row[key] === criteria[key]);

      const cellsToUpdate = [];

      let updatedRowCount = 0;

      for (let rowN = 0; rowN < rows.length; rowN++) {
        const row = rows[rowN];

        if (!rowMatchesCriteria(row)) {
          continue;
        }

        const cellsInRow = await getCellsAsync({
          'min-col': 1,
          'max-col': header.length,
          'min-row': rowN + 2,
          'max-row': rowN + 2,
          'return-empty': true,
        });

        for (const headerKey of Object.keys(nextRow)) {
          const headerIndex = header.indexOf(headerKey);

          if (!~headerIndex) {
            throw new Error(`Header ${headerKey} not found`);
          }

          const cell = cellsInRow.find(
            cell => cell.col === headerIndex + 1 && cell.row === rowN + 2
          );

          if (!cell) {
            throw new Error(`Update target cell not returned`);
          }

          cell.value = nextRow[headerKey];
          cellsToUpdate.push(cell);
        }

        updatedRowCount++;
      }

      if (cellsToUpdate.length) {
        await bulkUpdateCellsAsync(cellsToUpdate);
      }

      return {
        updatedRowCount,
        updatedCellCount: cellsToUpdate.length,
      };
    };

    const insertRow = async row => {
      const { rows, header } = await fetch();

      const cellsToUpdate = [];

      // console.log({ rows, header });

      const nextRowN = rows.length;

      const cellsInRow = await getCellsAsync({
        'min-col': 1,
        'max-col': header.length,
        'min-row': nextRowN + 2,
        'max-row': nextRowN + 2,
        'return-empty': true,
      });

      // console.log({ nextRowN, cellsInRow });

      for (const headerKey of Object.keys(row)) {
        const headerIndex = header.indexOf(headerKey);

        if (!~headerIndex) {
          throw new Error(`Header ${headerKey} not found`);
        }

        const cell = cellsInRow.find(cell => cell.col === headerIndex + 1);

        if (!cell) {
          throw new Error(`Update target cell not returned`);
        }

        cell.value = row[headerKey];
        cellsToUpdate.push(cell);
      }

      await bulkUpdateCellsAsync(cellsToUpdate);
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
      insertRows,
    };
  };

  return {
    forBook,
  };
}

module.exports = createShitsu;
