const { find } = require('lodash');
const test = require('ava').default;
const createShitsu = require('./index');

require('dotenv').config();

const createShitsuForTest = () =>
  createShitsu(process.env.SHEET_KEY, require('./creds.json'));

const createShitsuForTestBook = book =>
  createShitsuForTest().then(_ => _.forBook(book));

test('can read sparse header', async t => {
  const book = await createShitsuForTestBook('SparseHeader');
  const actualHeader = await book.fetchHeader();

  // A - J with E, H, I missing
  const expectedHeader = ['A', 'B', 'C', 'D', '', 'F', 'G', '', '', 'J'];

  t.deepEqual(actualHeader, expectedHeader);
});

test('can fetch header for People', async t => {
  const book = await createShitsuForTestBook('People');
  const actualHeader = await book.fetchHeader();

  const expectedHeader = ['Name', 'Age', 'Weight'];

  t.deepEqual(actualHeader, expectedHeader);
});

test('can read a large Rates sheet row 1', async t => {
  const book = await createShitsuForTestBook('Rates');
  const { rows } = await book.fetch();

  const expectedFirstRow = {
    Date: '2018-01-31',
    BTCUSD: '10,102.08',
    BCHBTC: '0.14194',
    DASHBTC: '0.06819',
    ZECBTC: '0.03756',
    LTCBTC: '0.01605',
    XRPBTC: '0.00011',
    ETHBTC: '0.1023',
    SPANKETH: '0.00025',
    XMRBTC: '0.026569',
    CETBCH: '0',
    EURUSD: '1.241718',
    EOSBTC: '0.001164',
    GRLCUSD: '0.0253332',
    EDRUSD: '0',
  };

  t.deepEqual(rows[0], expectedFirstRow);
});

test('can read/write about people', async t => {
  const book = await createShitsuForTestBook('PeopleModify');
  const { header } = await book.fetch();

  t.deepEqual(header, ['Name', 'Age', 'Weight']);

  await book.updateRow(
    {
      Name: 'Andreas',
    },
    {
      Age: 34,
    }
  );

  let rows;
  let row;
  let update;

  rows = (await book.fetch()).rows;

  row = find(rows, row => row.Name === 'Andreas');

  t.is(row.Age, '34');

  update = await book.updateRow(
    {
      Name: 'Andreas',
    },
    {
      Age: '35',
      Weight: '321',
    }
  );

  rows = (await book.fetch()).rows;
  row = find(rows, row => row.Name === 'Andreas');
  t.is(row.Age, '35');
});

test('can insert rows', async t => {
  const book = await createShitsuForTestBook('Insert');

  const { rows: prevRows } = await book.fetch();

  await book.insertRow({
    Name: 'Andreas',
    Age: prevRows.length + 1,
  });

  const { rows: nextRows } = await book.fetch();

  t.is(nextRows.length, prevRows.length + 1);

  const insertedRow = nextRows[nextRows.length - 1];

  t.deepEqual(insertedRow, {
    Name: 'Andreas',
    Age: (prevRows.length + 1).toString(),
    Sex: '',
  });
});
