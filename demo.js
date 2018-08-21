const createShitsu = require('./index');

async function main() {
  const book = await createShitsu(
    process.env.SHEET_KEY,
    require('./creds.json')
  );

  const sheet = await book.forBook('Rates');

  const positions = await sheet.fetch();

  const derp = positions.rows.filter(_ => _.Date === '2018-02-16');

  console.log({ derp });

  // console.log(positions);

  // await sheet.updateRow(
  //   {
  //     Name: 'Ben',
  //     // Age: 9,
  //     // Weight: 11,
  //   },
  //   { replace: true }
  // );

  // await sheet.insertRow(
  //   {
  //     Name: 'Sven',
  //     Age: 1,
  //     Weight: 2,
  //   },
  //   { replace: true, insert: true }
  // );
}

main().then(process.exit);
