const DealerGenerator = require('./DealerGenerator');

const dealerGenerator = new DealerGenerator();

dealerGenerator.once('start', () => {
    console.log('start of the generation of dealers');
});

dealerGenerator.on('genereted', (nbDealersGenereted) => {
	//console.log(`${nbDealersGenereted} dealers generated`);
});

dealerGenerator.on('saved', (nbDealersSaved) => {
	console.log(`${nbDealersSaved} dealers saved`);
});

dealerGenerator.on('end', (nb) => {
    console.log('dealers generation completed successfully!');
    console.log(`${nb} dealer generated`);
});

async function main() {
    
    const dbs = await dealerGenerator.createDbs();

    const purgeDealers = await dealerGenerator.removeAllDealers(dbs[0]);
    

    dealerGenerator.prepareAndPushToInsertIntoDB(dbs);


	return 'done.';
}


main()
  .then(console.log)
  .catch(console.error)
  .finally(() => {

});