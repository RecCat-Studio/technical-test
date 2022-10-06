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

dealerGenerator.on('ready', (key, db) => {
    console.log(`${key} is ready`);

    if (!dealerGenerator.init) {
        dealerGenerator.dropDealersCollection();
    }

    dealerGenerator.prepareAndInsertDealers(key, db);
});

dealerGenerator.on('working', (key) => {
    console.log(`${key} working`);
});


(async () => {
	await dealerGenerator.createDbs();
})();