const { MongoClient } = require('mongodb');
const { readFileSync, writeFileSync } = require('fs');

const DEALERS = 'dealers';
const COUNTER = 'counter';

const bufferInput = readFileSync('./usecase2/input.json');
const inputList = JSON.parse(bufferInput);


// Connection URL
const url = `mongodb://user:password@localhost:27017/usecase2`;

// Database Name
const dbName = 'usecase2';

// Use connect method to connect to the server
MongoClient.connect(url, { useUnifiedTopology: true, useNewUrlParser: true }, async function (err, client) {
	try {
		if (err) {
			throw new Error('err');
		}

		console.log(`connected to database ${dbName} Success !`);
		const db = client.db(dbName);

		//purge DB
		const purgeCounter = await removeAllCounter(db);
		const purgeDealers = await removeAllDealers(db);

		//Generate data for DB
		const counter = generateCounter();
		const dealers = generateDealers();

		//Create data in the DB
		const counterCreated = await createCounter(db, counter);
		const dealersCreated = await createDealers(db, dealers);

		//Read data in the DB
		const initialCounter = await findAllByCollection(db, COUNTER);
		const initialListOfDealers = await findAllByCollection(db, DEALERS);

		//Data processing
		const outputList = await transform(db, inputList);
		console.log(outputList);

        //Write data in output file
		const outputListStringify = JSON.stringify(outputList);
		writeFileSync('./usecase2/output.json', outputListStringify);

		client.close();
		console.log(`connected to database ${dbName} Closed !`);
	} catch (err) {

		return console.error(err);		
	}
});    

const generateDealers = () => {
    const dealers = [
		{
			Name: 'Dealer 1',
			Street: 'Road 1',
			City: 'City1',
			internal_id: 1079380,
		},
		{
			Name: 'Dealer 1',
			Street: 'Road 2',
			City: 'City2',
			internal_id: 1079381,
		},
		{
			Name: 'Dealer 3',
			Street: 'Road 3',
			City: 'City3',
			internal_id: 1079382,
		},
	];

    return dealers;
}

const generateCounter = () => {
    const counter = {
        counter: 1079383
    }

    return counter
}

const findAllByCollection = (db, collection) => {
	return (
		db
		.collection(collection)
		.find({})
		.toArray()
	);
};

const createDealers = (db, data) => {
	return (
		db
		.collection(DEALERS)
		.insertMany(data)
	);
};

const transform = async (db, inputList) => {
    return Promise.all(inputList.map(async (dealer) => {
		let dealerFind = await db.collection(DEALERS).findOne({
			Name: dealer.Name,
			Street: dealer.Street,
			city: dealer.city,
		});

        if (dealerFind === null) {
			const counter = await db.collection(COUNTER).findOneAndUpdate(
				{},
				{
					$inc: { counter: 1 },
				}
			);

			dealer.internal_id = counter.value.counter;

			const newDealerAdded = await db.collection(DEALERS).insertOne(dealer, {
				returnNewDocument: true,
			});

			dealerFind = dealer;
		}

        delete dealerFind._id;
        
        return dealerFind;
    }));
}

const removeAllDealers = (db) => {
	return (
		db
		.collection(DEALERS)
		.deleteMany({})
	);
};

const createCounter = (db, data) => {
	return (
		db
		.collection(COUNTER)
		.insertOne(data)
	);
};

const removeAllCounter = (db) => {
	return (
		db
		.collection(COUNTER)
		.deleteMany({})
	);
};
