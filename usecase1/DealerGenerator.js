const { MongoClient } = require('mongodb');
const EventEmitter = require('events').EventEmitter;

// Connection URL
const url = `mongodb://user:password@localhost:27017/usecase1`;

// Database Name
const dbName = 'usecase1';

// Collection Name
const DEALERS = 'dealers';

const NB_INSERT = 1000000000;
const MAX_RECORDS_PER_INSERT = 2000;


class DealerGenerator extends EventEmitter {
	constructor() {
		super();
		this.nbDealerGenerated = 0;
		this.nbDealerRequested = 0;
		this.nbDealerSaved = 0;
	}

	generateDealers(nbDealersToGenerate) {
		this.emit('start');
		const dealers = [];

		for (let i = 0; i < nbDealersToGenerate; i++) {
			const dealer = {
				Name: `Dealer ${this.nbDealerGenerated + 1}`,
				Street: `Road ${this.nbDealerGenerated + 1}`,
				City: `City ${this.nbDealerGenerated + 1}`,
			};

			dealers.push(dealer);

			this.emit('genereted', ++this.nbDealerGenerated);
		}

		return dealers;
	}

	prepareAndPushToInsertIntoDB = async (dbs) => {
        while (this.nbDealerGenerated < NB_INSERT) {
            await Promise.all(dbs.map(async (db) => {
                if (this.nbDealerGenerated < NB_INSERT) {

                    const dealers = this.generateDealers(MAX_RECORDS_PER_INSERT);
    
                    await this.insertManyDealers(db, dealers);
                }
			}));	
		}

        this.emit('end', this.nbDealerGenerated);
	};

	insertManyDealers = async (db, data) => {
		const dealersInserted = await db.collection(DEALERS).insertMany(data);
		this.nbDealerSaved += dealersInserted.insertedCount;

		this.emit('saved', this.nbDealerSaved);

		return dealersInserted;
	};

	removeAllDealers = async (db) => {
		const dealersPurged = await db.collection(DEALERS).deleteMany({});
		console.log('collection of dealers purged ');

		return dealersPurged;
	};

	createDbs = async () => {
		const dbs = [];

		for (let i = 1; i <= 4; i++) {
			const client = new MongoClient(url);
			await client.connect();
			console.log(`Client ${i} connected successfully to server`);

			const db = client.db(dbName);
			dbs.push(db);
		}

		return dbs;
	};
}

module.exports = DealerGenerator;
