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
		this.nbDealerSaved = 0;
		this.init = false;
		this.readyDBs = new Map();
		this.workingDBs = new Map();
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

	prepareAndInsertDealers = async  (key, db) => {	
		if (this.nbDealerGenerated >= NB_INSERT) {
			this.emit('end', this.nbDealerGenerated);
			return 
		}
		
		this.workingDBs.set(key, db);
		this.readyDBs.delete(key);
		this.emit('working', key);

		const dealers = this.generateDealers(MAX_RECORDS_PER_INSERT);

		await this.insertManyDealers(db, dealers);
	
		this.readyDBs.set(key, db);
		this.emit('ready', key, db);
		this.workingDBs.delete(key);				
	} 
	
	insertManyDealers = async (db, data) => {
		const dealersInserted = await db.collection(DEALERS).insertMany(data);
		this.nbDealerSaved += dealersInserted.insertedCount;

		this.emit('saved', this.nbDealerSaved);

		return dealersInserted;
	};

	dropDealersCollection = async () => {
		try {
			const iterator = this.readyDBs.entries();
			const db = iterator.next().value;

			this.workingDBs.set(db[0], db[1]);
			this.readyDBs.delete(db[0]);
			this.emit('working', db[0]);
			
			const dealersCollectionDropped = await db[1].collection(DEALERS).drop();
			this.init = true;
			console.log('collection of dealers dropped ');
	
			this.readyDBs.set(db[0], db[1]);
			this.emit('ready', db[0], db[1]);
			this.workingDBs.delete(db[0]);
			
			return dealersCollectionDropped;
		} catch (error) {
			console.log(error);
		}
	};

	createDbs = async () => {
		for (let key = 1; key <= 4; key++) {
			const client = new MongoClient(url);
			await client.connect();
			console.log(`Client ${key} connected successfully to server`);

			const db = client.db(dbName);
			this.readyDBs.set(`client${key}`, db);
			this.emit('ready', `client${key}`, db);
		}

		return this.readyDBs;
	};
}

module.exports = DealerGenerator;
