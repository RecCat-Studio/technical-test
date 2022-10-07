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
const NUMBER_OF_MONGO_CLIENT = 4;


class DealerGenerator extends EventEmitter {
	constructor() {
		super();
		this.nbDealerGenerated = 0;
		this.nbDealerSaved = 0;
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

			const dealersCollectionDropped = await db[1].collection(DEALERS).drop();
			this.init = true;
			console.log('collection of dealers dropped ');
	
			return dealersCollectionDropped;
		} catch (error) {
			console.log('collection not exist');
		}
	};

	createDbs = async () => {
		for (let key = 1; key <= NUMBER_OF_MONGO_CLIENT; key++) {
			const client = new MongoClient(url);
			await client.connect();
			console.log(`Client ${key} connected successfully to server`);

			const db = client.db(dbName);
			this.readyDBs.set(`client${key}`, db);	
		}
	};

	start = async () =>{
		const emitIsReady = (value, key) => {
			this.emit('ready', key, value);
		}; 

		this.readyDBs.forEach(emitIsReady);
	}
}

module.exports = DealerGenerator;
