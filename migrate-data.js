const mongodb = require('mongodb');
const async = require('async');
const mainData = require('./m3-customer-data.json');
const addressData = require('./m3-customer-address-data.json');

const url = 'mongodb://localhost:27017/edx-course-db';
const groupSize = parseInt(process.argv[2]) || 1000;
const processes = mainData.length / groupSize;

let tasks = [];

mongodb.MongoClient.connect(url, (error, db) => {
    if (error) process.exit(1);
    const database = db.db('edx-course-db');
    const startTime = Date.now();
    for (let index in mainData) {
        mainData[index] = Object.assign(mainData[index], addressData[index]);
        if (index % groupSize == 0) {
            const start = parseInt(index);
            const end = (start + groupSize > mainData.length) ? mainData.length - 1 : start + groupSize;
            tasks.push((done) => {
                console.log("Import records " + start + " - " + end + " of " + mainData.length)
                database.collection('bitcoin-customers').insert(mainData.slice(start, end), (error, results) => {
                    done(error, results);
                });
            });
        }
    }


    console.log(`Lauching ${tasks.length} parallel task(s)`)
    
    async.parallel(tasks, (error, results) => {
        if (error) console.error(error);
        const done = Date.now();
        console.log("Finished in " + Math.abs(done - startTime)) + "ms";
        db.close();
      });
});