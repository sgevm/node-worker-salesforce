var sqlite3 = require('sqlite3').verbose();
var mkdirp = require('mkdirp');

mkdirp.sync('./var/db');

var db = new sqlite3.Database('./var/db/jobs.db');

db.serialize(function() {
 
  db.run("CREATE TABLE IF NOT EXISTS jobs ( \
    jobid TEXT PRIMARY KEY, \
    external_key TEXT UNIQUE, \
    status TEXT NOT NULL, \
    message TEXT, \
    mc_records INTEGER, \
    sc_records INTEGER, \
    start_dt TEXT DEFAULT CURRENT_TIMESTAMP, \
    end_dt TEXT \
  )"); 
  
  console.log("db.js - after DDL statements");
});

module.exports = db;
