// var sqlite3 = require('sqlite3').verbose();
// var mkdirp = require('mkdirp');
// mkdirp.sync('./var/db');
// var db = new sqlite3.Database('./var/db/jobs.db');

// db.serialize(function() {
 
//   db.run("CREATE TABLE IF NOT EXISTS jobs ( \
//     jobid TEXT PRIMARY KEY, \
//     external_key TEXT UNIQUE, \
//     status TEXT NOT NULL, \
//     message TEXT, \
//     mc_records INTEGER, \
//     sc_records INTEGER, \
//     start_dt TEXT DEFAULT CURRENT_TIMESTAMP, \
//     end_dt TEXT \
//   )"); 
  
//   console.log("db.js - after DDL statements");
// });

const Pool = require("pg").Pool;
require("dotenv").config();

const isProduction = process.env.NODE_ENV === "production";
const connectionString = `postgresql://${process.env.PG_USER}:${process.env.PG_PASSWORD}@${process.env.PG_HOST}:${process.env.PG_PORT}/${process.env.PG_DATABASE}`;
var pool;
if(isProduction){
  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false,
    }
  });  
}else{
  pool = new Pool({connectionString: connectionString});
}


pool.query("CREATE TABLE IF NOT EXISTS jobs\
(\
    jobid text NOT NULL,\
    status text NOT NULL,\
    message text ,\
    start_dt timestamp without time zone,\
    end_dt timestamp without time zone,\
    external_key text ,\
    mc_records integer,\
    sc_records integer,\
    CONSTRAINT jobs_pkey PRIMARY KEY (jobid)\
)",(err, res)=>console.log(err,res));

module.exports = pool;
