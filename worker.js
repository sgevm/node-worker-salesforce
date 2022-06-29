require("dotenv").config();
var throng = require('throng');
var Queue = require("bull");
var jsforce = require("jsforce");
const request = require("request");
var parseString = require('xml2js').parseString;
//const db = require("./db");
const pool = require("./db");


// Connect to a local redis instance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
let workers = process.env.WEB_CONCURRENCY || 1;

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network 
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
let maxJobsPerWorker = 1;

function start(id, disconnect) {
  console.log(`start() : Started worker ${ id }`);

  process.on('SIGTERM', ()=>{
    console.log('shutdown');
    pool.query("UPDATE jobs SET status=$1, message=$2 WHERE status=$3", ['Aborted', 'Aborted on shutdown', 'In Progress'],function(err,rows){
      if (err) { 
        console.log('shutdown . update jobs.status & message:'); 
        console.log(err); 
        /*throw an error*/ 
      }
    });
    //disconnect(); 
  });


  // Connect to the named work queue
  let workQueue = new Queue('work', REDIS_URL);

  workQueue.process(maxJobsPerWorker, async (job) => {    
    // This is an example job that just slowly reports on progress
    // while doing no work. Replace this with your own job logic.
    var progress = 0;

    console.log(`....inside workQueue.process ${job.id}`);

    try{
      var row = await queryJobById(job.id);
      if (row[0]) {
        console.log(`....inside workQueue.process ${job.id} - before update`);
        var rows = await updateJobStatus(job.id, 'In Progress');
        console.log(`....inside workQueue.process ${job.id} - after update`);
      }else{
        console.log(`....inside workQueue.process ${job.id} - before insert`);
        await insertJob(job.id, 'In Progress', 'New', 0, 0);
        console.log(`....inside workQueue.process ${job.id} - after insert`);
        console.log(`....inside workQueue.process - before generateToken`);
        generateToken(job);
        console.log(`....inside workQueue.process - after generateToken`);            
      }      
    }catch(e){
      console.log(e);
    }
    
    // A job can return values that will be stored in Redis as JSON
    // This return value is unused in this demo application.
    return { value: 'Started processing job.Id: ' + job.id };
  });//process

  
  workQueue.on('active', function (job, jobPromise) {
    // A job has started. You can use `jobPromise.cancel()`` to abort it.
    console.log('onActive:'+job.id);
  });
  
  workQueue.on('stalled', function (job) {
    // A job has been marked as stalled. This is useful for debugging job
    // workers that crash or pause the event loop.
    console.log('onStalled:'+job.id);
  });
   
  workQueue.on('progress', function (job, progress) {
    // A job's progress was updated!
    //job.log('job progress is updated');
    console.log('onProgress:'+job.id);
  });
  
  workQueue.on('completed', function (job, result) {
    // A job successfully completed with a `result`.
    console.log('onCompleted:'+job.id);
  });
  
  workQueue.on('failed', function (job, err) {
    // A job failed with reason `err`!
    console.log('onFailed:'+job.id);
    console.log(err);
  });
  
  workQueue.on('resumed', function (job) {
    // The queue has been resumed.
    console.log('onResumed:'+job.id);
  }); 
 
  workQueue.on('removed', function (job) {
    // A job successfully removed.
    console.log('onRemoved:'+job.id);
  });
  
  

}//start




function generateToken(job) {  
  console.log(`....inside generateToken`);
  var options = { method: 'POST',
    url: `${process.env.MC_TOKEN_URL}`,
    headers: 
     { 'Cache-Control': 'no-cache',
       'Content-Type': 'application/json' },
    body: 
     { grant_type: process.env.MC_GRANT_TYPE,
       client_id: process.env.MC_CLIENT_ID,
       client_secret: process.env.MC_CLIENT_SECRET,
       scope: process.env.MC_SCOPE,
       account_id: process.env.MC_ACCOUNT_ID
      },
    json: true };
  
    request(options, (error, response, body) => {
      console.log(`....inside generateToken.request`);
      if (error) {
        console.log(`....inside generateToken.request.error`);
        console.log(error);
        process.env.MC_CONNECTION_STATUS=error;
        throw new Error(error);
      }
      job.progress=25;
      //console.log('MC Generate Token:'+JSON.stringify(body));
      process.env.access_token = body.access_token;
      process.env.soap_instance_url = body.soap_instance_url;
      process.env.rest_instance_url = body.rest_instance_url;

      //salesforce token
      var conn = new jsforce.Connection({
        oauth2 : {
          loginUrl : process.env.SF_LOGIN_URL,
          clientId : process.env.SF_CLIENT_ID,
          clientSecret : process.env.SF_CLIENT_SECRET,
          redirectUri : process.env.SF_REDIRECT_URL
        }
      });
      conn.login(process.env.SF_USERNAME, process.env.SF_PASSWORD, async (err, userInfo) => {
        if (err) { return console.error(err); }
        // Now you can get the access token and instance URL information.
        // Save them to establish connection next time.
        process.env.SF_ACCESS_TOKEN=conn.accessToken;
        process.env.SF_INSTANCE_URL=conn.instanceUrl;
        // logged in user property
        //console.log("User ID: " + userInfo.id);
        //console.log("Org ID: " + userInfo.organizationId);
        var retval = await fetchDataExtensionRecords(job.id, conn, '', '');
      });      
    });
}//generateToken

async function fetchDataExtensionRecords(jobid, conn, pOverallStatus, pRequestId) {
  console.log('1.1....inside fetchDataExtensionRecords ');
  var batch_size = process.env.MC_BATCH_SIZE || 1000;
  var request = require("request");
  var reqBody='';
  if (pOverallStatus=='MoreDataAvailable') {
    reqBody=`<?xml version="1.0" encoding="utf-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soapenv:Header><fueloauth>${process.env.access_token}</fueloauth></soapenv:Header><soapenv:Body><RetrieveRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI"><RetrieveRequest><ClientIDs><ClientID>${process.env.MC_ACCOUNT_ID}</ClientID></ClientIDs><ContinueRequest>${pRequestId}</ContinueRequest><ObjectType>DataExtensionObject[SF_ZIPCODE_SERVICEAREA]</ObjectType><Properties>Id</Properties><Properties>Name</Properties><Properties>Service_Area__c</Properties><Properties>Zip_Country__c</Properties><Options><BatchSize>${batch_size}</BatchSize></Options></RetrieveRequest></RetrieveRequestMsg></soapenv:Body></soapenv:Envelope>`;
  }else{
    reqBody=`<?xml version="1.0" encoding="utf-8"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><soapenv:Header><fueloauth>${process.env.access_token}</fueloauth></soapenv:Header><soapenv:Body><RetrieveRequestMsg xmlns="http://exacttarget.com/wsdl/partnerAPI"><RetrieveRequest><ClientIDs><ClientID>${process.env.MC_ACCOUNT_ID}</ClientID></ClientIDs><ObjectType>DataExtensionObject[SF_ZIPCODE_SERVICEAREA]</ObjectType><Properties>Id</Properties><Properties>Name</Properties><Properties>Service_Area__c</Properties><Properties>Zip_Country__c</Properties><Options><BatchSize>${batch_size}</BatchSize></Options></RetrieveRequest></RetrieveRequestMsg></soapenv:Body></soapenv:Envelope>`;
  }


  var options = { method: 'POST',
    url: process.env.soap_instance_url + 'Service.asmx',
    headers: 
     { 'Cache-Control': 'no-cache',
       SOAPAction: 'Retrieve',
       'Content-Type': 'text/xml' 
      },
    body: reqBody
  };
  request(options, async (error, response, body) => {
    console.log('1.1....inside fetchDataExtensionRecords .request');
    if (error) throw new Error(error);
    parseString(body, {ignoreAttrs: true, explicitArray: false, stripPrefix: true}, async (err, result) => {
      const retrieveResponse = result["soap:Envelope"]["soap:Body"].RetrieveResponseMsg;
      const results = retrieveResponse.Results;
      const overallStatus = retrieveResponse.OverallStatus; //MoreDataAvailable
      const requestId = retrieveResponse.RequestID;

      console.log('1.2....inside fetchDataExtensionRecords UPDATE jobs.extenal_key');
      pool.query("UPDATE jobs SET external_key=$1 WHERE jobid=$2", [retrieveResponse.RequestID, jobid],function(err,rows){
        if (err) { console.log(err); /*throw an error*/ }
      });

      var jsonResults=[];
      var fieldNames=['Id','Name','Service_Area__c', 'Zip_Country__c'];
      jsonResults = results.map(result=>{
        var obj={};
        result.Properties.Property.forEach(prop => {
          if(fieldNames.includes(prop.Name)){
            var key = (prop.Name==='Id'?'ExternalKey__c':prop.Name);
            obj[key] = prop.Value; 
          }
        });
        return {...obj};
      });

      console.log('1.3....calling updateSFMCRecordCount ');
      await updateSFMCRecordCount(jobid, parseInt(jsonResults.length));
      console.log('1.4....calling salesforceBulkUpsert ');
      await salesforceBulkUpsert(jobid, conn, jsonResults, overallStatus, requestId);
    });
  });
  return 'done';
}//fetchDataExtensionRecords


async function salesforceBulkUpsert(jobid, conn, records, pOverallStatus, pRequestId){
  console.log('------3.1....inside salesforceBulkUpsert ');
  conn.bulk.pollTimeout = 60000; // Bulk timeout can be specified globally on the connection object
  conn.bulk.load("ZipCode__c", "upsert", {extIdField:'ExternalKey__c'}, records, async (err, rets) => {
    if (err) { return console.error(err); }
    console.log('------3.2....inside salesforceBulkUpsert  bulk.load.upsert');
    var successCounter=0;
    var failureCounter=0;
    for (var i=0; i < rets.length; i++) {
      if (rets[i].success) {
        successCounter++;
      } else {
        failureCounter++;
      }
    }//for

    console.log('------3.3....calling updateSFSCRecordCount');
    await updateSFSCRecordCount(jobid, successCounter);

    if (pOverallStatus=='MoreDataAvailable') {
      console.log('------3.4....calling fetchDataExtensionRecords');
      await fetchDataExtensionRecords(jobid, conn, pOverallStatus, pRequestId);
    }else{
      console.log('------3.5....UPDATE jobs.status=Completed');
      await updateJobStatus(jobid, 'Completed');
    }
  });//load
  
}//salesforceBulkUpsert

async function updateSFMCRecordCount(jobid, recordcount){
  console.log('----2.1....inside updateSFMCRecordCount ');
  var rows = await queryJobById(jobid);

  var newCount = (rows[0].mc_records==undefined?0:rows[0].mc_records) + recordcount;
  return await updateSCcount(jobid, newCount);
}//updateSFMCRecordCount

async function updateSFSCRecordCount(jobid, recordcount){
  console.log('--------4.1....inside updateSFSCRecordCount ');
  var rows = await queryJobById(jobid);
  var newCount = (rows[0].sc_records==undefined?0:rows[0].sc_records) + recordcount;
  return await updateSCcount(jobid, newCount);
}//updateSFSCRecordCount

async function queryJobById(jobid){
  console.log('....inside queryJobById ');
  var promiseQuery = () => {
    return new Promise((resolve, reject) => {
        pool.query('SELECT * FROM jobs WHERE jobid =$1', [ jobid ], (err, results) => {
          if (err) { 
            reject(err) 
          }else{
            resolve(results.rows);
          }});
    });
  }
  return await promiseQuery();
}//queryJobById

async function updateJobStatus(jobid, status){
  console.log('....inside updateJobStatus ');
  var promiseUpdate = () => {
    return new Promise((resolve, reject) => {
        pool.query('UPDATE jobs SET status=$1 WHERE jobid =$2', [ status, jobid ], (err, results) => {
          if (err) { 
            console.log('....inside updateJobStatus . reject');
            reject(err) 
          }else{
            console.log('....inside updateJobStatus . resolve');
            resolve(results.rows);
          }});
    });
  }
  return await promiseUpdate();
}//updateJobStatus

async function insertJob(jobid, status, message){
  console.log('....inside insertJob '+ jobid);
  var promiseInsert = () => {
    return new Promise((resolve, reject) => {
        pool.query('INSERT INTO jobs(jobid, status, message, mc_records, sc_records, start_dt) VALUES($1,$2,$3,$4,$5,now()) RETURNING *', [ jobid, status, message, 0, 0 ], (err, results) => {
          if (err) { 
            console.log('....inside insertJob . reject');
            reject(err) 
          }else{
            console.log('....inside insertJob . resolve');
            resolve(results.rows);
          }});
    });
  }
  return await promiseInsert();
}//insertJob

async function updateMCcount(jobid, recordcount){
  console.log('....inside updateMCcount ');
  var promiseUpdate = () => {
    return new Promise((resolve, reject) => {
        pool.query('UPDATE jobs SET mc_records=$1 WHERE jobid =$2', [ recordcount, jobid ], (err, results) => {
          if (err) { 
            console.log('....inside updateMCcount . reject');
            reject(err) 
          }else{
            console.log('....inside updateMCcount . resolve');
            resolve(results.rows);
          }});
    });
  }
  return await promiseUpdate();
}//updateMCcount

async function updateSCcount(jobid, recordcount){
  console.log('....inside updateSCcount ');
  var promiseUpdate = () => {
    return new Promise((resolve, reject) => {
        pool.query('UPDATE jobs SET sc_records=$1 WHERE jobid =$2', [ recordcount, jobid ], (err, results) => {
          if (err) { 
            console.log('....inside updateSCcount . reject');
            reject(err) 
          }else{
            console.log('....inside updateSCcount . resolve');
            resolve(results.rows);
          }});
    });
  }
  return await promiseUpdate();
}//updateSCcount

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
