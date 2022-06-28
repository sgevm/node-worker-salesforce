require("dotenv").config();
var throng = require('throng');
var Queue = require("bull");
var jsforce = require("jsforce");
const request = require("request");
var parseString = require('xml2js').parseString;
const db = require("./db");



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
  console.log(`start() : Started worker ${ id } / pid:` + process.pid);
  process.on('SIGTERM', ()=>{
    console.log('shutdown' + process.pid);
    db.run("UPDATE jobs SET status=? message=? WHERE status=?", ['Aborted', 'Aborted on shutdown', 'In Progress'],function(err,rows){
      if (err) { console(err); /*throw an error*/ }
    });
    disconnect();
  });


  // Connect to the named work queue
  let workQueue = new Queue('work', REDIS_URL);

  workQueue.process(maxJobsPerWorker, async (job) => {    
    // This is an example job that just slowly reports on progress
    // while doing no work. Replace this with your own job logic.
    var progress = 0;

    console.log(`....inside workQueue.process ${job.id}`);

    db.get('SELECT * FROM jobs WHERE jobid = ?', [ job.id ], function(err, row) {
      if (err) { console(err); /*throw an error*/ }
      if (!row) { 
        console.log(`....inside workQueue.process ${job.id} - before insert`);
        db.run('INSERT INTO jobs (jobid, status, message, mc_records, sc_records) VALUES (?, ?, ?, ?, ?)', [job.id, 'In Progress', 'New', 0, 0], function(err) {
          if (err) { console.log(err); /*throw an error*/ }
        });
      }else{
        db.run("UPDATE jobs SET status=? WHERE jobid=?", ['In Progress', job.id],function(err,rows){
          if (err) { console.log(err); /*throw an error*/ }
        });
      }
    });

    // var conn = new jsforce.Connection({
    //   oauth2 : {
    //     loginUrl : process.env.SF_LOGIN_URL,
    //     clientId : process.env.SF_CLIENT_ID,
    //     clientSecret : process.env.SF_CLIENT_SECRET,
    //     redirectUri : process.env.SF_REDIRECT_URL
    //   }
    // });

    // conn.login(process.env.SF_USERNAME, process.env.SF_PASSWORD, (err, userInfo) => {
    //   if (err) { return console.error(err); }
    //   progress = 50;
    //   job.progress(progress);
    //   //process.env.SF_ACCESS_TOKEN=conn.accessToken;
    //   //process.env.SF_INSTANCE_URL=conn.instanceUrl;

    //   const sobjectJSON = { ApexClass__c : 'jsforce', ApexMethodName__c: 'create', Object_Name__c: 'Custom_Errors__c', Name: `heroku-node-worker-job-${job.id}`};
    //   // Single record creation
    //   conn.sobject("Custom_Errors__c").create(sobjectJSON, function(err, ret) {
    //     if (err || !ret.success) { 
    //       console.error("Error in creating salesforce record : " + err);
    //       return console.error(err, ret); 
    //     }
    //     process.env.sfdcId = ret.id;
    //     console.log("Created record id : " + ret.id);
    //   });
      
    //   job.progress(90);
    //   conn.logout();      

    // });  

    generateToken(job);
    
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
      if (error) {
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
  
  //job.progress=50;

  //console.log('fetchDataExtensionRecords . body:'+JSON.stringify(reqBody));

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
      db.run("UPDATE jobs SET external_key=? WHERE jobid=?", [retrieveResponse.RequestID, jobid],function(err,rows){
        if (err) { console(err); /*throw an error*/ }
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
        //console.log('parseString: obj :' + JSON.stringify(obj));        
        return {...obj};
      });
      //console.log('fetchDataExtensionRecords . parseString . count:'+jsonResults.length + ' overallStatus: ' + overallStatus + ' requestId: ' + requestId);
      //process.env.DE_RECORDS = JSON.stringify(jsonResults);   
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
  //console.log('fetchDataExtensionRecords . records: ' + JSON.stringify(records));
  conn.bulk.load("ZipCode__c", "upsert", {extIdField:'ExternalKey__c'}, records, async (err, rets) => {
    if (err) { return console.error(err); }
    console.log('------3.2....inside salesforceBulkUpsert  bulk.load.upsert');
    //process.env.total_records = (process.env.total_records===undefined? parseInt(records.length):parseInt(process.env.total_records)+records.length);
    var successCounter=0;
    var failureCounter=0;
    for (var i=0; i < rets.length; i++) {
      if (rets[i].success) {
        successCounter++;
      } else {
        failureCounter++;
      }
    }//for
    //process.env.success_records = (process.env.success_records===undefined?successCounter:parseInt(process.env.success_records)+successCounter);
    //process.env.failure_records = (process.env.failure_records===undefined?failureCounter:parseInt(process.env.failure_records)+failureCounter);  
    //console.log('2.3....inside salesforceBulkUpsert . total_records='+process.env.total_records + ' success_records='+process.env.success_records + ' failure_records='+process.env.failure_records + ' pOverallStatus:' + pOverallStatus);  

    console.log('------3.3....calling updateSFSCRecordCount');
    await updateSFSCRecordCount(jobid, successCounter);

    if (pOverallStatus=='MoreDataAvailable') {
      console.log('------3.4....calling fetchDataExtensionRecords');
      await fetchDataExtensionRecords(jobid, conn, pOverallStatus, pRequestId);
    }else{
      console.log('------3.5....UPDATE jobs.status=Completed');
      db.run("UPDATE jobs SET status=?, end_dt=datetime('now') WHERE jobid=?", ['Completed', jobid],function(err,rows){
        if (err) { console(err); /*throw an error*/ }
      });
    }
  });//load
  
}//salesforceBulkUpsert

async function updateSFMCRecordCount(jobid, recordcount){
  console.log('----2.1....inside updateSFMCRecordCount ');
  var promiseQuery = () => {
    return new Promise((resolve, reject) => {
        db.get('SELECT * FROM jobs WHERE jobid = ?', [ jobid ], (err, row) => {
          if (err) { 
            reject(err) 
          }else{

            resolve(row);
            console.log('----2.2....inside updateSFMCRecordCount . promiseQuery.resolve');
          }});
    });
  }
  var row = await promiseQuery();

  var mc_records = (row.mc_records==undefined?0:row.mc_records);
  var newCount = mc_records + recordcount;
  console.log('----2.3....inside updateSFMCRecordCount . mc_records: ' + mc_records + ' newCount:' + newCount);

  var promiseUpate = () => { 
    return new Promise((resolve, reject) => {
      db.run("UPDATE jobs SET mc_records=? WHERE jobid=?", [newCount, row.jobid], (err,rows)=>{
          if (err) { 
            reject(err) 
          }else{
            resolve(rows);
            console.log('----2.4....inside updateSFMCRecordCount . promiseUpate.resolve');
          }});
    });
  }
  var rows = await promiseUpate();

  // await db.get('SELECT * FROM jobs WHERE jobid = ?', [ jobid ], async (err, row) => {
  //   if (err) { console(err); /*throw an error*/ }
  //   if (row) { 
  //     let mc_records = (row.MC_RECORDS==undefined?0:row.MC_RECORDS);
  //     console.log(`updateSFMCRecordCount . jobid = ${jobid} mc_records = ${mc_records}`);      
  //     mc_records += recordcount;
  //     console.log(`updateSFMCRecordCount . jobid = ${jobid} mc_records = ${mc_records}`);
  //     db.run("UPDATE jobs SET MC_RECORDS=? WHERE jobid=?", [mc_records, jobid],function(err,rows){
  //       if (err) { console(err); /*throw an error*/ 
  //       }else{
  //         console.log('successfully updated MC_RECORDS');
  //       }
  //     });
  //   }
  // });
}
async function updateSFSCRecordCount(jobid, recordcount){
  console.log('--------4.1....inside updateSFSCRecordCount ');
  var promiseQuery = () => {
    return new Promise((resolve, reject) => {
        db.get('SELECT * FROM jobs WHERE jobid = ?', [ jobid ], (err, row) => {
          if (err) { 
            reject(err) 
          }else{
            resolve(row);
            console.log('--------4.2....inside updateSFSCRecordCount promiseQuery.resolve');
          }});
    });
  }
  var row = await promiseQuery();

  var sc_records = (row.sc_records==undefined?0:row.sc_records);
  sc_records += recordcount;

  var promiseUpate = () => {
    return new Promise((resolve, reject) => {
      db.run("UPDATE jobs SET sc_records=? WHERE jobid=?", [sc_records, jobid], (err,rows)=>{
          if (err) { 
            reject(err) 
          }else{
            resolve(rows);
            console.log('--------4.3....inside updateSFSCRecordCount promiseUpate.resolve');
          }});
    });
  }
  var rows = await promiseUpate();

  /*
  await db.get('SELECT * FROM jobs WHERE jobid = ?', [ jobid ], async (err, row) => {
    if (err) { 
      console(err); 
      //throw an error 
    }
    if (row) { 
      let sc_records = (row.SC_RECORDS==undefined?0:row.SC_RECORDS);
      sc_records += recordcount;
      await db.run("UPDATE jobs SET SC_RECORDS=? WHERE jobid=?", [sc_records, jobid],function(err,rows){
        if (err) { 
          console(err); 
          //throw an error 
        }else{
          console.log('successfully updated SC_RECORDS');
        }
      });
    }
  });  
  */
}
// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
