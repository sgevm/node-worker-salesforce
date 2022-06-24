require("dotenv").config();
var throng = require('throng');
var Queue = require("bull");
var jsforce = require("jsforce");



// Connect to a local redis instance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

// Spin up multiple processes to handle jobs to take advantage of more CPU cores
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
let workers = process.env.WEB_CONCURRENCY || 2;

// The maximum number of jobs each worker should process at once. This will need
// to be tuned for your application. If each job is mostly waiting on network 
// responses it can be much higher. If each job is CPU-intensive, it might need
// to be much lower.
let maxJobsPerWorker = 1;


function start(id, disconnect) {
  console.log(`start() : Started worker ${ id }`);

  process.once('SIGTERM', shutdown);
  process.once('SIGINT', shutdown);

  // Connect to the named work queue
  let workQueue = new Queue('work', REDIS_URL);



  workQueue.process(maxJobsPerWorker, async (job) => {
    // This is an example job that just slowly reports on progress
    // while doing no work. Replace this with your own job logic.
    var progress = 0;
    var sfdcId = '';

    var conn = new jsforce.Connection({
      oauth2 : {
        loginUrl : process.env.SF_LOGIN_URL,
        clientId : process.env.SF_CLIENT_ID,
        clientSecret : process.env.SF_CLIENT_SECRET,
        redirectUri : process.env.SF_REDIRECT_URL
      }
    });

    conn.login(process.env.SF_USERNAME, process.env.SF_PASSWORD, function(err, userInfo) {
      if (err) { return console.error(err); }
      progress = 50;
      job.progress(progress);
      //process.env.SF_ACCESS_TOKEN=conn.accessToken;
      //process.env.SF_INSTANCE_URL=conn.instanceUrl;

      
      // Single record creation
      conn.sobject("Custom_Errors__c").create({ ApexClass__c : 'jsforce', ApexMethodName__c: 'create', Object_Name__c: 'Custom_Errors__c', Name: 'heroku-node-worker'}, function(err, ret) {
        if (err || !ret.success) { 
          console.error("Error in creating salesforce record : " + err);
          return console.error(err, ret); 
        }
        sfdcId = ret.id;
        console.log("Created record id : " + ret.id);
      });

      job.progress(90);
      conn.logout();
    });  
    
    

    // A job can return values that will be stored in Redis as JSON
    // This return value is unused in this demo application.
    return { value: 'Successful Salesforce interaction! job.Id: ' + job.id +' SFDC Record Id:' + sfdcId };
  });//process

  function shutdown() {
    console.log(`Worker ${ id } cleanup.`);
    disconnect();
  }//shutdown
}//start

// Initialize the clustered worker process
// See: https://devcenter.heroku.com/articles/node-concurrency for more info
throng({ workers, start });
