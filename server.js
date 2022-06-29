let express = require('express');
let Queue = require('bull');
var pool = require("./db");

// Serve on PORT on Heroku and on localhost:5000 locally
let PORT = process.env.PORT || '5000';
// Connect to a local redis intance locally, and the Heroku-provided URL in production
let REDIS_URL = process.env.REDIS_URL || 'redis://127.0.0.1:6379';

let app = express();

// Create / Connect to a named work queue
let workQueue = new Queue('work', REDIS_URL);

// Serve the two static assets
app.get('/', (req, res) => res.sendFile('index.html', { root: __dirname }));
app.get('/client.js', (req, res) => res.sendFile('client.js', { root: __dirname }));

// Kick off a new job by adding it to the work queue
app.post('/job', async (req, res) => {
  // This would be where you could pass arguments to the job
  // Ex: workQueue.add({ url: 'https://www.heroku.com' })
  // Docs: https://github.com/OptimalBits/bull/blob/develop/REFERENCE.md#queueadd
  console.log('server . POST /job . req');
  //console.log(req);
  let job = await workQueue.add();
  console.log('server . POST /job . job.id:'+job.id);
  res.json({ id: job.id });
});

// Allows the client to query the state of a background job
app.get('/job/:id', async (req, res) => {
  let id = req.params.id;
  let job = await workQueue.getJob(id);

  if (job === null) {
    res.status(404).end();
  } else {
    let state = await job.getState();
    let progress = job._progress;
    let reason = job.failedReason;
    let returnvalue = (job.returnvalue==undefined?'':job.returnvalue.value);
    res.json({ id, state, progress, reason, returnvalue });
  }
});

// Allows the client to query the job logs
app.get('/job/:id/logs', async (req, res) => {
  let id = req.params.id;
  let job = await workQueue.getJob(id);
  if (job === null) {
    res.status(404).end();
  } else {
    //console.log(workQueue);
    //let logsPromise = await workQueue.getJobLogs(id);
    let logs = 'logs';//await logsPromise.json();
    res.json({ logs });
  }  
});

// Allows the client to query the state of a background job
app.get('/jobs', async (req, res) => {
  var jobs = await workQueue.getJobs(['waiting','delayed','active','completed','failed']); //completed, failed, delayed, active, waiting, paused, stuck or null
  var jsonVal = [];
  jobs.forEach((job)=>{    
    var id = job.id;
    var progress = job._progress;
    var state = '';//job.getState();
    var reason = job.failedReason;
    var returnvalue = (job.returnvalue==undefined?'':job.returnvalue.value);
    jsonVal.push({ id, state, progress, reason, returnvalue });    
  });
  //console.log(jsonVal);
  res.json(jsonVal);
});


app.get('/jobtable', async (req, res) => {

  var rows = await queryAllJobs();
  if (rows!=undefined) {
    var rowsjson = rows.map((row) => {
      return {jobid: row.jobid, external_key: row.external_key, status: row.status, message: row.message, mc_records: row.mc_records, sc_records: row.sc_records, start_dt:row.start_dt, end_dt:row.end_dt};
    });
    console.log('GET /jobtable');
    console.log(rowsjson);
    res.json(rowsjson);    
  }else{
    res.json([]);    
  }

});

// Allows the client to query the state of a background job
app.delete('/job/:id', async (req, res) => {
  let id = req.params.id;
  console.log('DELETE /job/:id:'+id);
  let job = await workQueue.getJob(id);

  if (job === null) {
    //res.status(404).end();
    console.log('job not found for deletion');
  } else {
    let state = await job.remove();
    console.log('DELETE /job/:id:'+id + ' . removed');
    res.sendStatus(200);// json({id});
  }

  pool.query("DELETE FROM jobs WHERE jobid=$1", [id], function(err) {
    if(err){
        console.log(err)
    }
    else{
        console.log("DELETE Successful");
    }
    res.sendStatus(200);
  });
});

async function queryAllJobs(){
  console.log('....inside queryAllJobs ');
  var promiseQuery = () => {
    return new Promise((resolve, reject) => {
        pool.query('SELECT * FROM jobs;', [], (err, results) => {
          if (err) { 
            console.log('....inside queryAllJobs .reject ');
            reject(err) 
          }else{
            console.log('....inside queryAllJobs . resolve . ' + (results==undefined?0:results.rows.length));
            resolve(results.rows);
          }});
    });
  }
  return await promiseQuery();
}//queryAllJobs

// You can listen to global events to get notified when jobs are processed
workQueue.on('global:completed', (jobId, result) => {
  console.log(`Job completed with result ${result}`);
});

app.listen(PORT, () => console.log("Server started!"));
