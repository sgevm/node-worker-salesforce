// Store for all of the jobs in progress
let jobs = {};

// Kick off a new job by POST-ing to the server
async function addJob() {
  let res = await fetch('/job', {method: 'POST'});
  let job = await res.json();
  jobs[job.id] = {id: job.id, state: "queued"};
  await refresh();
}

async function removejob(e) {
  e.preventDefault();    
  var jobid=e.target.getAttribute("data-jobid");
  let res = await fetch(`/job/${jobid}`, {method: 'DELETE'});
  await refresh();
}//removejob

async function updateJobs() {
    jobs={};
    let res = await fetch(`/jobs`);
    var result = [];
    result = await res.json();
    result.forEach(job => {
      jobs[job.id] = job;
    });
}//updateJobs

async function renderJobsTable() {
  let res = await fetch(`/jobtable`);
  var rows = await res.json();
  var htmlval = '<table class="w-100 ba b--light-purple bg-lightest-purple" style="border-collapse: collapse;"><tr class="ba b--light-purple bg-light-purple"><th class="ba b--light-purple">Job Id</th><th class="ba b--light-purple">Request Id</th><th class="ba b--light-purple">Status</th><th class="ba b--light-purple">Message</th><th class="ba b--light-purple">Records From Mkt Cloud</th><th class="ba b--light-purple">Records Upserted in Sales Cloud</th><th class="ba b--light-purple">Start DateTime</th><th class="ba b--light-purple">End DateTime</th><th class="ba b--light-purple">Actions</th><tr>';
  rows.forEach(row => {
    htmlval += `<tr class="ba b--light-purple"><td class="ba b--light-purple">${row.jobid}</td><td class="ba b--light-purple">${row.external_key}</td><td class="ba b--light-purple">${row.status}</td><td class="ba b--light-purple">${row.message}</td><td class="ba b--light-purple">${row.mc_records}</td><td class="ba b--light-purple">${row.sc_records}</td><td class="ba b--light-purple">${row.start_dt}</td><td class="ba b--light-purple">${row.end_dt}</td><td><button data-jobid="${row.jobid}" onclick="removejob(event);" class='hk-button--primary'>Remove</button></td></tr>`;
  });
  htmlval += '</table>';
  console.log(htmlval);
  document.querySelector("#job-table").innerHTML = htmlval;
}//renderJobsTable

// Manual Refresh jobs info
async function refresh() {
  await updateJobs();
  for (let id of Object.keys(jobs)) {
    let res = await fetch(`/job/${id}`);
    let result = await res.json();
    if (!!jobs[id]) {
      jobs[id] = result;
    }      
  }
  render();
  await renderJobsTable();
}//refresh

async function refreshstatus() {
  await refresh();
}//refreshstatus

// Delete all stored jobs
async function clear() {
  jobs = {};
  await refresh();
}

// Update the UI
function render() {
  let s = "";
  for (let id of Object.keys(jobs)) {
    s += renderJob(jobs[id]);
  }

  // For demo simplicity this blows away all of the existing HTML and replaces it,
  // which is very inefficient. In a production app a library like React or Vue should
  // handle this work
  document.querySelector("#job-summary").innerHTML = s;
}

// Renders the HTML for each job object
function renderJob(job) {
  let reason = job.reason;
  let progress = job.progress || 0;
  let color = "bg-light-purple";
  if (job.state==undefined) {
    job.state='';
  }
  if (job.state === "completed") {
    color = "bg-purple";
    progress = 100;
  } else if (job.state === "failed") {
    color = "bg-dark-red";
    progress = 100;
  }
  
  return document.querySelector('#job-template')
    .innerHTML
    .replaceAll('{{id}}', job.id)
    .replace('{{state}}', job.state)
    .replace('{{color}}', color)
    .replace('{{progress}}', progress)
    .replace('{{reason}}', reason);
}//renderJob

// Attach click handlers and kick off background processes
window.onload = async function() {
  document.querySelector("#add-job").addEventListener("click", addJob);
  document.querySelector("#clear").addEventListener("click", clear);
  document.querySelector("#refresh").addEventListener("click", refresh);  
  await refresh();
};
