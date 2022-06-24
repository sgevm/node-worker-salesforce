// Store for all of the jobs in progress
let jobs = {};

// Kick off a new job by POST-ing to the server
async function addJob() {
  let res = await fetch('job/', {method: 'POST'});
  let job = await res.json();
  //console.log('addJob . job: ' + JSON.stringify(job));
  jobs[job.id] = {id: job.id, state: "queued"};
  render();
}

// Fetch updates for each job
async function updateJobs() {
  for (let id of Object.keys(jobs)) {
    let res = await fetch(`/job/${id}`);
    let result = await res.json();
    if (!!jobs[id]) {
      jobs[id] = result;
    }
    render();
  }
}

// Manual Refresh jobs info
function refresh() {
  updateJobs();
}//refresh

// Delete all stored jobs
function clear() {
  jobs = {};
  render();
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
  let returnvalue = job.returnvalue;
  let progress = job.progress || 0;
  let color = "bg-light-purple";

  if (job.state === "completed") {
    color = "bg-purple";
    progress = 100;
  } else if (job.state === "failed") {
    color = "bg-dark-red";
    progress = 100;
  }
  
  return document.querySelector('#job-template')
    .innerHTML
    .replace('{{id}}', job.id)
    .replace('{{state}}', job.state)
    .replace('{{color}}', color)
    .replace('{{progress}}', progress)
    .replace('{{returnvalue}}', returnvalue);
}

// Attach click handlers and kick off background processes
window.onload = function() {
  document.querySelector("#add-job").addEventListener("click", addJob);
  document.querySelector("#clear").addEventListener("click", clear);
  document.querySelector("#refresh").addEventListener("click", refresh);

  //setInterval(updateJobs, 200);
};
