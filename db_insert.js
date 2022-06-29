const db = require("./db");


db.run('INSERT INTO jobs(jobid, status, message, mc_records, sc_records) VALUES(?,?,?,?,?);', [ 5, 'In Progress', 'New', 0, 0 ], (err) => {
    if (err) { 
    console.log('....insert error');
    console.log(err);
    }else{
    console.log('....insert success');
}});           
 
dbc.all('SELECT * FROM jobs', [], function(err, rows) {
    if (err) { 
        console.log('error with SELECT');
        console.log(err);
    }
    if (rows) {
        console.log('success with SELECT');
        console.log(rows);
    }else{
        console.log('success with SELECT . no rows');
    }
});
