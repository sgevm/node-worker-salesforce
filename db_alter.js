var sqlite3 = require('sqlite3');
var mkdirp = require('mkdirp');

mkdirp.sync('./var/db');

var dbc = new sqlite3.Database('./var/db/jobs.db',(err)=>{
    if (err) {
        console.log('error connecting to database');  
        console.log(err);    
    }else{
        dbc.run("ALTER TABLE jobs ADD COLUMN mc_records INTEGER");
        dbc.run("ALTER TABLE jobs ADD COLUMN sc_records INTEGER");
        dbc.run("ALTER TABLE jobs ADD COLUMN start_dt TEXT DEFAULT CURRENT_TIMESTAMP");
        dbc.run("ALTER TABLE jobs ADD COLUMN end_dt TEXT");

        dbc.getAll('SELECT * FROM jobs', [], function(err, rows) {
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
    }    
});



