"use strict";
/*jshint eqnull:true */
/*jshint globalstrict:true */
/*jshint node:true */
/*jshint -W100 */

var sqlserver = require('..');

sqlserver.debug.pool={};

/** @type {import('..').ConnectParams} */
var conOpts = {
    server: "localhost",
    options: {
        useColumnNames:true, 
        trustServerCertificate: true,
        database: "test_db",
    },
    authentication: {
        type: "default",
        options: {  
            userName: "test_user",
            password: "test_pass",
        }
    },
};

async function example(){
    console.log('running example...')
    try{
        var client = await sqlserver.connect(conOpts);
        var {value} = await client.query('select count(*) from test_pgps.table1').fetchUniqueValue()
        console.log('rowCount',value);
        var {rows} = await client.query('select * from test_pgps.table1 order by id').fetchAll();
        console.log('rows',rows);
        await client.query("select * from test_pgps.table1 order by 2").onRow(async function(row){ 
            console.log('read one row',row);
        });
        console.log('done!');
        client.done();
        sqlserver.shutdown(true);
    } finally {
        console.log('finally done')
    }
}

example();