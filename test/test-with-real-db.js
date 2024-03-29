"use strict";

// WHEN COVER with NO DB NO DB
if(process.env.COVER==="ndb") return;

var expect = require('expect.js');
var tedious = require('tedious');
var sqlserver = require('..');
var colors = require('colors'); 
var bestGlobals = require('best-globals');
var discrepances = require('discrepances');
var MiniTools = require('mini-tools');
var TypeStore = require('type-store');
var {getConnectParams} = require('./helpers');

var fs = require('fs-extra');

console.warn(sqlserver.poolBalanceControl());

describe('sqlserver-strict with real database', function(){
    var connectParams;
    before(async function(){
        connectParams = await getConnectParams();
    });
    var expectedTable1Data = [
        {id:1, text1:'one'},
        {id:2, text1:'two'},
    ];
    describe('pool connections', function(){
        it('failed connection', function(){
            var errConnParams = bestGlobals.changing(connectParams, {authentication: {options: {user:'unnex_user', password: 'xxxx'}}});
            return MiniTools.readConfig([{db:errConnParams}, 'local-config'], {whenNotExist:'ignore'}).then(function(config){
                return sqlserver.connect(config.db);
            }).then(function(client){
                throw new Error('must raise error');
            }).catch(function(err){
                expect(err).to.be.a(Error);
                expect(err.message).to.match(/not? exist|auth?enti.*password|auth?enti.*fail|Login failed for user/);
                expect(err.code).to.match(/28000|28P01|ELOGIN/);
            });
        });
        it('failed connection within pool', async function(){
            var client = await sqlserver.connect(connectParams);
            try{
                await client.connect(connectParams);
                throw new Error('must throw error')
            }catch(err){
                expect(err.message).to.eql(sqlserver.messages.mustNotConnectClientFromPool);
            }
        });
        it('successful connection', async function() {
            this.timeout(5000)
            try {
                sqlserver.debug.Client=true;
                sqlserver.debug.pool=true;
                var config = await MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'});
                var client = await sqlserver.connect(config.db);
                console.log('ACA 1')
                expect(client).to.be.a(sqlserver.Client);
                expect(client._client).to.be.a(tedious.Connection);
                expect(sqlserver.poolBalanceControl().length>0).to.be.ok();
                await client.done();
                expect(sqlserver.poolBalanceControl().length==0).to.be.ok();
            } catch(err) {
                console.log('Check your database instalation. Then be sure to create the user and db with:');
                console.log("create user test_user password 'test_pass';".cyan);
                console.log("create database test_db owner test_user;".cyan);
                throw err;
            } finally {
                sqlserver.debug.Client=false;
            }
        });
        it('controls double done()', async function(){
            sqlserver.debug.Client=true;
            sqlserver.debug.pool=true;
            var config = await MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'});
            var client = await sqlserver.connect(config.db);
            client.done();
            sqlserver.debug.Client=false;
            try{
                await client.done();
                throw new Error("must throw an exception in promise")
            }catch(err){
                expect(err.message).to.match(/already done/);
            }
        });
    });
    describe('call queries', function(){
        var client;
        var poolLog;
        before(function(done){
            sqlserver.setAllTypes();
            sqlserver.log = sqlserver.logLastError;
            sqlserver.logLastError.inFileName='local-log-last-error.txt';
            MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'}).then(function(config){
                return sqlserver.connect(config.db);
            }).then(function(returnedClient){
                // if(sqlserver.poolBalanceControl().length>0) done(new Error("There are UNEXPECTED unbalanced conections"));
                client = returnedClient;
                done();
            });
        });
        after(function(){
            if(client){
                client.done();
            }
        });
        it("successful query that doesn't return rows", async function(){
            try {
                sqlserver.debug.Query=true;
                var result = await client.query("drop table if exists test_pgps.table1;").execute();
                expect(result.rowCount).to.not.be.ok();
                await client.query("drop table if exists test_pgps.table2;").execute();
                await client.query("drop table if exists test_pgps.table3;").execute();
                var result = await client.query("drop schema if exists test_pgps;").execute();
                expect(result.rowCount).to.not.be.ok();
            } finally {
                sqlserver.debug.Query=false;
            }
        });
        it.skip("executeSqlScript succefull", function(){
            return client.executeSqlScript("test/script-example.sql").then(function(result){
                expect(result.command).to.be("SELECT");
                expect(result.rows).to.eql(undefined);
            }).then(function(){
                sqlserver.debug.Query=false;
            });
        });
        it.skip("executeSqlScript with error", function(){
            return client.executeSqlScript("test/script-err-example.sql").then(function(result){
                throw new Error('must throw an error');
            },function(err){
                expect(err.code).to.eql('42601');
            });
        });
        function tipicalExecuteWay(queryText,done,commandExpected,resultExpected,functionName,params){
            return client.query(queryText,params)[functionName||"fetchAll"]().then(function(result){
                console.log('********************* sali del fetch')
                if(resultExpected){
                    for(var attr in resultExpected){
                        expect([attr,result[attr]]).to.eql([attr,resultExpected[attr]]);
                        discrepances.showAndThrow(result[attr],resultExpected[attr]);
                    }
                }else{
                    expect(result.rowCount).to.not.be.ok();
                }
                done();
            }).catch(done).then(function(){
                sqlserver.debug.Query=false;
            });
        }
        it("call execute directly", function(done){
            this.timeout(5000);
            tipicalExecuteWay("create schema test_pgps;",done,'CREATE');
        });
        async function tipicalFail(textQuery,reason,code,msg,functionName,expectedErrorLog, params){
            try{
                var result = await client.query(textQuery, params)[functionName||"execute"]();
                console.log("EXPECT FAIL BUT OBTAINS",result);
                throw new Error("Must fail because "+reason);
            }catch(err){
                if(err.message.startsWith('Must fail')){
                    throw err;
                }
                expect(err).to.be.a(Error);
                expect(err.message || err.errors[0].message).to.match(msg);
                expect(err.code ||  err.errors[0].code).to.be(code);
            }
            if(expectedErrorLog /*TODO: RESTAURAR ESTE CONTROL */ && false ){
                await sqlserver.readyLog;
                var content = await fs.readFile('local-log-last-error.txt','utf-8');
                if(expectedErrorLog instanceof RegExp){
                    expect(content).to.match(expectedErrorLog);
                }else{
                    expect(content).to.eql(expectedErrorLog);
                }
            }
        }
        it("failed call", function(){
            return tipicalFail("create schema test_pgps;","the schema exists",'EREQUEST',/(already.*|exist.*|test_pgps.*){2}/,
                null,
                /PG-ERROR --ERROR! 42P06.*test_pgps(.|\s)*-- QUERY(.|\s)*create schema test_pgps/m
            );
        });
        it("call a compound", function(done){
            tipicalExecuteWay(
                `begin
                create table test_pgps.table1(id integer primary key, text1 nvarchar(max));
                create table test_pgps.table2(text2 nvarchar(100) primary key, int2 integer);
                create table test_pgps.table3(id3 integer primary key, num3 numeric, dou3 double precision, dat3 date, big3 bigint);
                end`,
                done,
                "DO"
            )
        });
        it("successful delete that doesn't return rows", async function(){
            try {
                sqlserver.debug.Query=true;
                await client.query("delete from test_pgps.table1;").execute();
            } finally {
                sqlserver.debug.Query=false;
            }
        });
        it("call multiple insert", function(done){
            tipicalExecuteWay("insert into test_pgps.table1 values (1,'one'), (2,'two');",done,"INSERT",{
                rowCount:2
            },"execute")
        });
        it("query unique value", function(done){
            tipicalExecuteWay("select 3+$1",done,"SELECT",{
                value:8
            },"fetchUniqueValue",[5])
        });
        it("query unique date", function(done){
            tipicalExecuteWay("select cast('2020-02-14' as date)",done,"SELECT",{
                // value:bestGlobals.date.ymd(2020,2,14)
                value:new Date('2020-02-14')
            },"fetchUniqueValue")
        });
        it("fail to query unique value", function(){
            return tipicalFail("select 1 as one, CAST($1 as varchar) as b","returns 2 columns","54U11!",/query expects.*one field.*and obtains 2/,
                "fetchUniqueValue",
                /PG-ERROR --ERROR! 54U11!, query expects one field and obtains 2/,
                [2]
            )
        });
        it("query unique row replace limit with top when the first is commented", function(done){
            tipicalExecuteWay("select /*top (1)*/ * from test_pgps.table1 order by id limit 1",done,"SELECT",{
                row:expectedTable1Data[0]
            },"fetchUniqueRow")
        });
        it("query one row that exists", function(done){
            tipicalExecuteWay("select top 1 * from test_pgps.table1 order by id /*limit 1*/",done,"SELECT",{
                row:expectedTable1Data[0]
            },"fetchOneRowIfExists")
        });
        it("query one row that does not exist", function(done){
            tipicalExecuteWay("select * from test_pgps.table1 where false",done,"SELECT",{
                row:undefined
            },"fetchOneRowIfExists")
        });
        it("fail to query unique row", function(){
            return tipicalFail("select * from test_pgps.table1","returns 2 rows","54011!",/query expects.*one row.*and obtains 2/,
                "fetchUniqueRow",
                `PG-ERROR --ERROR! 54011!, query expects one row and obtains 2\n------- ------:\n-----------------------\n------- QUERY:\nselect * from test_pgps.table1;\n------- RESULT:\n-- [{"id":1,"text1":"one"},{"id":2,"text1":"two"}]`,
            )
        });
        it("fail to query unique row and obtains none", function(){
            return tipicalFail("select * from test_pgps.table1 where false","returns 0 rows","54011!",/query expects.*one row.*and obtains none/,
                "fetchUniqueRow",
                `PG-ERROR --ERROR! 54011!, query expects one row and obtains none\n------- ------:\n-----------------------\n------- QUERY:\nselect * from test_pgps.table1 where false;\n------- RESULT:\n-- []`,
            )
        });
        it("query row by row", async function() {
            var accumulate=[];
            await client.query("select * from test_pgps.table1 where id<$1 order  by id", [99]).onRow(function(row){
                accumulate.unshift(row);
            });
            accumulate.reverse();
            expect(accumulate).to.eql(expectedTable1Data);
        });
        it("control not call query row by row without callback", function(){
            return tipicalFail("select 1, 2","no callback provide","39004!",/fetchRowByRow must receive a callback/,
                "fetchRowByRow"
            )
        });
        it.skip('notices', async function(){
            var messages = []
            var consumerFunction=function(message){
                messages.push(message)
            }
            await client.query('SET client_min_messages = NOTICE;').execute();
            await client.query(
                `do $$
                begin
                    raise notice 'primer mensaje';
                    raise notice 'second message';
                end;$$`
            ).onNotice(consumerFunction).execute();
            expect(messages.map(notice=>notice.message)).to.eql([
                "primer mensaje",
                "second message"
            ]);
        })
        it.skip('get json', async function(){
            var sql='select id,text1 from test_pgps.table1';
            var result = await client.query(`
                select ${sqlserver.json(sql,'text1')} as arr, 
                    ${sqlserver.jsono(sql,'text1')} as obj
            `).fetchUniqueRow();
            expect(result.row).to.eql({
                arr: [
                    {id: 1, text1: "one"},
                    {id: 2, text1: "two"}
                ],
                obj: {
                    one:{id: 1, text1: "one"},
                    two:{id: 2, text1: "two"}
                }
            });
        })
        it.skip('get json 3rd param', async function(){
            var sql=`select id,text1,'x' as equis from test_pgps.table1`;
            var result = await client.query(`
                select ${sqlserver.json(sql,'text1',true)} as arr, 
                    ${sqlserver.json(sql,'text1','text1,id,equis'.split(','))} as arr2, 
                    ${sqlserver.jsono(sql,'text1','id')} as obj
            `).fetchUniqueRow();
            expect(result.row).to.eql({
                arr: [
                    {id: 1, equis:'x'},
                    {id: 2, equis:'x'}
                ],
                arr2: [
                    {id: 1, text1: "one", equis:'x'},
                    {id: 2, text1: "two", equis:'x'}
                ],
                obj: {
                    one:1,
                    two:2
                }
            });
        })
        describe('information_schema', function(){
            it('find a column', async function(){
                var info = await client.informationSchema.column('test_pgps','table1','text1');
                expect(info && info.data_type).to.eql('nvarchar');
            })
            it('not find a column', async function(){
                var info = await client.informationSchema.column('test_pgps','table1','text77');
                expect(info).to.eql(null);
            });
        });
    });
    describe('call queries with other languages', function(){
        var client;
        var poolLog;
        before(function(done){
            sqlserver.setAllTypes();
            sqlserver.log = sqlserver.logLastError;
            sqlserver.logLastError.inFileName='local-log-last-error.txt';
            MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'}).then(function(config){
                return sqlserver.connect(config.db);
            }).then(function(returnedClient){
                // if(sqlserver.poolBalanceControl().length>0) done(new Error("There are UNEXPECTED unbalanced conections"));
                client = returnedClient;
                done();
            });
            sqlserver.setLang('es');
        });
        after(function(){
            if(client){
                client.done();
            }
        });
        async function tipicalFail(textQuery,reason,code,msg,functionName,expectedErrorLog, params, msg2send){
            try{
                var result = await client.query(textQuery, params)[functionName||"execute"](msg2send);
                console.log("EXPECT FAIL BUT OBTAINS",result);
                throw new Error("Must fail because "+reason);
            }catch(err){
                if(err.message.startsWith('Must fail')){
                    throw err;
                }
                expect(err).to.be.a(Error);
                expect(err).to.match(msg);
                expect(err.code).to.be(code);
            }
            if(expectedErrorLog && false /* TODO: REINSTALL THIS COMPARISION */){
                await sqlserver.readyLog;
                var content = await fs.readFile('local-log-last-error.txt','utf-8');
                if(expectedErrorLog instanceof RegExp){
                    expect(content).to.match(expectedErrorLog);
                }else{
                    expect(content).to.eql(expectedErrorLog);
                }
            }
        }
        it("fail to query unique value", function(){
            return tipicalFail("select 1 as one, cast($1 as text) as b","returns 2 columns","54U11!",
                /se esperaba obtener un solo valor .* y se obtuvieron 2/,
                "fetchUniqueValue",
                'PG-ERROR --ERROR! 54U11!, se esperaba obtener un solo valor (columna o campo) y se obtuvieron 2\n------- ------:\n-----------------------\n------- QUERY-P:\n`select 1 as one, $1::text as b\n`\n------- QUERY-A:\n-- [2]\n------- QUERY:\nselect 1 as one, 2::text as b;\n------- RESULT:\n-- [{"one":1,"b":"2"}]',
                [2]
            )
        });
        it("fail to query unique value with custom message", function(){
            return tipicalFail("select 1 as one, cast($1 as text) as b","returns 2 columns","54U11!",
                /Solo un parametro puede leerse y se obtuvieron 2/,
                "fetchUniqueValue",
                'PG-ERROR --ERROR! 54U11!, Solo un parametro puede leerse y se obtuvieron 2\n------- ------:\n-----------------------\n------- QUERY-P:\n`select 1 as one, $1::text as b\n`\n------- QUERY-A:\n-- [2]\n------- QUERY:\nselect 1 as one, 2::text as b;\n------- RESULT:\n-- [{"one":1,"b":"2"}]',
                [2],
                "Solo un parametro puede leerse y $1"
            )
        });
        it("fail to query unique row", function(){
            return tipicalFail("select * from test_pgps.table1","returns 2 rows","54011!",
                /debe haber un solo registro en la tabla de test/,
                "fetchUniqueRow",
                `PG-ERROR --ERROR! 54011!, debe haber un solo registro en la tabla de test\n------- ------:\n-----------------------\n------- QUERY:\nselect * from test_pgps.table1;\n------- RESULT:\n-- [{"id":1,"text1":"one"},{"id":2,"text1":"two"}]`,
                [],
                "debe haber un solo registro en la tabla de test"
            )
        });
        it("fail to query one row if exists", function(){
            return tipicalFail("select * from test_pgps.table1","returns 2 rows","54011!",
                /demasiados registros se obtuvieron 2/,
                "fetchOneRowIfExists",
                `PG-ERROR --ERROR! 54011!, demasiados registros se obtuvieron 2\n------- ------:\n-----------------------\n------- QUERY:\nselect * from test_pgps.table1;\n------- RESULT:\n-- [{"id":1,"text1":"one"},{"id":2,"text1":"two"}]`,
                [],
                "demasiados registros $1"
            )
        });
        it("fail to then query", async function(){
            try{
                await client.query("select 1");
                throw new Error('must throw an error');
            }catch(err){
                expect(err.message).to.match(/await.*then/)
            }
        })
        it("fail to catch query", async function(){
            try{
                await client.query("select 2").catch(x=>x);
                throw new Error('must throw an error');
            }catch(err){
                expect(err.message).to.match(/await.*catch/)
            }
        })
        it.skip("bulk insert", async function(){
            await client.bulkInsert({
                schema: "test_pgps", 
                table: "table1", 
                columns: ['id', 'text1'],
                rows: [
                    [3, 'three'],
                    [4, 'four'],
                    [5, 'five'],
                    [6, 'six'],
                    [7, 'seven'],
                    [8, 'eight'],
                    [9, 'nine'],
                    [10, 'ten'],
                    [11, 'eleven'],
                    [12, 'twelve'],
                    [13, 'thirteen'],
                    [14, 'fourteen'],
                    [15, 'fifteen'],
                    [16, 'sixteen'],
                    [17, 'seventeen'],
                    [18, 'eighteen'],
                    [19, 'nineteen'],
                    [20, 'twenty'],
                    [21, 'twenty-one'],
                    [22, 'twenty-two'],
                    [23, 'twenty-three'],
                    [30, 'thirty'],
                    [40, 'forty'],
                    [50, 'fifty'],
                    [60, 'sixty'],
                    [70, 'seventy'],
                    [80, 'eighty'],
                    [90, 'ninety'],
                    [100, 'one hundred'],
                    [101, 'one hundred and one'],
                    [200, 'two hundred'],
                    [300, 'three hundred'],
                    [1000, 'one thousand'],
                    [1000000, 'one million'],
                    [10000000, 'ten million'],               
                ]
            });
            var result = await client.query("select sum(id) as sum_id from test_pgps.table1").fetchUniqueRow();
            expect(result.row.sum_id).to.eql(11002397);
        });
        it.skip("bulk insert with recovery", async function(){
            var recovered = [];
            await client.query("set search_path = test_pgps").execute();
            await client.bulkInsert({
                table: "table1", 
                columns: ['id', 'text1'],
                rows: [
                    [1001, 'mil uno'],
                    [1001, 'mil uno otra vez'],
                ],
                onerror:function(err, row){
                    recovered.push(row);
                }
            });
            var result = await client.query("select sum(id) as sum_id from test_pgps.table1 where id between 1001 and 1999").fetchUniqueRow();
            expect(result.row.sum_id).to.eql(1001);
            expect(recovered).to.eql([[1001, 'mil uno otra vez']]);
        });
        it.skip("bulk insert with error", async function(){
            var recovered = [];
            await client.query("set search_path = test_pgps").execute();
            try{
                await client.bulkInsert({
                    table: "table1", 
                    columns: ['id', 'text1'],
                    rows: [
                        [2001, 'mil uno'],
                        [2001, 'mil uno otra vez'],
                    ],
                });
                throw new Error('must throw an error')
            }catch(err){
                expect(err.code).to.eql('23505');
            }
            var result = await client.query("select sum(id) as sum_id from test_pgps.table1 where id between 2001 and 2999").fetchUniqueRow();
            expect(result.row.sum_id).to.eql(2001);
        });
    })
    describe('pool-less connections', function(){
        describe('call queries', function(){
            var client;
            before(function(done){
                MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'}).then(function(config){
                    client = new sqlserver.Client(config.db);
                    done();
                });
                sqlserver.setLang('en');
            });
            it("successful query", function(done){
                client.connect().then(function(){
                    return client.query("select * from test_pgps.table1 where id<3 order by id;").fetchAll();
                }).then(function(result){
                    expect(result.rows).to.eql(expectedTable1Data);
                    client.end();
                    done();
                }).catch(done).then(function(){
                });
            });
            it("unsuccessful query", async function(){
                this.timeout(15000);
                sqlserver.debug.Client=true;
                var config = MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'});
                try {
                    var client = new sqlserver.Client("Server=localhost;Database=this_db;User Id=this_user;Password=this_pass;")
                    expect(client).to.be.a(sqlserver.Client);
                    await client.connect();
                    expect(client._client).to.be.a(tedious.Client);
                    throw new Error("must raise error");
                } catch(err) {
                    if (config.db?.port==connectParams.port) {
                        expect(err.message).to.match(/auth?enti.*password|not? exist|auth?enti.*fail/);
                    } else {
                        expect(err.message).to.match(/ECONNREFUSED/);
                    }
                };
            });
            it("connect with extra parameter", function(done){
                this.timeout(5000);
                sqlserver.debug.Client=true;
                client = new sqlserver.Client("this_user@xxxx");
                expect(client).to.be.a(sqlserver.Client);
                expect(client._client).to.be.a(tedious.Client);
                client.connect("extra parameter").then(function(){
                    done(new Error("must raise error because must not have parameters"));
                }).catch(function(err){
                    expect(err.message).to.match(/must no receive parameters/);
                    done();
                }).catch(done).then(function(){
                });
            });
        });
    });
    describe.skip("onRow async ensures", function(){
        var client;
        before(function(done){
            sqlserver.setAllTypes();
            MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'}).then(function(config){
                return sqlserver.connect(config.db);
            }).then(function(returnedClient){
                // if(sqlserver.poolBalanceControl().length>0) done(new Error("There are UNEXPECTED unbalanced conections"));
                client = returnedClient;
                done();
            });
        });
        after(function(){
            if(client){
                client.done();
            }
        });
        it("immediate row processing", async function(){
            var adder=0;
            await client.query('select num from generate_series(1,10) num').onRow(async function(row){
                adder+=row.num;
            });
            expect(adder).to.eql(55)
        })
        it("wait for each row be processed", async function(){
            var adder=0;
            await client.query('select num from generate_series(1,10) num').onRow(async function(row){
                await bestGlobals.sleep(100)
                adder+=row.num;
            });
            expect(adder).to.eql(55)
        })
        it("do not wait for each row be processed", async function(){
            var adder=0;
            await client.query('select num from generate_series(1,10) num').onRow(function(row){
                setTimeout(function(){
                    adder+=row.num;
                },100);
            });
            expect(adder).to.eql(0)
        })
    })
});

