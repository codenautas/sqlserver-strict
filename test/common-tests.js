"use strict";

var assert = require('assert');
var expect = require('expect.js');
var tedious = require('tedious');
var sqlserver = require('../dist/lib/sqlserver-strict.js');
var queryWithEmitter = require('./query-with-emitter.js');
var bestGlobals = require('best-globals');

var MiniTools = require('mini-tools');
var {getConnectParams} = require('./helpers.js');

describe('sqlserver-strict common tests', function(){
    var connectParams;
    var client;
    var poolLog;
    before(async function(){
        connectParams = await getConnectParams();
        this.timeout(5000)
        sqlserver.allTypes=true;
        sqlserver.alsoLogRows=true;
        var config = await MiniTools.readConfig([{db:connectParams}, 'local-config'], {whenNotExist:'ignore'})
        console.log('config',config)
        client = await sqlserver.connect(config.db);
        console.log('conectado!')
    });
    after(async function(){
        console.log('en el after')
        if(client) await client.done();
        console.log('salio del after')
        // sqlserver.shutdown()
    });
    describe('internal controls', function(){
        it('control the log in error',function(){
            var messages=[];
            sqlserver.log=function(message){
                messages.push(message);
            };
            return Promise.resolve().then(function(){
                return client.query(
                    'select $1, $2, $3, $4, $5, illegal syntax here, $6', 
                    [1, "one's", true, null, bestGlobals.date.iso('2019-01-05'), {typeStore:true, toLiteral(){ return 'lit'}}]
                ).execute();
            }).catch(function(err){
                var resultExpected="ERROR! "+err.code+", "+err.message;
                console.log(messages);
                expect(messages).to.eql([
                    '-----------------------',
                    '`select $1, $2, $3, $4, $5, illegal syntax here, $6\n`',
                    '-- [1,"one\'s",true,null,'+JSON.stringify(bestGlobals.date.iso('2019-01-05'))+',"lit"]',
                    "select 1, 'one\'\'s', true, null, '2019-01-05', illegal syntax here, 'lit';",
                    '--'+resultExpected
                ]);
                messages=[];
                return client.query("select 'exit', 0/0 as inf").execute().catch(function(err){
                    var resultExpected="ERROR! "+err.code+", "+err.message;
                    expect(messages).to.eql([
                        '-----------------------',
                        "select 'exit', 0/0 as inf;",
                        '--'+resultExpected
                    ]);
                    sqlserver.log=null;
                });
            });
        });
        it('log with data',function(){
            var messages=[];
            sqlserver.log=function(message,type){
                messages.push([type,message]);
            };
            return Promise.resolve().then(function(){
                return client.query(
                    'select 1 as one union select 2'
                ).execute();
            }).then(function(result){
                expect(messages).to.eql([
                    ["------","-----------------------"],
                    ["QUERY","select 1 as one union select 2;"],
                    ["RESULT","-- [{\"one\":1},{\"one\":2}]"]
                ]);
            });
        });
        it('log with data row by row',function(){
            var messages=[];
            sqlserver.log=function(message,type){
                messages.push([type,message]);
            };
            return Promise.resolve().then(function(){
                return client.query(
                    'select 1 as one union select 2', 
                    
                ).onRow(function(){});
            }).then(function(result){
                expect(messages).to.eql([
                    ["------","-----------------------"],
                    ["QUERY","select 1 as one union select 2;"],
                    ["ROW","-- {\"one\":1}"],
                    ["ROW","-- {\"one\":2}"],
                    ["RESULT","-- []"]
                ]);
            });
        });
    });
    describe('service', function(){
        it("quoteIdent", function(){
            expect(sqlserver.quoteIdent("column1")).to.eql('"column1"');
            expect(sqlserver.quoteIdent('column"delta"')).to.eql('"column""delta"""');
            expect(sqlserver.quoteIdentList(['voilà','c\'est fini'])).to.eql('"voilà","c\'est fini"');
        });
        it("quoteLiteral", function(){
            // expect(sqlserver.quoteLiteral('hi')).to.eql("'hi'");
            // expect(sqlserver.quoteLiteral("don't")).to.eql("'don''t'");
            // expect(sqlserver.quoteLiteral(7)).to.eql("'7'");
            // expect(sqlserver.quoteLiteral({a:5})).to.eql(`'{"a":5}'`);
            // expect(sqlserver.quoteLiteral(new Date('2018-12-24'))).to.eql("'2018-12-24T00:00:00.000Z'");
            // expect(sqlserver.quoteLiteral(bestGlobals.date.iso('2018-12-25'))).to.eql("'2018-12-25'");
            // expect(sqlserver.quoteLiteral(new Date('2018-12-24 10:20'))).to.eql("'2018-12-24T00:00:00.000Z'");
            expect(sqlserver.quoteLiteral(bestGlobals.datetime.iso('2018-12-26 10:20:30'))).to.eql("'2018-12-26 10:20:30'");
        });
        it("quoteNullable", function(){
            expect(sqlserver.quoteNullable('hi')).to.eql("'hi'");
            expect(sqlserver.quoteNullable("don't")).to.eql("'don''t'");
            expect(sqlserver.quoteNullable(7)).to.eql("'7'");
            expect(sqlserver.quoteNullable(null)).to.eql("null");
            expect(sqlserver.quoteNullable(true)).to.eql("'true'");
            expect(()=>sqlserver.quoteNullable(sqlserver.quoteNullable)).to.throwError(/insane value/);
        });
    });
    describe('handle errors', function(){
        it("reject non string object names", function(){
            expect(function(){
                sqlserver.quoteIdent(null);
            }).to.throwError(/name/i);
        });
        it("reject null text", function(){
            expect(function(){
                sqlserver.quoteLiteral(null);
            }).to.throwError(/null/i);
        });
    });
});
