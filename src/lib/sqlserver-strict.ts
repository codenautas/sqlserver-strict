"use strict";

import * as tedious from 'tedious';

import {promises as fs} from 'fs';

import * as util from 'util';
import * as likeAr from 'like-ar';
import { unexpected } from 'cast-error';
import {Stream, Transform} from 'stream';

const MESSAGES_SEPARATOR_TYPE='------';
const MESSAGES_SEPARATOR='-----------------------';

export var messages = {
    attemptTobulkInsertOnNotConnected:"sqlserver-strict: atempt to bulkInsert on not connected",
    attemptTocopyFromOnNotConnected:"sqlserver-strict: atempt to copyFrom on not connected",
    attemptToExecuteSentencesOnNotConnected:"sqlserver-strict: atempt to executeSentences on not connected",
    attemptToExecuteSqlScriptOnNotConnected:"sqlserver-strict: atempt to executeSqlScript on not connected",
    clientAlreadyDone:"sqlserver-strict: client already done",
    clientConenctMustNotReceiveParams:"client.connect must no receive parameters, it returns a Promise",
    copyFromInlineDumpStreamOptsDoneExperimental:"WARNING! copyFromInlineDumpStream opts.done func is experimental",
    fetchRowByRowMustReceiveCallback:"fetchRowByRow must receive a callback that executes for each row",
    formatNullableToInlineDumpErrorParsing:"formatNullableToInlineDump error parsing",
    insaneName:"insane name",
    lackOfClient:"sqlserver-strict: lack of Client._client",
    mustNotConnectClientFromPool:"sqlserver-strict: Must not connect client from pool",
    mustNotEndClientFromPool:"sqlserver-strict: Must not end client from pool",
    nullInQuoteLiteral:"null in quoteLiteral",
    obtains1:"obtains $1",
    obtainsNone:"obtains none",
    queryExpectsOneFieldAnd1:"query expects one field and $1",
    queryExpectsOneRowAnd1:"query expects one row and $1",
    queryMustNotBeCatched:"sqlserver-strict: Query must not be awaited nor catched",
    queryMustNotBeThened:"sqlserver-strict: Query must not be awaited nor thened",
    queryNotConnected:"sqlserver-strict: query not connected",
    unbalancedConnection:"sqlserver-strict.debug.pool unbalanced connection",
}

export var i18n:{
    messages:{
        en:typeof messages,
        [k:string]:Partial<typeof messages>
    }
} = {
    messages:{
        en:messages,
        es:{
            attemptTobulkInsertOnNotConnected:"sqlserver-strict: intento de bulkInsert en un cliente sin conexion",
            attemptTocopyFromOnNotConnected:"sqlserver-strict: intento de copyFrom en un cliente sin conexion",
            attemptToExecuteSentencesOnNotConnected:"sqlserver-strict: intento de executeSentences en un cliente sin conexion",
            attemptToExecuteSqlScriptOnNotConnected:"sqlserver-strict: intento de executeSqlScript en un cliente sin conexion",
            clientAlreadyDone:"sqlserver-strict: el cliente ya fue terminado",
            clientConenctMustNotReceiveParams:"sqlserver-strict: client.connect no debe recibir parametetros, devuelve una Promesa",
            copyFromInlineDumpStreamOptsDoneExperimental:"WARNING! copyFromInlineDumpStream opts.done es experimental",
            fetchRowByRowMustReceiveCallback:"fetchRowByRow debe recibir una funcion callback para ejecutar en cada registro",
            formatNullableToInlineDumpErrorParsing:"error al parsear en formatNullableToInlineDump",
            insaneName:"nombre invalido para objeto sql, debe ser solo letras, numeros o rayas empezando por una letra",
            lackOfClient:"sqlserver-strict: falta Client._client",
            mustNotConnectClientFromPool:"sqlserver-strict: No se puede conectar un 'Client' de un 'pool'",
            mustNotEndClientFromPool:"sqlserver-strict: no debe terminar el client desde un 'pool'",
            nullInQuoteLiteral:"la funcion quoteLiteral no debe recibir null",
            obtains1:"se obtuvieron $1",
            obtainsNone:"no se obtuvo ninguno",
            queryExpectsOneFieldAnd1:"se esperaba obtener un solo valor (columna o campo) y $1",
            queryExpectsOneRowAnd1:"se esperaba obtener un registro y $1",
            queryMustNotBeCatched:"sqlserver-strict: Query no puede ser usada con await o catch",
            queryMustNotBeThened:"sqlserver-strict: Query no puede ser usada con await o then",
            queryNotConnected:"sqlserver-strict: 'query' no conectada",
        }
    }
}

export function setLang(lang:string){
    /* istanbul ignore else */
    if(lang in i18n.messages){
        messages = {...i18n.messages.en, ...i18n.messages[lang]};
    }
}

export var debug:{
    pool?:true|{
        [key:string]:{ count:number, client: tedious.Connection }
    }
}={};

export var defaults={
    releaseTimeout:{inactive:60000, connection:600000}
};

/* instanbul ignore next */
export function noLog(_message:string, _type:string){}

export var log:(message:string, type:string)=>void=noLog;
export var alsoLogRows = false;
export var logExceptions = false;

export function quoteIdent(name:string){
    if(typeof name!=="string"){
        if(logExceptions){
            console.error('Context for error',{name})
        }
        throw new Error(messages.insaneName);
    }
    return '"'+name.replace(/"/g, '""')+'"';
};

export function quoteIdentList(objectNames:string[]){
    return objectNames.map(function(objectName){ return quoteIdent(objectName); }).join(',');
};

export type AnyQuoteable = string|number|Date|{isRealDate:boolean, toYmd:()=>string}|{toSqlString:()=>string}|{toString:()=>string};
export function quoteNullable(anyValue:null|AnyQuoteable){
    if(anyValue==null){
        return 'null';
    }
    var text:string
    if(typeof anyValue==="string"){
        text = anyValue;
    }else if(!(anyValue instanceof Object)){
        text=anyValue.toString();
    }else if('isRealDate' in anyValue && anyValue.isRealDate){
        text = anyValue.toYmd();
    }else if(anyValue instanceof Date){
        text = anyValue.toISOString();
    }else if('toSqlString' in anyValue && anyValue.toSqlString instanceof Function){
        text = anyValue.toSqlString();
    }else{
        text = JSON.stringify(anyValue);
    }
    if(text==undefined){
        if(logExceptions){
            console.error('Context for error',{anyValue})
        }
        throw new Error('quotableNull insane value: '+typeof anyValue)
    }
    return "'"+text.replace(/'/g,"''")+"'";
};

export function quoteLiteral(anyValue:AnyQuoteable){
    if(anyValue==null){
        if(logExceptions){
            console.error('Context for error',{anyValue})
        }
        throw new Error(messages.nullInQuoteLiteral);
    }
    return quoteNullable(anyValue);
};

export const param3rd4sql=(exprOrWithoutkeyOrKeys?:string|true|string[], base?:string, keys?:string|string[])=>
    exprOrWithoutkeyOrKeys==true?`to_jsonb(${base}) - ${keys instanceof Array?keys:keys?.split(',').map(x=>quoteLiteral(x.trim()))}`:
    exprOrWithoutkeyOrKeys==null?`to_jsonb(${base})`:
    typeof exprOrWithoutkeyOrKeys == "string"?exprOrWithoutkeyOrKeys:
    `to_jsonb(jsonb_build_object(${exprOrWithoutkeyOrKeys.map(name=>quoteLiteral(name)+', '+quoteIdent(name)).join(', ')}))`
    ;

export function json(sql:string, orderby:string,expr:string):string;
export function json(sql:string, orderby:string,keys:string[]):string;
export function json(sql:string, orderby:string,withoutKeys:true):string;
export function json(sql:string, orderby:string):string;
export function json(sql:string, orderby:string,exprOrWithoutkeyOrKeys?:string|true|string[]){
    return `COALESCE((SELECT jsonb_agg(${param3rd4sql(exprOrWithoutkeyOrKeys,'j.*',orderby)} ORDER BY ${orderby}) from (${sql}) as j),'[]'::jsonb)`;
    // return `(SELECT coalesce(jsonb_agg(to_jsonb(j.*) ORDER BY ${orderby}),'[]'::jsonb) from (${sql}) as j)`
}

export function jsono(sql:string, indexedby:string,expr:string):string;
export function jsono(sql:string, indexedby:string,keys:string[]):string;
export function jsono(sql:string, indexedby:string,withoutKeys:true):string;
export function jsono(sql:string, indexedby:string):string;
export function jsono(sql:string, indexedby:string,exprOrWithoutkeyOrKeys?:string|true|string[]){
    return `COALESCE((SELECT jsonb_object_agg(${indexedby},${param3rd4sql(exprOrWithoutkeyOrKeys,'j.*',indexedby)}) from (${sql}) as j),'{}'::jsonb)`
}

export function adaptParameterTypes(parameters?:any[]){
    // @ts-ignore 
    if(parameters==null){
        return null;
    }
    return parameters.map(function(value){
        if(value && value.typeStore){
            return value.toLiteral();
        }
        return value;
    });
};

export type ConnectParams = tedious.ConnectionConfiguration & {
    host?:string // server alias
};

export type CopyFromOptsCommon={table:string,columns?:string[],done?:(err?:Error)=>void, with?:string}
export type CopyFromOptsFile={inStream?:undefined, filename:string}&CopyFromOptsCommon
export type CopyFromOptsStream={inStream:Stream,filename?:undefined}&CopyFromOptsCommon
export type CopyFromOpts=CopyFromOptsFile|CopyFromOptsStream
export type BulkInsertParams={schema?:string,table:string,columns:string[],rows:any[][], onerror?:(err:Error, row:any[])=>Promise<void>}

export type Column = {data_type:string};

export class InformationSchemaReader{
    constructor(private client:Client){
    }
    async column(table_schema:string, table_name:string, column_name:string):Promise<Column|null>{
        var result = await this.client.query(`
            select * 
                from information_schema.columns
                where table_schema=$1
                    and table_name=$2
                    and column_name=$3;
        `,[table_schema, table_name, column_name]).fetchOneRowIfExists(); 
        if (!result.row) return null;
        var info = {} as Record<string, any>;
        for (let field in result.row) {
            info[field.toLowerCase()] = result.row[field];
        }
        // @ts-ignore emits more!
        return info;
    }
}

function toUninterpolatedQuery(sql:string, parameters:any[]){
    var sentence = sql.replace(/(\/\*\s*TOP\s*\((\d+)\)\s*\*\/)(.*)(LIMIT\s*(\d+))/gi, 
        function (all, _top, topN, between, limit, limitN) {
            if (topN == limitN) {
                return `TOP (${topN})${between} /*${limit}*/`;
            }
            return all;
        }
    ).replace(/\btrue\b/ig,'(/*true*/1=1)')
    .replace(/\bfalse\b/ig,'(/*false*/1=0)');
    var result = sentence.replace(/\$(\d+)\b/g, (_, number:number) => quoteNullable(parameters[number - 1]));
    console.log("*************** SQL:", result)
    return result
}


function normalizeConnectionOptions(opts:ConnectParams):tedious.ConnectionConfiguration{
    const {host, ...normOpts} = opts
    const newOpts = {...normOpts, server:opts.host ?? opts.server}
    return newOpts;
}


/** TODO: any en opts */
export class Client{
    private connected:null|{
        lastOperationTimestamp:number,
        lastConnectionTimestamp:number
    }=null;
    private fromPool:boolean=false;
    private postConnect(){
        var nowTs=new Date().getTime();
        this.connected = {
            lastOperationTimestamp:nowTs,
            lastConnectionTimestamp:nowTs
        }
    }
    private _pendingConection?: Promise<tedious.Connection>;
    private _client?: tedious.Connection;
    private _informationSchema:InformationSchemaReader|null=null;
    registerDebugPool(client:tedious.Connection){
        // @ts-expect-error secretKey is a hack
        var secretKey = client.secretKey = client.secretKey ?? 'secret_'+Math.random();
        if(debug.pool){
            if(debug.pool===true){
                debug.pool={};
            }
            if(!(secretKey in debug.pool)){
                debug.pool[secretKey] = {client, count:0};
            }
            debug.pool[secretKey].count++;
        }
    }
    constructor(connOpts:ConnectParams)
    constructor(connOpts:null, client:tedious.Connection)
    constructor(connOpts:ConnectParams|null, client?:tedious.Connection){
        if(client != null) {
            this._client = client;
            this.registerDebugPool(client)
            this.fromPool=true;
            this.postConnect();
        } else if(connOpts != null) {
            var self = this;
            this._pendingConection = TediousConnect(normalizeConnectionOptions(connOpts)).then(function(conn){
                self.registerDebugPool(conn);
                self._client = conn;
                return conn;
            });
        } else {
            throw new Error("Client.contructor with null, null")
        }
    }
    async connect(){
        if(this.fromPool){
            throw new Error(messages.mustNotConnectClientFromPool)
        }
        if(arguments.length){
            throw new Error(messages.clientConenctMustNotReceiveParams);
        }
        /* istanbul ignore next */
        if(!this._client && !this._pendingConection){
            throw new Error(messages.lackOfClient);
        }
        this._client = await this._pendingConection;
        this.postConnect();
        return this;
    };
    end(){
        /* istanbul ignore next */
        if(this.fromPool){
            throw new Error(messages.mustNotEndClientFromPool)
        }
        /* istanbul ignore else */
        if(this._client instanceof tedious.Connection){
            // this._client.close();
        }else{
            throw new Error(messages.lackOfClient);
        }
    };
    async done(){
        if(!this._client){
            throw new Error(messages.clientAlreadyDone);
        }
        if(debug.pool){
            // @ts-ignore DEBUGGING
            debug.pool[this._client.secretKey].count--;
        }
        // var clientToDone=this._client;
        // @ts-expect-error disposing:
        this._client=null;
        // return clientToDone.close();
        return ;
    }
    query(sql:string):Query
    query(sql:string, params:any[]):Query
    query(sqlObject:{text:string, values:any[]}):Query
    query():Query{
        /* istanbul ignore next */
        if(!this.connected || !this._client){
            throw new Error(messages.queryNotConnected)
        }
        this.connected.lastOperationTimestamp = new Date().getTime();
        var queryArguments = Array.prototype.slice.call(arguments);
        var queryText:string;
        var queryValues:null|any[]=null;
        if(typeof queryArguments[0] === 'string'){
            queryText = queryArguments[0];
            queryValues = queryArguments[1] = adaptParameterTypes(queryArguments[1]||null);
        }else /* istanbul ignore else */ if(queryArguments[0] instanceof Object){
            queryText = queryArguments[0].text;
            queryValues = adaptParameterTypes(queryArguments[0].values||null);
            queryArguments[0].values = queryValues;
        }
        /* istanbul ignore else */
        var uninterpolatedQuery = toUninterpolatedQuery(queryArguments[0], queryArguments[1])
        if(log){
            // @ts-ignore if no queryText, the value must be showed also
            var sql=queryText;
            log(MESSAGES_SEPARATOR, MESSAGES_SEPARATOR_TYPE);
            if(queryValues && queryValues.length){
                log('`'+sql+'\n`','QUERY-P');
                log('-- '+JSON.stringify(queryValues),'QUERY-A');
                queryValues.forEach(function(value:any, i:number){
                    sql=sql.replace(new RegExp('\\$'+(i+1)+'\\b'), 
                        // @ts-expect-error numbers and booleans can be used here also
                        typeof value == "number" || typeof value == "boolean"?value:quoteNullable(value)
                    );
                });
            }
            log(sql+';','QUERY');
        }
        return new Query(uninterpolatedQuery, this);
    };
    get informationSchema():InformationSchemaReader{
        return this._informationSchema || new InformationSchemaReader(this);
    }
    async executeSentences(sentences:string[]){
        var self = this;
        /* istanbul ignore next */
        if(!this._client || !this.connected){
            throw new Error(messages.attemptToExecuteSentencesOnNotConnected+" "+!this._client+','+!this.connected)
        }
        var cdp:Promise<ResultCommand|void> = Promise.resolve();
        sentences.forEach(function(sentence){
            cdp = cdp.then(async function(){
                if(!sentence.trim()){
                    return ;
                }
                return await self.query(sentence).execute().catch(function(err:Error){
                    throw err;
                });
            });
        });
        return cdp;
    }
    async executeSqlScript(fileName:string){
        var self=this;
        /* istanbul ignore next */
        if(!this._client || !this.connected){
            throw new Error(messages.attemptToExecuteSqlScriptOnNotConnected+" "+!this._client+','+!this.connected)
        }
        return fs.readFile(fileName,'utf-8').then(function(content){
            var sentences = content.split(/\r?\n\r?\n/);
            return self.executeSentences(sentences);
        });
    }
    async bulkInsert(params:BulkInsertParams):Promise<void>{
        var self = this;
        /* istanbul ignore next */
        if(!this._client || !this.connected){
            throw new Error(messages.attemptTobulkInsertOnNotConnected+" "+!this._client+','+!this.connected)
        }
        var sql = "INSERT INTO "+(params.schema?quoteIdent(params.schema)+'.':'')+
            quoteIdent(params.table)+" ("+
            params.columns.map(quoteIdent).join(', ')+") VALUES ("+
            params.columns.map(function(_name:string, i_name:number){ return '$'+(i_name+1); })+")";
        var i_rows=0;
        while(i_rows<params.rows.length){
            try{
                await self.query(sql, params.rows[i_rows]).execute();
            }catch(err){
                var error = unexpected(err);
                if(params.onerror){
                    await params.onerror(error, params.rows[i_rows]);
                }else{
                    if(logExceptions){
                        console.error('Context for error',{row: params.rows[i_rows]})
                    }
                    throw error;
                }
            }
            i_rows++;
        }
    }
    copyFromParseParams(opts:CopyFromOpts){
        /* istanbul ignore next */
        if(opts.done){
            console.log(messages.copyFromInlineDumpStreamOptsDoneExperimental);
        }
        /* istanbul ignore next */
        if(!this._client || !this.connected){
            throw new Error(messages.attemptTocopyFromOnNotConnected+" "+!this._client+','+!this.connected)
        }
        var from = opts.inStream ? 'STDIN' : quoteLiteral(opts.filename);
        var sql = `COPY ${opts.table} ${opts.columns?`(${opts.columns.map(name=>quoteIdent(name)).join(',')})`:''} FROM ${from} ${opts.with?'WITH '+opts.with:''}`;
        return {sql, _client:this._client};
    }
    async copyFromFile(opts:CopyFromOptsFile):Promise<ResultCommand>{
        var {sql} = this.copyFromParseParams(opts);
        return this.query(sql).execute();
    }
    copyFromInlineDumpStream(_opts:CopyFromOptsStream){
        throw new Error('not implemented')
    }
    formatNullableToInlineDump(nullable:any){
        if(nullable==null){
            return '\\N'
        }else if(typeof nullable === "number" && isNaN(nullable)){
            return '\\N'
        }else{
            return nullable.toString().replace(/(\r)|(\n)|(\t)|(\\)/g, 
                function(_all:string,bsr:string,bsn:string,bst:string,bs:string){
                    if(bsr) return '\\r';
                    if(bsn) return '\\n';
                    if(bst) return '\\t';
                    /* istanbul ignore else por la regexp es imposible que pase al else */
                    if(bs) return '\\\\';
                    /* istanbul ignore next Esto es imposible que suceda */
                    if(logExceptions){
                        console.error('Context for error',{_all})
                    }
                    throw new Error(messages.formatNullableToInlineDumpErrorParsing)
                }
            );
        }
    }
    copyFromArrayStream(opts:CopyFromOptsStream){
        var c = this;
        var transform = new Transform({
            writableObjectMode:true,
            readableObjectMode:true,
            transform(arrayChunk:any[], _encoding, next){
                this.push(arrayChunk.map(x=>c.formatNullableToInlineDump(x)).join('\t')+'\n')
                next();
            },
            flush(next){
                this.push('\\.\n');
                next();
            }
        });
        var {inStream, ...rest} = opts;
        inStream.pipe(transform);
        return this.copyFromInlineDumpStream({inStream:transform, ...rest})
    }
}

export interface Result{
    rowCount:number
    // fields:typeof queryResult.fields
}
export interface ResultCommand{
    command?:string, rowCount:number
}
export interface ResultOneRow extends Result{
    row:{[key:string]:any}
}
export interface ResultOneRowIfExists extends Result{
    row?:{[key:string]:any}|null
}
export interface ResultRows extends Result{
    rows:{[key:string]:any}[]
}
export interface ResultValue extends Result{
    value:any
}
export type ResultGeneric = ResultValue|ResultRows|ResultOneRowIfExists|ResultOneRow|Result|ResultCommand

type Notice = string;

function logErrorIfNeeded<T>(err:Error, code?:T):Error{
    if(code != null){
        // @ts-ignore EXTENDED ERROR
        err.code=code;
    }
    /* istanbul ignore else */
    if(log){
        // @ts-ignore EXTENDED ERROR
        log('--ERROR! '+err.code+', '+err.message, 'ERROR');
    }
    return err;
}

function obtains(message:string, count:number):string{
    return message.replace('$1',
        count?messages.obtains1.replace('$1',count.toString()):messages.obtainsNone
    );
} 


type ResultBuilder<T> = T & {
    addRow: (row:T) => void
}

class Query{
    constructor(private uninterpolatedQuery:string, public client:Client/*, private _internalClient:mssql.ConnectionPool*/){
    }
    onNotice(_callbackNoticeConsumer:(notice:Notice)=>void):Query{
        return this;
    };
    private async _execute<TR extends ResultGeneric>(
        adapterCallback:null|((result:ResultBuilder<any>, resolve:(result:TR)=>void, reject:(err:Error)=>void)=>void),
        callbackForEachRow?:(row:{}, result:ResultBuilder<any>)=>Promise<void>, 
    ):Promise<TR>{
        var q = this;
        return new Promise<TR>(function(resolve, reject){
            var pendingRows = 0;
            var rows = [] as Record<string, any>[];
            var request = new tedious.Request(q.uninterpolatedQuery, function(err, rowCount){
                if (err) { 
                    reject(err)
                } else {
                    endMark={result:{rowCount: rowCount ?? rows.length, rows}};
                    whenEnd();
                }
            });
            var endMark:null|{result:ResultGeneric}=null;
            request.on('error',function(err){
                reject(err);
            });
            var addRow = (row:TR)=>rows.push(row);
            request.on('row',async function(row:Record<string,{value:any}>){
                var valueRow = likeAr(row).map(c => c.value).plain();
                if(callbackForEachRow){
                    pendingRows++;
                    /* istanbul ignore else */
                    if(log && alsoLogRows){
                        log('-- '+JSON.stringify(valueRow), 'ROW');
                    }
                    await callbackForEachRow(valueRow, {addRow});
                    --pendingRows;
                    whenEnd();
                }else{
                    // @ts-ignore
                    addRow(valueRow);
                }
            });
            function whenEnd(){
                if(endMark && !pendingRows){
                    if(adapterCallback){
                        adapterCallback(endMark.result, resolve, reject);
                    }else{
                        resolve(endMark.result as unknown as TR);
                    }
                }
            }
            var sqlConnection:tedious.Connection = q.client
                // @ts-ignore
                ._client!;
            sqlConnection.execSql(request);
        }).catch(function(err){
            throw logErrorIfNeeded(err);
        });
    };
    async fetchUniqueValue(errorMessage?:string):Promise<ResultValue>  { 
        const {row, ...result} = await this.fetchUniqueRow();
        let count = 0;
        for(var columnName in row){
            var value = row[columnName];
            count++;
        }
        if(count!==1){
            throw logErrorIfNeeded(
                new Error(obtains(errorMessage||messages.queryExpectsOneFieldAnd1, count)),
                '54U11!'
            );
        }
        return {value, ...result};
    }
    fetchUniqueRow(errorMessage?:string,acceptNoRows?:boolean):Promise<ResultOneRow> { 
        return this._execute(function(result:ResultBuilder<any>, resolve:(result:ResultOneRow)=>void, reject:(err:Error)=>void):void{
            var {rowCount} = result
            if(rowCount!==1 && (!acceptNoRows || !!rowCount)){
                var err = new Error(obtains(errorMessage||messages.queryExpectsOneRowAnd1, rowCount));
                //@ts-ignore err.code
                err.code = '54011!'
                reject(err);
            }else{
                resolve({row:result.rows[0] as any[], rowCount});
            }
        });
    }
    fetchOneRowIfExists(errorMessage?:string):Promise<ResultOneRow> { 
        return this.fetchUniqueRow(errorMessage,true);
    }
    fetchAll():Promise<ResultRows>{
        return this._execute(function(result:ResultBuilder<any>, resolve:(result:ResultRows)=>void, _reject:(err:Error)=>void):void{
            resolve(result);
        });
    }
    execute():Promise<ResultCommand>{ 
        return this._execute(function(result:ResultBuilder<any>, resolve:(result:ResultCommand)=>void, _reject:(err:Error)=>void):void{
            resolve(result);
        });
    }
    async fetchRowByRow(cb:(row:{}, result:ResultBuilder<any>)=>Promise<void>):Promise<void>{ 
        if(!(cb instanceof Function)){
            var err=new Error(messages.fetchRowByRowMustReceiveCallback);
            // @ts-ignore EXTENDED ERROR
            err.code='39004!';
            return Promise.reject(err);
        }
        await this._execute(null, cb);
    }
    async onRow(cb:(row:{}, result:ResultBuilder<any>)=>Promise<void>):Promise<void>{ 
        return this.fetchRowByRow(cb);
    }
    then(){
        throw new Error(messages.queryMustNotBeThened)
    }
    catch(){
        throw new Error(messages.queryMustNotBeCatched)
    }
};

export var allTypes=false;

export function setAllTypes(){
};

var pools:{
    [key:string]:tedious.Connection
} = {}

async function TediousConnect(params:tedious.ConnectionConfiguration):Promise<tedious.Connection>{
    return new Promise(function(resolve, reject){
        var conn = tedious.connect(params, function(err){
            if (err) {
                reject(err);
            } else {
                resolve(conn);
            }
        })
    })
}

export async function connect(connectParameters:ConnectParams):Promise<Client>{
    /* istanbul ignore else */
    if(allTypes){
        setAllTypes();
    }
    var idConnectParameters = JSON.stringify(connectParameters);
    var pool = pools[idConnectParameters] || await TediousConnect(normalizeConnectionOptions(connectParameters));
    pools[idConnectParameters] = pool;
    return new Client(null, pool);
};

export var readyLog = Promise.resolve();

/* xxistanbul ignore next */
export function logLastError(message:string, messageType:string):void{
    /* istanbul ignore else */
    if(messageType){
        if(messageType=='ERROR'){
            /* istanbul ignore else */
            if(logLastError.inFileName){
                var lines=['PG-ERROR '+message];
                /*jshint forin:false */
                for(var attr in logLastError.receivedMessages){
                    lines.push("------- "+attr+":\n"+logLastError.receivedMessages[attr]);
                }
                /*jshint forin:true */
                /*eslint guard-for-in: 0*/
                readyLog = readyLog.then(_=>fs.writeFile(logLastError.inFileName,lines.join('\n')));
            }else{
                /*jshint forin:false */
                for(var attr2 in logLastError.receivedMessages){
                    /* istanbul ignore next */
                    console.log(attr2, logLastError.receivedMessages[attr2]);
                }
                /*jshint forin:true */
                /*eslint guard-for-in: 0*/
            }
            logLastError.receivedMessages = {};
        }else{
            if(messageType==MESSAGES_SEPARATOR_TYPE){
                logLastError.receivedMessages = {};
            }
            logLastError.receivedMessages[messageType] = message;
        }
    }
}

logLastError.inFileName = './local-sql-error.log';
logLastError.receivedMessages={} as {
    [key:string]:string
};

export function poolBalanceControl(){
    var rta:string[]=[];
    if(typeof debug.pool === "object"){
        likeAr(debug.pool).forEach(function(pool){
            if(pool.count){
                rta.push(messages.unbalancedConnection+' '+util.inspect(pool));
            }
        });
    }
    return rta.join('\n');
};

/* istanbul ignore next */
process.on('exit',function(){
    console.warn(poolBalanceControl());
});
