var MiniTools = require('mini-tools');

var bufferConnectParams = null;

async function getConnectParams(){
    if(!bufferConnectParams){
        bufferConnectParams = (await MiniTools.readConfig([
            {db:{
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
            }}, 
            'local-config'
        ],{whenNotExist:'ignore'})).db;
    }
    return bufferConnectParams;
}

module.exports = {
    getConnectParams
};