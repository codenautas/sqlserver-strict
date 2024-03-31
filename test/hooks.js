var sqlserver = require('../dist/lib/sqlserver-strict');

exports.mochaHooks = {
    async afterAll() {
        console.log('HHHHHHHHHHHHHHHHHHHHH');
        sqlserver.shutdown();
    }
};