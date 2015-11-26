(function() {
    'use strict';
    
    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , assert        = require('assert')
        , QueryContext  = require('related-query-context')
        , TestConfig    = require('test-config')
        ;



    var   PostgresConnection = require('../')
        , idleCount = 0
        , busyCount = 0
        , connection
        , config = new TestConfig('test-config.js', {
              database  : 'test'
            , host      : 'localhost'
            , username  : 'postgres'
            , password  : ''
            , port      : 5432
        })
        ;






   describe('The Connection', function() {

        it('should emit the idle event if the conenction could be established', function(done) {
            connection = new PostgresConnection(config, 1);

            var i = 0, doneCall = () => {
                if (++i === 2) done();
            }

            connection.connect().then(doneCall).catch(done);

            connection.on('idle', function() {idleCount++;});
            connection.on('busy', function() {busyCount++;});

            connection.once('idle', doneCall);
        });


        it('should accept raw sql queries', function(done) {
            connection.query(new QueryContext({sql: 'select 1;', mode: 'query'})).then(data => {
                assert(data);
                done();
            }).catch(done);
        });


        it('should not accept non SQL queries', function(done) {
            connection.query(new QueryContext({query: {
                  select: ['*']
                , from: 'event'
                , database: 'related_test_postgres'
            }, mode: 'select'})).then(() => {
                done(new Error('Expected the query to fail!'))
            }).catch((err) => {
                assert(err instanceof Error);
                done();
            });
        });


        


        it('should create a transaction', function(done) {
            connection.createTransaction().then(() => {
                done();
            }).catch(done);
        });


        it('should commit a transaction', function(done) {
            connection.commit().then(() => {
                done();
            }).catch(done);
        });


        it('should not accept any queries anymore', function(done) {
            connection.query(new QueryContext({sql: 'select 1;', mode: 'select'})).then(() => {done()}).catch((err) => {
                assert(err instanceof Error);
                done();
            });
        });


        it('should have emitted the correct amount of events', function() {
            assert(idleCount === 2);
            assert(busyCount === 2);
        });


        it('should return the correct config object', function() {
            assert(typeof connection.getConfig() === 'object');
        });




        it('should kill itself if asked to do so', function(done) {
            let connection = new PostgresConnection(config, 1);

            connection.connect();

            connection.on('end', done);

            connection.kill();
        });
    });
})();
    