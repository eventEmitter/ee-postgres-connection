(function(){

    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , pg            = require('pg')
        , RelatedError  = require('related-error')
        , type          = require('ee-types')
        , argv          = require('ee-argv')
        , Connection    = require('related-db-connection')
        , QueryBuilder  = require('related-postgres-query-builder');





    module.exports = new Class({
        inherits: Connection


        // brand name used for logging
        , brand: 'POSTGRES'

        /*
         * LOCK_READ:        NOT IMPLEMENTED
         * LOCK_WRITE:       SHARE ROW EXCLUSIVE -> This mode protects a table against concurrent data changes, only one session can hold it at a time.
         * LOCK_EXCLUSIVE:   ACCESS EXCLUSIVE -> This mode guarantees that the holder is the only transaction accessing the table in any way
         */
        , lockModes: {
              LOCK_WRITE:       'SHARE ROW EXCLUSIVE'
            , LOCK_EXCLUSIVE:   'ACCESS EXCLUSIVE'
        }



        // extract query parameters form a string
        , paramterizeReg: /\?([a-z0-9_-]+)/gi



        /**
         * class constructor
         *
         * @param <Object> connection options
         */
        , init: function init(options, id) {
            init.super.call(this, options, id);

            this.querBuilder = new QueryBuilder({
                  escapeId: this.escapeId.bind(this)
                , escape:   this.escape.bind(this)
            });
        }




        /**
         * the _connect() method creates the database connection
         *
         * @param <Function> done callback
         */
        , driverConnect: function(config, callback) {
            this.connection = new pg.Client({
                  user        : config.username
                , password    : config.password
                , host        : config.host
                , port        : config.port
                , database    : config.database
            });


            // connect
            this.connection.connect(function(err) {
                if (err) {
                    if (err.code === '28P01') err = new RelatedError.InvalidCredentialsError(err);
                    else if (err.code === 'ECONNREFUSED') err = new RelatedError.FailedToConnectError(err);
                    else if (err.code === 'ENETUNREACH') err = new RelatedError.FailedToConnectError(err);
                    else if (err.code === 'ECONNRESET') err = new RelatedError.FailedToConnectError(err);
                }

                callback(err);
            }.bind(this));


            // remove dead connections from the pool
            this.connection.on('error', function(err) {

                if (err.code === 'ECONNREFUSED') err = new RelatedError.FailedToConnectError(err);
                else if (err.code === 'ENETUNREACH') err = new RelatedError.FailedToConnectError(err);
                else if (err.code === 'ECONNRESET') err = new RelatedError.FailedToConnectError(err);

                // since the conenciton probably ended 
                // anyway we are going to kill it off
                this.connection.end();
                delete this.connection;

                // emit the error event, its used by super
                // to inddicate theat no query is running 
                // anymore
                this.emit('error', err);

                // call the super end method
                this.end(err);
            }.bind(this));
        }




        /**
         * renders sql from the internal query representation
         *
         * @param {query} query the query object
         */
        , render: function(query) {
            var result = this.querBuilder._render(query.mode, query.query);

            query.SQL = result.SQLString.trim();+';';
            query.parameters = result.parameters.get();
        }




        /**
         * ends the connection
         */
        , endConnection: function(callback) {
            this.connection.once('end', callback);
            this.connection.end();
        }





        /*
         * set a lock on a tblae
         */
        , lock: function(schema, table, lockType, callback) {
            if (!this.lockModes[lockType]) callback(Error('Invalid or not supported lock type «'+lockType+'»!'));
            else this.query({SQL: 'LOCK TABLE '+(schema? this.escapeId(schema)+'.': '')+this.escapeId(table)+' IN '+this.lockModes[lockType]+' MODE;', mode: 'lock'}, callback);
        }






        /**
         * the _escape() securely escapes values preventing sql injection
         *
         * @param <String> input
         */
        , escape: function(input) {
            return this.connection.escapeLiteral(input+'');
        }


        /**
         * the _escapeId() method escapes a name so it doesnt collide with
         * reserved keywords
         *
         * @param <String> input
         */
        , escapeId: function(input) {
            if (!type.string(input) || !input.length) throw new Error('Cannot escape id «'+input+'»!');

            return this.connection.escapeIdentifier(input);
        }




        /*
         * bring the query into the correcto format
         *
         * @param {object} query query definition
         */
         , paramterizeQuery: function(query) {
            var match;

            // we're recycling the regex obejct
            this.paramterizeReg.lastIndex = 0;

            // values array for the parameters
            query.values = [];

            // get a list of parameters from the string
            while (match = this.paramterizeReg.exec(query.SQL)) {

                // add value
                if (!query.parameters || type.undefined(query.parameters[match[1]])) throw new Error('Cannot parameterize the query, the parameter «'+match[1]+'» is not set for the query: '+query.SQL);
                query.values.push(query.parameters[match[1]]);

                // replace inside sql string
                query.SQL = query.SQL.replace(match[0], '$'+query.values.length);

                // move the index to the correct location
                this.paramterizeReg.lastIndex += ('$'+query.values.length).length-match[0].length;
            }
        }






        /**
         * the _query() method send a query to the rdbms
         *
         * @param <Object> query configuration
         */
        , executeQuery: function(query, callback) {
            
            // collect debug info
            this.debug(query);

            // call the pg driver
            this.connection.query(query.SQL, query.values, function(err, data) {
                if (err && err.code && err.code === '23505') err = new RelatedError.DuplicateKeyError(err);


                // pront debug info
                this.printDebugInfo(query, err, data && data.rows ? data.rows : null);

                // return to caller
                if (callback) {
                    if (err) callback(err);
                    else {
                        if (type.object(data)) {
                            switch (data.command) {
                                case 'SELECT':
                                    callback(null, data.rows);
                                    break;

                                case 'INSERT':
                                    callback(null, {
                                          type: 'id'
                                        , values: data.rows && data.rows.length ? data.rows[0] : null
                                    });
                                    break;

                                default:
                                     callback(null, data);
                            }
                        }
                        else {
                            log(data);
                            throw new Error('unexpected return value from pg driver!');
                        }
                    }
                }
            }.bind(this));
        }





        /*
         * build a raw sql query from a pg context
         *
         * @param <Object> pq query context
         *
         * @returns <String> full SQL query
         */
        , renderSQLQuery: function(SQL, values) {
            var   SQL       = SQL.text || ''
                , values    = values || []
                , reg       = /\$[0-9]+/gi
                , index     = 0
                , match;

            while (match = reg.exec(SQL)) {
                if (values.length > index) {
                    SQL = SQL.replace(match[0], this.escape(values[index]));
                }

                // adjust regexp
                reg.lastIndex += this.escape(values[index]).length-match[0].length;

                index++;
            }

            return SQL;
        }
    });
})();
