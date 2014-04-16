!function(){

    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , pg            = require('pg')
        , type          = require('ee-types')
        , async         = require('ee-async')
        , argv          = require('ee-argv')
        , moment        = require('moment')
        , pluralize     = require('pluralize')
        , Connection    = require('ee-db-connection') //*/require('../../ee-db-connection')
        , QueryBuilder  = require('ee-query-builder') //*/require('../../ee-query-builder');


    var debug = argv.has('debug-sql');



    module.exports = new Class({
        inherits: Connection


        , _bleedReg: /transaction|declare|set|delimiter|execute/gi
        , _writeReg: /create|insert|delete|update|alter|flush|drop|truncate|call|DELIMITER|execute|DEALLOCATE/gi


        /**
         * class constructor
         *
         * @param <Object> connection options
         */
        , init: function init(options) {
            init.parent(options);

            this._querBuilder = new QueryBuilder({
                  escapeId: this._escapeId.bind(this)
                , escape:   this._escape.bind(this)
            });
        }


        /**
         * the _connect() method creates the database connection
         *
         * @param <Function> done callback
         */
        , _connect: function(done){
            this.connection = new pg.Client({
                  user        : this.options.username
                , password    : this.options.password
                , host        : this.options.host
                , port        : this.options.port
                , database    : this.options.database
            });


            // connect
            this.connection.connect(function(err){
                if (err) this._handleConnectionError(err);
                done(err);
            }.bind(this));
        }



        , _render: function(){
            return this._querBuilder._render.apply(this._querBuilder, Array.prototype.slice.call(arguments));
        }


        , _toString: function(){
            return this._querBuilder._toString.apply(this._toString, Array.prototype.slice.call(arguments));
        }


        , _toType: function(){
            return this._querBuilder._toType.apply(this._toType, Array.prototype.slice.call(arguments));
        }



        /**
         * the _canBleed() securely checks if the sql contains statements which
         * can bleed into the next queries. if yes the conenction must be 
         * terminated after the query was executed.
         *
         * @param <String> input
         */
        , _canBleed: function(input){
            this._bleedReg.lastIndex = 0;
            return this._bleedReg.test(input);          
        }



        /**
         * the _escape() securely escapes values preventing sql injection
         *
         * @param <String> input
         */
        , _escape: function(input){
            return this.connection.escapeLiteral(input);
        }


        /**
         * the _escapeId() method escapes a name so it doesnt collide with
         * reserved keywords
         *
         * @param <String> input
         */
        , _escapeId: function(input){
            return this.connection.escapeIdentifier(input);
        }


        /**
         * the _query() method send a query to the rdbms
         *
         * @param <String> sql
         * @param <Function> callback
         */
        , _query: function(sql, callback) {
            var start;

            if (debug) {
                log(sql);
                start = Date.now();
            }

            this.connection.query(sql, function(err, results){
                if (debug){
                    log('query took «'+(Date.now()-start)+'» msec ...');
                    log('query returned «'+(results && results.rows ? results.rows.length : 0 )+'» rows ...');
                }

                if (err) callback(err);
                else {
                    if (type.object(results)) {
                        // not an select
                        if (results.insertId){
                            // insert
                            callback(null, {
                                  type: 'id'
                                , id: results.insertId
                            });
                        }
                        if (type.array(results.rows)) {
                            callback(null, results.rows);
                        }
                        else callback(null, results);
                    }
                    else {
                        log(results);
                        throw new Error('unexpected return value from pg driver!');
                    }
                }
            });
        }
        

        /**
         * the _describe() method returns a detailed description of all 
         * databases, tables and attributes
         *
         * @param <Function> callback
         */
        , _describe: function(databases, callback){

            // get definition for each database
            async.each(databases, function(databaseName, next){
                // get relations
                async.wait(function(done){
                    this.listContraints(databaseName, done);
                }.bind(this)

                // get table definitions
                , function(done){
                    this.describeTables(databaseName, done);
                }.bind(this)

                // clean up results
                , function(err, results){
                    if(err) callback(err);
                    else { 
                        next(null, {
                              databaseName: databaseName
                            , constraints:  results[0]
                            , tables:       results[1]
                        });
                    }
                }.bind(this));
            }.bind(this)

            // reformat definitions
            , function(err, definitions) {
                if (err) callback(err);
                else {
                    var dbs = {};

                    definitions.forEach(function(db){
                        var database;

                        if (!dbs[db.databaseName]) {
                            dbs[db.databaseName] = {};
                            Object.defineProperty(dbs[db.databaseName], 'getDatabaseName', {
                                value: function(){return db.databaseName;}
                            });
                        }
                        database = dbs[db.databaseName];
                        

                        // map tables
                        db.tables.forEach(function(definition){
                            var table;

                            if (!database[definition.table_name]) {
                                database[definition.table_name] = {
                                      name          : definition.table_name
                                    , primaryKeys   : []
                                    , isMapping     : false
                                    , columns       : {}
                                };

                                Object.defineProperty(database[definition.table_name], 'getTableName', {
                                    value: function(){return definition.table_name;}
                                });
                                Object.defineProperty(database[definition.table_name], 'getDatabaseName', {
                                    value: function(){return db.databaseName;}
                                });
                            }
                            table = database[definition.table_name];
                            
                            table.columns[definition.column_name] = {
                                  name:         definition.column_name
                                , type:         definition.data_type
                                , length:       definition.character_maximum_length || definition.numeric_precision
                                , nullable:     definition.is_nullable === 'YES'
                                , isPrimary:    false
                                , isUnique:     false
                                , isForeignKey: false
                                , isReferenced: false
                                , mapsTo:       []
                                , belongsTo:    []
                            };
                        }.bind(this));

                        // log(database);

                        // map constraints
                        Object.keys(db.constraints).forEach(function(tableName){

                            // gather info
                            Object.keys(db.constraints[tableName]).forEach(function(constraintName){
                                var   constraint = db.constraints[tableName][constraintName];


                                constraint.rules.forEach(function(rule){
                                    switch (constraint.type) {
                                        case 'primary key':
                                            database[tableName].columns[rule.column_name].isPrimary = true;
                                            database[tableName].primaryKeys.push(rule.column_name);
                                            break;

                                        case 'unique':
                                            database[tableName].columns[rule.column_name].isUnique = true;
                                            break;

                                        case 'foreign key':
                                            database[tableName].columns[rule.column_name].isForeignKey = true;
                                            database[tableName].columns[rule.column_name].referencedTable = rule.referenced_table_name;
                                            database[tableName].columns[rule.column_name].referencedColumn = rule.referenced_column_name;
                                            database[tableName].columns[rule.column_name].referencedModel = database[rule.referenced_table_name];

                                            // tell the other side its referenced
                                            database[rule.referenced_table_name].columns[rule.referenced_column_name].belongsTo.push({
                                                  targetColumn: rule.column_name
                                                , name: tableName
                                                , model: database[tableName]
                                            });
                                            database[rule.referenced_table_name].columns[rule.referenced_column_name].isReferenced = true;
                                            break;
                                    }
                                });
                            }.bind(this));


                            Object.keys(db.constraints[tableName]).forEach(function(constraintName){
                                var   constraint = db.constraints[tableName][constraintName];

                                // check for mapping table
                                // a rule must have two memebers and may be of type primary 
                                // or unique. if this rule has fks on both column we got a mapping table
                                if (constraint.rules.length === 2 && (constraint.type === 'primary key' || constraint.type === 'unique')){
                                    var columns = constraint.rules.map(function(rule){ return rule.column_name; });

                                    // serach for fks on both columns, go through all rules on the table, look for a fk constraint
                                    if (Object.keys(db.constraints[tableName]).filter(function(checkContraintName){
                                                var checkConstraint = db.constraints[tableName][checkContraintName];

                                                return checkConstraint.type === 'foreign key' && (checkConstraint.rules.filter(function(checkRule){
                                                    return columns.indexOf(checkRule.column_name) >= 0;
                                                })).length === 1;
                                            }).length === 2){

                                        database[tableName].isMapping = true;
                                        database[tableName].mappingColumns = columns;

                                        // set mapping reference on tables
                                        var   modelA = database[tableName].columns[columns[0]].referencedModel
                                            , modelB = database[tableName].columns[columns[1]].referencedModel;

                                        modelA.columns[database[tableName].columns[columns[0]].referencedColumn].mapsTo.push({
                                              model         : modelB
                                            , column        : modelB.columns[database[tableName].columns[columns[1]].referencedColumn]
                                            , name          : pluralize.plural(modelB.name)
                                            , via: {
                                                  model     : database[tableName]
                                                , fk        : columns[0]
                                                , otherFk   : columns[1]
                                            }
                                        });

                                        // don't add mappings to myself twice
                                        if (modelB !== modelA) {
                                            modelB.columns[database[tableName].columns[columns[1]].referencedColumn].mapsTo.push({
                                                  model         : modelA
                                                , column        : modelA.columns[database[tableName].columns[columns[0]].referencedColumn]
                                                , name          : pluralize.plural(modelA.name)
                                                , via: {
                                                      model     : database[tableName]
                                                    , fk        : columns[1]
                                                    , otherFk   : columns[0]
                                                }
                                            });
                                        }
                                    }
                                }
                            }.bind(this));
                        }.bind(this));
                    }.bind(this));
                                                
                    callback(null, dbs);
                }
            }.bind(this));
        }

        /*

SELECT tc.constraint_name, tc.table_name, kcu.column_name, ccu.table_name AS referenced_table_name, ccu.column_name AS referenced_column_name FROM information_schema.table_constraints AS tc JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name WHERE constraint_type = 'FOREIGN KEY' AND tc.constraint_schema='eventbooster';
*/

        , listContraints: function(databaseName, callback){
            async.wait(function(done){
                // from https://code.google.com/p/pgutils/source/browse/trunk/sql/fk.sql
                this.queryRaw('SELECT tc.table_name , kcu.column_name , tc.constraint_type , tc.constraint_name , fks.referenced_table_name , fks.referenced_column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name LEFT JOIN ( SELECT fkr.relname AS table_name , fka.attname AS column_name , pkr.relname AS referenced_table_name , pka.attname AS referenced_column_name , c.conname AS constraint_name , c.contype FROM pg_constraint c JOIN pg_namespace cn ON cn.oid = c.connamespace JOIN pg_class fkr ON fkr.oid = c.conrelid JOIN pg_namespace fkn ON fkn.oid = fkr.relnamespace JOIN pg_attribute fka ON fka.attrelid = c.conrelid AND fka.attnum = ANY (c.conkey) JOIN pg_class pkr ON pkr.oid = c.confrelid JOIN pg_namespace pkn ON pkn.oid = pkr.relnamespace JOIN pg_attribute pka ON pka.attrelid = c.confrelid AND pka.attnum = ANY (c.confkey) WHERE (c.contype = \'f\'::"char" ) AND pkn.nspname = ?databaseName ) as fks ON fks.table_name = tc.table_name AND fks.column_name = kcu.column_name AND fks.constraint_name = kcu.constraint_name WHERE tc.constraint_schema = ?databaseName AND kcu.constraint_schema = ?databaseName ORDER BY tc.table_name, tc.constraint_name, tc.constraint_type;', {databaseName: databaseName}, done);
               
            }.bind(this)


            , function(err, results){
                if (err) callback(err);
                else {
                    var constraints = {}, tables = {};

                    // join the separate results
                    results[0].forEach(function(constraint){
                        if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
                        if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: [], type: 'unknown'};

                        constraints[constraint.table_name][constraint.constraint_name].rules.push(constraint);
                        constraints[constraint.table_name][constraint.constraint_name].type = constraint.constraint_type.toLowerCase();
                    });

                    callback(null, constraints);
                }
            }.bind(this));
        }


        , describeTables: function(databaseName, callback){
            this.query({
                filter: {
                    table_schema: databaseName
                }
                , database: 'information_schema'
                , from:     'columns'
                , select: ['table_schema', 'table_name', 'column_name', 'column_default', 'is_nullable', 'data_type', 'character_maximum_length', 'numeric_precision']
            }, callback);
        }


        , listTables: function(databaseName, callback){

            this._query('SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = '+databaseName+' ORDER BY table_schema,table_name;', callback);
        }


        , listDatabases: function(callback){
            this._query('SELECT datname FROM pg_database WHERE datistemplate = false;', function(err, databases){
                if (err) callback(err);
                else {
                    databases = (databases.rows || []).filter(function(row){
                        return row.datname !== 'information_schema';
                    }).map(function(row){
                        return row.datname;
                    })

                    callback(null, databases);
                }
            }.bind(this));
        }

        

        /**
         * the _handleConnectionError() method handles connection errors
         *
         * @param <Error> error, optional
         */
        , _handleConnectionError: function(err){
            this._disconnected();
            this._end(err);
        }
    });
}();
