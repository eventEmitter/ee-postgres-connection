!function(){

    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , pg            = require('pg')
        , type          = require('ee-types')
        , async         = require('ee-async')
        , argv          = require('ee-argv')
        , Connection    = require('related-db-connection')
        , QueryBuilder  = require('related-postgres-query-builder');


    var   debug         = argv.has('debug-sql') || process.env.debug_sql === true
        , debugErrors   = argv.has('debug-sql-errors')
        , debugSlow     = argv.has('debug-slow-queries')
        , slowDebugTime = debugSlow && type.string(argv.get('debug-slow-queries')) ? argv.get('debug-slow-queries') : 200;



    module.exports = new Class({
        inherits: Connection


        , _bleedReg: /transaction|declare|set|delimiter|execute/gi
        , _writeReg: /create|insert|delete|update|alter|flush|drop|truncate|call|DELIMITER|execute|DEALLOCATE/gi


        /*
         * LOCK_READ:        NOT IMPLEMENTED
         * LOCK_WRITE:       SHARE ROW EXCLUSIVE -> This mode protects a table against concurrent data changes, only one session can hold it at a time.
         * LOCK_EXCLUSIVE:   ACCESS EXCLUSIVE -> This mode guarantees that the holder is the only transaction accessing the table in any way
         */
        , _lockModes: {
              LOCK_WRITE:       'SHARE ROW EXCLUSIVE'
            , LOCK_EXCLUSIVE:   'ACCESS EXCLUSIVE'
        }



        // extract query parameters form a string
        , _paramterizeReg: /\?([a-z0-9_-]+)/gi


        /**
         * class constructor
         *
         * @param <Object> connection options
         */
        , init: function init(options, id) {
            init.super.call(this, options, id);

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
            this.connection.connect(done);


            // remove dead connections from the pool
            this.connection.on('error', function(err) {
                this._end();
            }.bind(this));

            // close the conenction if the end event is emitted
            this.on('end', function() {
                if (this.connection) {
                    this.connection.end();
                    this.connection = null;
                }
            }.bind(this));
        }




        , _toString: function(){
            return this._querBuilder._toString.apply(this._toString, Array.prototype.slice.call(arguments));
        }


        , _toType: function(){
            return this._querBuilder._toType.apply(this._toType, Array.prototype.slice.call(arguments));
        }



        /*
         * st a lock on a tblae
         */
        , lock: function(schema, table, lockType, callback) {
            if (!this._lockModes[lockType]) throw new Error('Invalid or not supported lock type «'+lockType+'»!');

            this._query({SQL: 'LOCK TABLE '+(schema? this._escapeId(schema)+'.': '')+this._escapeId(table)+' IN '+this._lockModes[lockType]+' MODE;', callback: callback});
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
            return this.connection.escapeLiteral(input+'');
        }


        /**
         * the _escapeId() method escapes a name so it doesnt collide with
         * reserved keywords
         *
         * @param <String> input
         */
        , _escapeId: function(input){
            var oldLimit;

            if (!type.string(input) || !input.length) {
                oldLimit = Error.stackTraceLimit;
                Error.stackTraceLimit = Infinity;
                throw new Error('Cannot escape id «'+input+'»!');
                Error.stackTraceLimit = oldLimit;
            }
            return this.connection.escapeIdentifier(input);
        }




        /*
         * bring the query into the correcto format
         *
         * @param <String> SQL
         * @param <Mixed> object, array, null, undefined query parameters
         */
         , _paramterizeQuery: function(configuration) {
            var match;

            // ew're recycling the regex obejct
            this._paramterizeReg.lastIndex = 0;

            // values array for the parameters
            configuration.values = [];

            // get a list of parameters from the string
            while (match = this._paramterizeReg.exec(configuration.SQL)) {
                // add value
                configuration.values.push(configuration.parameters[match[1]]);

                // replace inside sql string
                configuration.SQL = configuration.SQL.replace(match[0], '$'+configuration.values.length);

                // move the index to the correct location
                this._paramterizeReg.lastIndex += ('$'+configuration.values.length).length-match[0].length;
            }
        }




        /**
         * the _query() method send a query to the rdbms
         *
         * @param <Object> query configuration
         */
        , _query: function(configuration) {
            var _this = this, start, oldLimit;

            if (debug || debugSlow || configuration.debug) start = Date.now();

            if (debugErrors || debug) {
                oldLimit = Error.stackTraceLimit;
                Error.stackTraceLimit = Infinity;
                configuration.stack = new Error('stacktrace');
                Error.stackTraceLimit = oldLimit;
            }

            // call the pg driver
            this.connection.query(configuration.SQL, configuration.values, function(err, data) {
                // we've goit th epq query context which is useful
                // for debuggin queries
                _this._queryCallback(this, err, data, start, configuration);
            });
        }
        


        /**
         * callback called by the query function
         * 
         * @param <Object> the postgress driver context
         * @param <Error> optional error object
         * @param <Object> pg results object
         * @param <Number> optional timestamp when the query was executed
         * @param <Object> the query configuration
         */
        , _queryCallback: function(pgQueryContext, err, data, start, configuration) {
            var time, logStr;


            // debug logging
            if (debug || configuration.debug || (debugSlow && (Date.now()-start) > slowDebugTime) || (debugErrors && err)) {
                // capture query time
                time = Date.now()-start;
                logStr = '[POSTGRES]['+this.id+'] ';

                // banner
                log.debug(logStr+this._createDebugBanner(debug || configuration.debug ? 'QUERY DEBUGGER' : 'SLOW QUERY')); 

                // status
                if (err) log.error(logStr+'The query failed: '+err);
                else log.debug(logStr+'Query returned '.grey+((data && data.rows ? data.rows.length : 0)+'').yellow+' rows'.white+' ('.grey+((Date.now()-start)+'').yellow+' msec'.white+') ...'.grey);

                // query
                log.debug(logStr+this._renderSQLQuery(pgQueryContext).white);

                // trace
                if (err && configuration.stack) {
                    log.info('Stacktrace:');
                    log(configuration.stack);
                } 

                // end banner
                log.debug(logStr+this._createDebugBanner((debug || configuration.debug ? 'QUERY DEBUGGER' : 'SLOW QUERY'), true));
            }

            if (err) {
                err.sql = this._renderSQLQuery(pgQueryContext);
            }


            // don't care if ther is no callback
            if (configuration.callback) {
                if (err) configuration.callback(err);
                else {
                    if (type.object(data)) {

                        switch (data.command) {
                            case 'SELECT':
                                configuration.callback(null, data.rows);
                                break;

                            case 'INSERT':
                                configuration.callback(null, {
                                      type: 'id'
                                    , values: data.rows && data.rows.length ? data.rows[0] : null
                                });
                                break;

                            default:
                                 configuration.callback(null, data);
                        }
                    }
                    else {
                        log(data);
                        throw new Error('unexpected return value from pg driver!');
                    }
                }
            }
        }




        /*
         * build a raw sql query from a pg context
         * 
         * @param <Object> pq query context
         *
         * @returns <String> full SQL query
         */
        , _renderSQLQuery: function(pgQueryContext) {
            var   SQL       = pgQueryContext.text || ''
                , values    = pgQueryContext.values || []
                , reg       = /\$[0-9]+/gi
                , index     = 0
                , match;

            while (match = reg.exec(SQL)) {
                if (values.length > index) {
                    SQL = SQL.replace(match[0], this._escape(values[index]));
                }

                // adjust regexp
                reg.lastIndex += this._escape(values[index]).length-match[0].length;

                index++;
            }

            return SQL;
        }



        /**
         * the _describe() method returns a detailed description of all 
         * databases, tables and attributes
         *
         * @param <Function> callback
         */
        , _describe: function(schemas, callback) { 
            // schemas, datbase = this.options.database;

            // get definition for each database
            async.each(schemas, function(databaseName, next){

                // get relations
                async.wait(function(done){
                    this.listContraints(databaseName, done);
                }.bind(this)


                // get table definitions
                , function(done){
                    this.describeTables(databaseName, done);
                }.bind(this)


                // check if the schema exists
                , function(done){
                    this.schemaExists(databaseName, done);
                }.bind(this)


                // clean up results
                , function(err, results){
                    if(err) callback(err);
                    else { 
                        next(null, {
                              databaseName: databaseName
                            , constraints:  results[0]
                            , tables:       results[1]
                            , exists:       results[2]
                        });
                    }
                }.bind(this));
            }.bind(this)

            // reformat definitions
            , function(err, definitions) { //log(definitions);
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
                            Object.defineProperty(dbs[db.databaseName], 'schemaExists', {
                                value: function(){return db.exists;}
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
                            
                            // build type object
                            table.columns[definition.column_name] = this._mapTypes(definition); 
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
                                            , name          : modelB.name
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
                                                , name          : modelA.name
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
         * translate pg type definition to standard orm type definition
         *
         * @param <Object> pg column description
         *
         * @returns <Object> standardized type object
         */
        , _mapTypes: function(pgDefinition) {
            var ormType = {};

            // column identifier
            ormType.name = pgDefinition.column_name;



            // type conversion
            switch (pgDefinition.data_type) {
                case 'integer':
                case 'bigint':
                case 'smallint':
                    ormType.type            = 'integer';
                    ormType.jsTypeMapping   = 'number';
                    ormType.bitLength       = pgDefinition.numeric_precision;
                    ormType.variableLength  = false;
                    if (type.string(pgDefinition.column_default)) {
                        if (/nextval\(.*\:\:regclass\)/gi.test(pgDefinition.column_default)) ormType.isAutoIncrementing = true;
                        else if (/[^0-9]+/gi.test(pgDefinition.column_default)) ormType.defaultValue = pgDefinition.column_default;
                        else ormType.defaultValue = parseInt(pgDefinition.column_default, 10);
                    }
                    break;

                case 'bit':
                    ormType.type            = 'bit';
                    ormType.jsTypeMapping   = 'arrayBuffer';
                    ormType.variableLength  = false;
                    ormType.bitLength       = pgDefinition.character_maximum_length;
                    break;

                case 'bit varying':
                    ormType.type            = 'bit';
                    ormType.jsTypeMapping   = 'arrayBuffer';
                    ormType.variableLength  = true;
                    ormType.maxBitLength    = pgDefinition.character_maximum_length;
                    break;

                case 'boolean':
                    ormType.type            = 'boolean';
                    ormType.jsTypeMapping   = 'boolean';
                    break;

                case 'character':
                    ormType.type            = 'string';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = false;
                    ormType.length          = pgDefinition.character_maximum_length;
                    break;

                case 'character varying':
                case 'text':
                    ormType.type            = 'string';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = true;
                    ormType.maxLength       = pgDefinition.character_maximum_length;
                    break;

                case 'date':
                    ormType.type            = 'date';
                    ormType.jsTypeMapping   = 'date';
                    ormType.variableLength  = false;
                    break;

                case 'double precision':
                    ormType.type            = 'float';
                    ormType.jsTypeMapping   = 'number';
                    ormType.variableLength  = false;
                    ormType.bitLength       = pgDefinition.numeric_precision;
                    break;

                case 'numeric':
                case 'decimal':
                    ormType.type            = 'decimal';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = false;
                    ormType.length          = pgDefinition.numeric_precision;
                    break;

                case 'real':
                    ormType.type            = 'float';
                    ormType.jsTypeMapping   = 'number';
                    ormType.variableLength  = false;
                    ormType.bitLength       = pgDefinition.numeric_precision;
                    break;

                case 'time without time zone':
                    ormType.type            = 'time';
                    ormType.withTimeZone    = false;
                    ormType.jsTypeMapping   = 'string';
                    break;

                case 'time with time zone':
                    ormType.type            = 'time';
                    ormType.withTimeZone    = true;
                    ormType.jsTypeMapping   = 'string';
                    break;

                case 'timestamp without time zone':
                    ormType.type            = 'datetime';
                    ormType.withTimeZone    = false;
                    ormType.jsTypeMapping   = 'date';
                    break;

                case 'timestamp with time zone':
                    ormType.type            = 'datetime';
                    ormType.withTimeZone    = true;
                    ormType.jsTypeMapping   = 'date';
                    break;
            }



            // is null allowed
            ormType.nullable = pgDefinition.is_nullable === 'YES';

            // autoincrementing?
            if (!ormType.isAutoIncrementing) ormType.isAutoIncrementing = false;

            // has a default value?
            if (type.undefined(ormType.defaultValue)) {
                if (type.string(pgDefinition.column_default)) ormType.defaultValue = pgDefinition.column_default;
                else ormType.defaultValue = null;
            }

            // will be set later
            ormType.isPrimary       = false;
            ormType.isUnique        = false;
            ormType.isReferenced    = false;
            ormType.isForeignKey    = false;

            // the native type, should not be used by the users, differs for every db
            ormType.nativeType = pgDefinition.data_type;

            // will be filled later
            ormType.mapsTo          = [];
            ormType.belongsTo       = [];

            return ormType;
        }




        , listContraints: function(databaseName, callback){
            async.wait(function(done){
                // from https://code.google.com/p/pgutils/source/browse/trunk/sql/fk.sql
                this.queryRaw('SELECT tc.table_name, kcu.column_name, tc.constraint_type, tc.constraint_name, (SELECT pkr.relname AS referenced_table_name FROM pg_constraint c JOIN pg_namespace cn ON cn.oid = c.connamespace JOIN pg_class fkr ON fkr.oid = c.conrelid JOIN pg_namespace fkn ON fkn.oid = fkr.relnamespace JOIN pg_attribute fka ON fka.attrelid = c.conrelid AND fka.attnum = ANY (c.conkey) JOIN pg_class pkr ON pkr.oid = c.confrelid JOIN pg_namespace pkn ON pkn.oid = pkr.relnamespace JOIN pg_attribute pka ON pka.attrelid = c.confrelid AND pka.attnum = ANY (c.confkey) WHERE (c.contype = \'f\'::"char" ) AND pkn.nspname = ?databaseName AND fkr.relname = tc.table_name AND fka.attname = kcu.column_name AND c.conname = kcu.constraint_name LIMIT 1) referenced_table_name, (SELECT pka.attname AS referenced_column_name FROM pg_constraint c JOIN pg_namespace cn ON cn.oid = c.connamespace JOIN pg_class fkr ON fkr.oid = c.conrelid JOIN pg_namespace fkn ON fkn.oid = fkr.relnamespace JOIN pg_attribute fka ON fka.attrelid = c.conrelid AND fka.attnum = ANY (c.conkey) JOIN pg_class pkr ON pkr.oid = c.confrelid JOIN pg_namespace pkn ON pkn.oid = pkr.relnamespace JOIN pg_attribute pka ON pka.attrelid = c.confrelid AND pka.attnum = ANY (c.confkey) WHERE (c.contype = \'f\'::"char" ) AND pkn.nspname = ?databaseName AND fkr.relname = tc.table_name AND fka.attname = kcu.column_name AND c.conname = kcu.constraint_name LIMIT 1) referenced_column_name FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_name = kcu.table_name WHERE tc.constraint_schema = ?databaseName AND kcu.constraint_schema = ?databaseName ORDER BY tc.table_name, tc.constraint_name, tc.constraint_type;', {databaseName: databaseName}, done);
               
            }.bind(this)


            , function(done) {
                this.queryRaw("select t.relname as table_name, a.attname as column_name, 'UNIQUE' as constraint_type, i.relname as constraint_name, null as referenced_table_name, null as referenced_column_name from pg_class t, pg_class i, pg_index ix, pg_attribute a, pg_namespace n where t.oid = ix.indrelid and i.oid = ix.indexrelid and a.attrelid = t.oid and a.attnum = ANY(ix.indkey) and t.relkind = 'r' and ix.indisunique = true and t.relnamespace = n.oid and n.nspname = ?databaseName and ix.indisprimary = false and indnatts > 1 order by t.relname, i.relname;", {databaseName: databaseName}, done);
            }.bind(this)


            , function(err, results) {
                if (err) callback(err);
                else {
                    var   constraints = {}
                        , tables = {}
                        , handledConstraints = {};

                    // join the separate results
                    results[0].forEach(function(constraint){
                        // we are loading some constraints from the index table, 
                        // make sure there are no duplicates
                        handledConstraints[constraint.constraint_name] = true;

                        if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
                        if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: [], type: 'unknown'};

                        constraints[constraint.table_name][constraint.constraint_name].rules.push(constraint);
                        constraints[constraint.table_name][constraint.constraint_name].type = constraint.constraint_type.toLowerCase();
                    });

                    // cnstraints forom uniue indeess
                    results[1].forEach(function(constraint){
                        // we are loading some constraints from the index table, 
                        // make sure there are no duplicates
                        if (!handledConstraints[constraint.constraint_name]) {
                            if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
                            if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: [], type: 'unknown'};

                            constraints[constraint.table_name][constraint.constraint_name].rules.push(constraint);
                            constraints[constraint.table_name][constraint.constraint_name].type = constraint.constraint_type.toLowerCase();
                        }
                    });

                    callback(null, constraints);
                }
            }.bind(this));
        }

        //select nspname from pg_catalog.pg_namespace;
        , schemaExists: function(schemaName, callback) {
            this.query({query: {
                filter: {
                    nspname: schemaName
                }
                , database: 'pg_catalog'
                , from:     'pg_namespace'
                , select: ['nspname']
            }, callback: function(err, records) {
                if (err) callback(err);
                else callback(null, !!records.length);
            }.bind(this)});
        }


        , describeTables: function(databaseName, callback){
            this.query({query: {
                filter: {
                    table_schema: databaseName
                }
                , database: 'information_schema'
                , from:     'columns'
                , select: ['table_schema', 'table_name', 'column_name', 'column_default', 'is_nullable', 'data_type', 'character_maximum_length', 'numeric_precision']
            }, callback: callback});
        }


        , listTables: function(databaseName, callback){

            this._query({SQL: 'SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema = '+databaseName+' ORDER BY table_schema,table_name;', callback: callback});
        }


        , listDatabases: function(callback){
            this._query({SQL: 'SELECT datname FROM pg_database WHERE datistemplate = false;', callback: function(err, databases){
                if (err) callback(err);
                else {
                    databases = (databases.rows || []).filter(function(row){
                        return row.datname !== 'information_schema';
                    }).map(function(row){
                        return row.datname;
                    })

                    callback(null, databases);
                }
            }.bind(this)});
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
