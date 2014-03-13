


    var PostgresConnection = require('./')
        , log = require('ee-log');



    var connection = new PostgresConnection({
          host      : '10.0.100.1'
        , username  : 'postgres'
        , password  : ''
        , port      : 5432
        , database  : 'information_schema'
    });



    connection.describe(function(err, description){
        log(err, description);
    });
