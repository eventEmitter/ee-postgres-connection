


    var PostgresConnection = require('./')
        , log = require('ee-log');



    var connection = new PostgresConnection({
          host      : '10.80.100.1'
        , username  : 'postgres'
        , password  : ''
        , port      : 5432
        , database  : 'eventbooster'
    });


    connection.on('load', function(err) {
        log(err);

        connection.describe(['eventbooster'], function(err, description) {
            log(err, description);
        });
    });

    
