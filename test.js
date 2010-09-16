/*
  Test with the table from test.sql
*/

require.paths.push(__dirname+'/lib');

var Client = require('mysql').Client,
    client = new Client();

client.user = 'root';
client.password = null;
client.database = 'test';
  
client.connect(function(error) { 
  if (error) throw error;
});
    
client.on('error', function(error) {
  throw error;
});
  
var tests = 10000, 
    finished = 0,
    good = {};

for (var i = 0; i < tests; i++) {
  var query = client.query("SHOW COLUMNS FROM test");
  
  query.on('row', function(row) {
    if (!row.Type) {
      console.log("Baaaad:");
      console.log(row);
      console.log("Good would have been:");
      console.log(good[row.Field]);
    } else {
      good[row.Field] = row;
    }
  });
  
  query.on('end', function() {
    if (++finished === tests) {
      process.exit();
    }
  });
}