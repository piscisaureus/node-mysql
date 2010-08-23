require.paths.push(__dirname+"/lib");

var fs = require('fs'), 
    Parser = require('mysql/parser'),
    sys = require('sys');

var repeat = 100;

var start = new Date();

(function benchmark(left) {
  var input = fs.createReadStream('benchmark.bin');
  var parser = new Parser();
  input.on('data', function(data) {
    parser.write(data);
  });
  input.on('end', function() {
    if (--left > 0) {
      benchmark(left);
    } else {
      var end = new Date();
      
      console.log(repeat + " repetitions took " + ((end - start) / 1000) + " s");
    }
  });
})(repeat);