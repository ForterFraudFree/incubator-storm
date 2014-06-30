/**
 * Created by anya on 6/30/14.
 */


var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function SplitSentenceBolt() {
    BasicBolt.call(this);
};

SplitSentenceBolt.prototype = new BasicBolt();
SplitSentenceBolt.prototype = Object.create(BasicBolt.prototype);

SplitSentenceBolt.prototype.run = function() {
    var self = this;
    setTimeout(function() {
        self.logToFile('run');
        self.startReadingInput();
    }, 500)
}

SplitSentenceBolt.prototype.process = function(tup, callback) {
    var self = this;
    setTimeout(function() {
        var words = tup.values[0].split(" ");
        words.forEach(function(word) {
            self.emit([word], null, null, null, function(taskId) {
                storm.logToFile('Task id - ' + JSON.stringify(taskId) + ' work - ' + word);
            });
        });
        callback();
    }, 5000)
}

new SplitSentenceBolt().run();