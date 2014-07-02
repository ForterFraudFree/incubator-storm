/**
 * Simple Bolt example - receives sentence and breaks it into words.
 */

var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function SplitSentenceBolt() {
    BasicBolt.call(this);
};

SplitSentenceBolt.prototype = Object.create(BasicBolt.prototype);
SplitSentenceBolt.prototype.constructor = SplitSentenceBolt;

SplitSentenceBolt.prototype.process = function(tup, callback) {
        var self = this;
        var words = tup.values[0].split(" ");
        words.forEach(function(word) {
            self.emit([word], null, null, null, function(taskIds) {
                storm.logToFile('Task id - ' + JSON.stringify(taskIds) + ' work - ' + word);
            });
        });
        callback();
}

new SplitSentenceBolt().run();