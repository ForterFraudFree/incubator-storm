/**
 * Example for async bolt. Receives sentence and breaks it into words.
 *
 * Created by anya on 6/30/14.
 */


var storm = require('./storm');
var BasicBolt = storm.BasicBolt;

function SplitSentenceBolt() {
    BasicBolt.call(this);
};

SplitSentenceBolt.prototype = Object.create(BasicBolt.prototype);
SplitSentenceBolt.prototype.constructor = SplitSentenceBolt;

SplitSentenceBolt.prototype.process = function(tup, done) {
    var self = this;

    // Here setTimeout is not really needed, we use it to demonstrate asynchronous code in the process method:
    setTimeout(function() {
        var words = tup.values[0].split(" ");
        words.forEach(function(word) {
            self.emit([word], null, null, null, function(taskId) {
                self.log(word + 'Sent to task ids - ' + taskIds);
            });
        });
        done();
    }, 5000)
}

new SplitSentenceBolt().run();