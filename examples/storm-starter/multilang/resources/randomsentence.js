/**
 * Example for storm spout. Emits random sentences.
 * The original class in java - storm.starter.spout.RandomSentenceSpout.
 *
 * Created by anya on 6/26/14.
 */

var storm = require('./storm');
var Spout = storm.Spout;


var SENTENCES = [
    "the cow jumped over the moon",
    "an apple a day keeps the doctor away",
    "four score and seven years ago",
    "snow white and the seven dwarfs",
    "i am at two with nature"]

function RandomSentenceSpout(sentences) {
    Spout.call(this);
    this.runningTupleId = getRandomInt(0,Math.pow(2,16));
    this.sentences = sentences;
    this.pending = {};
};

RandomSentenceSpout.prototype = Object.create(Spout.prototype);
RandomSentenceSpout.prototype.constructor = RandomSentenceSpout;

RandomSentenceSpout.prototype.getRandomSentence = function() {
    return this.sentences[getRandomInt(0, this.sentences.length - 1)];
}

RandomSentenceSpout.prototype.nextTuple = function(done) {
    var sentence = this.getRandomSentence();
    var tup = [sentence];
    var id = this.runningTupleId;
    this.pending[id] = tup;
    this.emit(tup, null, id, null);
    this.runningTupleId++;
    done();
}

RandomSentenceSpout.prototype.ack = function(id, done) {
    this.log('Received ack for - ' + id);
    delete this.pending[id];
    done();
}

RandomSentenceSpout.prototype.fail = function(id, done) {
    this.log('Received fail for - ' + id);
    this.emit(this.pending[id], null, id, null);
    done();
}

/**
 * Returns a random integer between min (inclusive) and max (inclusive)
 */
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

new RandomSentenceSpout(SENTENCES).run();
