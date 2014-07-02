/**
 * Base classes in node-js for storm Bolt and Spout.
 */

var fs = require('fs');

function logToFile(msg) {

    fs.appendFileSync('/Users/anya/tmp/storm/log', msg + '\n\n\n');
}

function Storm() {
    this.lines = [];
    this.taskIdsCallbacks = [];
    this.isFirstMessage = true;
}

Storm.prototype.logToFile = function(msg) {
    logToFile(this.name + ':\n' + msg);
}

Storm.prototype.sendMsgToParent = function(msg) {
    var str = JSON.stringify(msg) + '\nend\n';
    process.stdout.write(str);
}

Storm.prototype.sync = function() {
    this.sendMsgToParent({'command':'sync'});
}

Storm.prototype.sendpid = function(heartbeatdir) {
    var pid = process.pid;
    this.sendMsgToParent({'pid':pid})
    fs.closeSync(fs.openSync(heartbeatdir + "/" + pid, "w"));
}

Storm.prototype.log = function(msg) {
    this.sendMsgToParent({"command": "log", "msg": msg});
}

Storm.prototype.initSetupInfo = function(setupInfo) {
    var self = this;
    var callback = function() {
        self.sendpid(setupInfo['pidDir']);
    }
    this.initialize(setupInfo['conf'], setupInfo['context'], callback);
}

Storm.prototype.startReadingInput = function() {
    var self = this;
    this.logToFile('Start reading input from stdin.');

    process.stdin.on('readable', function() {
        var chunk = process.stdin.read();

        if (!!chunk && chunk.length !== 0) {
          var lines = chunk.toString().split('\n');
          lines.forEach(function(line) {
              self.handleNewLine(line);
          })
        }
    });
}

Storm.prototype.handleNewLine = function(line) {
    if (line === 'end') {
        var msg = this.collectMessageLines();
        this.cleanLines();
        this.handleNewMessage(msg);
    } else {
        this.storeLine(line);
    }
}

Storm.prototype.collectMessageLines = function() {
    return this.lines.join('\n');
}

Storm.prototype.cleanLines = function() {
    this.lines = [];
}

Storm.prototype.storeLine = function(line) {
    this.lines.push(line);
}

Storm.prototype.isTaskIds = function(msg) {
    return (msg instanceof Array);
}

Storm.prototype.handleNewMessage = function(msg) {
    var parsedMsg = JSON.parse(msg);

    if (this.isFirstMessage) {
        this.initSetupInfo(parsedMsg);
        this.isFirstMessage = false;
    } else if (this.isTaskIds(parsedMsg)) {
        this.logToFile('New task ids received.');
        this.handleNewTaskId(parsedMsg);
    } else {
        this.logToFile('New command received.');
        this.handleNewCommand(parsedMsg);
    }
}

Storm.prototype.handleNewTaskId = function(taskIds) {
    //When new list of task ids arrives, the callback that was passed with the corresponding emit should be called.
    //Storm assures that the task ids will be sent in the same order as their corresponding emits so it we can simply
    //take the first callback in the list and be sure it is the right one.

    var callback = this.taskIdsCallbacks.shift();
    if (callback) {
        callback(taskIds);
    } else {
        throw new Error('Something went wrong, we off the split of task id callbacks');
    }
}

Storm.prototype.createDefaultEmitCallback = function(tupleId) {
    return function(taskIds) {
        logToFile('Tuple ' + tupleId + ' sent to task ids - ' + JSON.stringify(taskIds));
    };
}

Storm.prototype.emit = function(tup, stream, id, directTask, callback) {
    //Every emit triggers a response - list of task ids to which the tuple was emitted. The task ids are accessible
    //through the callback (will be called when the response arrives). The callback is stored in a list until the
    //corresponding task id list arrives.

    if (!callback) {
        callback = this.createDefaultEmitCallback(id);
    }

    this.taskIdsCallbacks.push(callback);
    this.__emit(tup, stream, id, directTask);
}

Storm.prototype.emitDirect = function(tup, stream, id, directTask) {
    this.__emit(tup, stream, id, directTask)
}

Storm.prototype.initialize = function(conf, context, done) {
    done();
}

Storm.prototype.run = function() {
    this.logToFile('Start running');
    this.startReadingInput();
}

function Tuple(id, component, stream, task, values) {
    this.id = id;
    this.component = component;
    this.stream = stream;
    this.task = task;
    this.values = values;
}

function BasicBolt() {
    Storm.call(this);
    this.anchorTuple = null;
    this.name = 'BOLT'
};

BasicBolt.prototype = Object.create(Storm.prototype);
BasicBolt.prototype.constructor = BasicBolt;

BasicBolt.prototype.process = function(tuple, done) {};

BasicBolt.prototype.__emit = function(tup, stream, anchors, directTask) {
    var self = this;
    if (typeof anchors === 'undefined') {
        anchors = [];
    }

    if (this.anchorTuple !== null) {
        anchors = [this.anchorTuple]
    }
    var m = {"command": "emit"};

    if (typeof stream !== 'undefined') {
        m["stream"] = stream
    }

    m["anchors"] = anchors.map(function (a) {
        return a.id;
    });

    if (typeof directTask !== 'undefined') {
        m["task"] = directTask;
    }
    m["tuple"] = tup;
    this.sendMsgToParent(m);
}

BasicBolt.prototype.handleNewCommand = function(command) {
    var self = this;
    var tup = new Tuple(command["id"], command["comp"], command["stream"], command["task"], command["tuple"]);
    this.anchorTuple = tup;
    var callback = function(err) {
          if (!!err) {
              self.fail(tup, err);
          }
          self.ack(tup);
      }
    this.process(tup, callback);
}

BasicBolt.prototype.ack = function(tup) {
    this.sendMsgToParent({"command": "ack", "id": tup.id});
}

BasicBolt.prototype.fail = function(tup, err) {
    this.sendMsgToParent({"command": "fail", "id": tup.id});
}

function Spout() {
    Storm.call(this);
    this.name = 'SPOUT';
};
Spout.prototype = Object.create(Storm.prototype);
Spout.prototype.constructor = Spout;

Spout.prototype.ack = function(id, done) {};

Spout.prototype.fail = function(id, done) {};

Spout.prototype.nextTuple = function(done) {};

Spout.prototype.handleNewCommand = function(command) {
    var self = this;
    var callback = function() {
        self.sync();
    }

    if (command["command"] === "next") {
        this.nextTuple(callback);
    }

    if (command["command"] === "ack") {
        this.ack(command["id"], callback);
    }

    if (command["command"] === "fail") {
        this.fail(command["id"], callback);
    }
}

Spout.prototype.__emit = function(tup, stream, id, directTask) {
    var m = {"command": "emit"};
    if (typeof id !== 'undefined') {
        m["id"] = id;
    }

    if (typeof stream !== 'undefined') {
        m["stream"] = stream;
    }

    if (typeof directTask !== 'undefined') {
        m["task"] = directTask;
    }

    m["tuple"] = tup;
    this.sendMsgToParent(m);
}

module.exports.BasicBolt = BasicBolt;
module.exports.logToFile = logToFile;
module.exports.Spout = Spout;