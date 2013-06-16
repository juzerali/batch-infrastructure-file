/**
* Module dependencies
*/
var fs = require("fs")
,	EventEmitter = require("events").EventEmitter
,	util = require("util")
,	readline = require("readline");


var FileInfrastructure = {};
module.exports = FileInfrastructure;

FileInfrastructure.getInputDatasource = function(uri) {
	return new InputDatasource(uri);
}

FileInfrastructure.getOutputDatasource = function(uri) {
	return new OutputDatasource(uri)
}

function InputDatasource (options) {
	var self = this;
	var options = options || {};
	self.uri = options.uri;
	self.splitter = options.splitter || ",";
	self.line = 0;
	EventEmitter.call(self);
}

util.inherits(InputDatasource, EventEmitter);

InputDatasource.prototype.read = function() {
	var self = this
	var readstream = fs.createReadStream(self.uri);
	rl = readline.createInterface({
		"input": readstream,
		"output": process.stdout,
		"terminal": false
	});

	rl.on("line", function(line){
		self.emit("data", null, line);
	});

	readstream.on("end", function() {
		rl.close();
		readstream.close()
		self.emit("end");
	})
};

function OutputDatasource (options) {
	var self = this;
	options = self.options = options || {};
	self.uri = options.uri;
	self.stream = fs.createWriteStream(options.uri);
	self.lineseparator = options.lineseparator || "\n";
}

OutputDatasource.prototype.write = function(data) {
	var self = this;
	self.stream.write(data + self.lineseparator)
};

OutputDatasource.prototype.close = function(){
	this.stream.end("");
}