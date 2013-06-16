/**
* Module dependencies
*/
var fs = require("fs")
,	EventEmitter = require("events").EventEmitter
,	util = require("util")
,	readline = require("readline");


var FileInfrastructure = {};
module.exports = FileInfrastructure;

FileInfrastructure.getInputDatasource = function(options) {
	return new InputDatasource(options);
}

FileInfrastructure.getOutputDatasource = function(options) {
	return new OutputDatasource(options);
}

/**
* Required
*/
function InputDatasource (options) {
	var self = this;
	var options = options || {};
	self.path = options.path;
	self.separator();
	self.parser();
	self.schema();
	self.mapper();
	self.line = 0;
	EventEmitter.call(self);
}

util.inherits(InputDatasource, EventEmitter);

InputDatasource.prototype.separator = function(s){
	this._separator = s || ",";
}

InputDatasource.prototype.parser = function(parser){
	var self = this;
	self._parse = (typeof parser === 'function' && parser) || function(row){
		return row.split(self._separator);
	}
}

InputDatasource.prototype.schema = function(schema){
	if(!schema){
		return;
	}
	this._schema = schema.split(this._separator);
}

InputDatasource.prototype.mapper = function(mapper){
	var self = this;
	var obj = {};
	this._map = (typeof mapper === 'function' && mapper) || function(row){
		if(self._schema){
			self._schema.forEach(function(elem, i){
				obj[elem] = row[i];
			});
			return obj;
		}
		return row;
	};
}

InputDatasource.prototype.read = function() {
	var self = this
	var readstream = fs.createReadStream(self.path);
	rl = readline.createInterface({
		"input": readstream,
		"output": process.stdout,
		"terminal": false
	});

	rl.on("line", function(line){
		var data = self._map(self._parse(line));
		self.emit("data", null, data);
	});

	readstream.on("end", function() {
		rl.close();
		readstream.close()
		self.emit("end");
	})
};

/**
* Required
*/
function OutputDatasource (options) {
	var self = this;
	options = self.options = options || {};
	self.path = options.path;
	self.stream = fs.createWriteStream(options.path);
	self.lineseparator();
}

OutputDatasource.prototype.lineseparator = function(sep){
	this._lineseparator = sep || "\n";
}

OutputDatasource.prototype.beforeWrite = function(fn){
	ensureFunction(fn);
	self._beforeWrite = fn;
}

OutputDatasource.prototype.write = function(data) {
	var self = this;
	if(self._beforeWrite){
		data = self._beforeWrite(data);
	}
	self.stream.write(data + self._lineseparator)
};

OutputDatasource.prototype.close = function(){
	this.stream.end("");
}


function ensureFunction(fn){
	if(!typeof fn === "function")
	throw new Error(fn + ": Expected function but got " + typeof fn);
}