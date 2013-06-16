var Job = require("node-batch")
,	FileInfra = require("../../batch-infrastructure-file");

var inDataOpt = {
		"path" : "./input.txt"	
	};

var outDataOpt = {
		"path": "./output.out"
	}

var indatasource = FileInfra.getInputDatasource(inDataOpt);
indatasource.separator(",");
indatasource.parser(function(row){
	return row.split(this._separator).reverse();
});
indatasource.schema("9,8,7,6,5,4,3,2,1")
indatasource.mapper(function(row){
	var self = this;
	var obj = {};
	row.forEach(function(elem, i){
		obj[self._schema[i]] = elem;
	});
	return obj;
});

var outdatasource = FileInfra.getOutputDatasource(outDataOpt);
outdatasource.lineseparator("\n");

var job = new Job(indatasource, outdatasource);

job.use(function(done){
	var data = this.data;
	data["0"] = (Math.random().toString(36).substring(2));
})
.use(function(done){
	var data = this.data;
	data["10"] = ("Processed!")
})
.use(function(){
	var str = "";
	var data = this.data;
	for (var key in data){
		str += data[key] + ",";
	}
	this.data = str;
})

job.run();