

var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

smtq.connect(function (error) {
	if (error) {
		throw new Error('Failed to connect to queue');
	}

	console.log('connected to queue');

	var connections = parseInt(process.argv[2], 10) | 1;

	for (var i = 0; i < connections; i++) {
		recursive(1, deque);
	}


});

var recursive = function (arg, fn) {

	fn(arg, function () {
		recursive(arg, fn);
	});

};

var deque = function (arg, callback2) {

	//console.log(arg + ' dequeue');

	smtq.dequeue('app1', function (error, message, callback) {
		if (error) { throw error; }

		//console.log(arg + ' dequed: ' + message.partition + '|' + message.content + ' ' + message.stream);


		setImmediate(function () {
			callback(null);
			callback2(null);
		});



	});
};

