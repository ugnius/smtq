

var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

//smtq.connect(function (error) {
//	if (error) {
//		throw new Error('Failed to connect to queue');
//	}

//	console.log('connected to queue');



//});

var recursive = function (arg, fn) {

	fn(arg, function () {
		recursive(arg, fn);
	});

};

var deque = function (arg, callback2) {

	//console.log(arg + ' dequeue');

	smtq.dequeue('app1', function (error, message, ackCallback) {
		if (error) {
			console.log(error);
			return callback2(error);
		}

		//console.log(arg + ' dequed: ' + message.partition + '|' + message.content + ' ' + message.stream);
		
		setImmediate(function () {
		//setTimeout(function () {
			ackCallback(null, function (error) {
				callback2(error);
			});
		//}, 90);
		});

	});
};

var connections = parseInt(process.argv[2], 10) || 1;

for (var i = 0; i < connections; i++) {
	recursive(1, deque);
}
