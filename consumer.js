

var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

smtq.connect(function (error) {
	if (error) {
		throw new Error('Failed to connect to queue');
	}

	console.log('connected to queue');

	recursive(1, deque);
	//recursive(2, deque);


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

		console.log(arg + ' dequed: ' + message.partition + '|' + message.content + ' ' + message.stream);

		if (message.content === '2') {
			setTimeout(function () {
				callback(new Error('bananas'));
				callback2(null);
			}, 10);
		}
		else {
			setImmediate(function () {
				callback(null);
				callback2(null);
			});
		}



	});
};

