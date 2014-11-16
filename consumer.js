

var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

smtq.connect(function (error) {
	if (error) {
		throw new Error('Failed to connect to queue');
	}

	console.log('connected to queue');

	recursive(deque);


});

var recursive = function (fn) {

	fn(function () {
		recursive(fn);
	});

};

var deque = function (callback2) {

	console.log('dequeue');

	smtq.dequeue('app1', function (error, message, callback) {
		if (error) { throw error; }

		console.log('dequed: ' + message.partition + '|' + message.content);

		//callback(new Error('HUR DUR'));

		setTimeout(function () {
			callback(null);
			callback2(null);
		}, 100);

	});
};

