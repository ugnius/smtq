
var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

smtq.connect(function (error) {
	if (error) {
		throw new Error('Failed to connect to queue');
	}

	console.log('connected to queue');

	var connections = parseInt(process.argv[2], 10) || 1;
	var messages = parseInt(process.argv[3], 10) || 10;
	console.log(connections + ' connections ' + messages + ' messages');

	var done = 0;

	for (var i = 0; i < connections; i++) {
		repeat(enqueue, messages / connections, function () {
			done++;
			if ( done === connections ) {
				smtq.close();
			}
		});
	}

});



var repeat = function (fn, times, callback) {

	if (times <= 0) {
		return callback(null);
	}

	fn(function () {
		repeat(fn, times - 1, callback);
	});
};


var enqueue = function (callback) {
	var time = (Math.random() * 100) | 0;
	var partition = (Math.random() * 1000) | 0;

	//console.log(partition, time);
	smtq.enqueue('app1', String(partition), time, String(time), callback);
}

