

var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

smtq.connect(function (error) {
	if (error) {
		throw new Error('Failed to connect to queue');
	}

	console.log('connected to queue');

	var text = (new Date()).toISOString();
	var partition = String((Math.random() * 4) | 0);

	repeat(enqueue, 66000, function () {
		smtq.close();
	});

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

