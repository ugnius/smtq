

var SmtqClient = require('./smtq_client');

var smtq = new SmtqClient('localhost');

smtq.connect(function (error) {
	if (error) {
		throw new Error('Failed to connect to queue');
	}

	console.log('connected to queue');

	var text = (new Date()).toISOString();
	var partition = String((Math.random() * 4) | 0);

	//smtq.enqueue('app1', partition, Date.now(), text, function (error) {

	//	if (error) {
	//		throw error;
	//	}

	//	console.log('enqued: ' + partition + '|' + text);
	//	smtq.close();

	//});

	repeat(enqueue, 10, function () {
		smtq.close();
	});


	//smtq.enqueue('app1', '1', 2, '2', function () {
	//	smtq.enqueue('app1', '1', 3, '3', function () {
	//		smtq.enqueue('app1', '1', 1, '1', function () { });
	//	});
	//});
	

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

	smtq.enqueue('app1', '1', time, String(time), callback);
}

