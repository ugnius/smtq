
var net = require('net');
var serializer = require('./serializer');



var queues = {};

var Queue = function (name) {
	this.name = name;
	this.partitions = {};

	this.dequeue_queue = [];

};

Queue.prototype.enqueue = function (message) {
	var partition = this.partitions[message.partition];
	if (!partition) {
		partition = new Partition(message.partition, this);
		this.partitions[message.partition] = partition;
	}

	partition.enqueue(message);

	setImmediate(this.serveQueue.bind(this));
};


Queue.prototype.dequeue = function (callback) {

	this.dequeue_queue.push(callback);
	setImmediate(this.serveQueue.bind(this));

};


Queue.prototype.serveQueue = function () {

	var activePartition = null;

	// TODO: partitions are selected by name priority
	for (var name in this.partitions) {
		var partition = this.partitions[name];
		if (partition.isActive()) {
			activePartition = partition;
			break;
		}
	}

	if (!activePartition) {
		return;
	}

	var req = this.dequeue_queue.shift();
	if (!req) {
		return;
	}

	activePartition.dequeue(req);

};


var Partition = function (name, queue) {
	this.name = name;
	this.messages = [];
	this.queue = queue;
	this.busy = false;

	this.freshTimeout = null;

};

Partition.prototype.isActive = function()
{
	return this.messages.length > 0 && !this.busy && !this.freshTimeout;
}

Partition.prototype.enqueue = function (message) {

	var i;
	for (i = this.messages.length - 1; i >= 0; i--) {
		if (this.messages[i].timestamp <= message.timestamp) {
			break;
		}
	}

	this.messages.splice(i + 1, 0, {
		timestamp: message.timestamp,
		content: message.message
	});

	if (this.freshTimeout) {
		clearTimeout(this.freshTimeout);
	}

	var partition = this;

	partition.freshTimeout = setTimeout(function () {
		partition.freshTimeout = null;

		setImmediate(partition.queue.serveQueue());

	}, 1000);

};

Partition.prototype.dequeue = function (callback) {
	
	var partition = this;
	partition.busy = true;

	var m = partition.messages[0];

	callback(null, {
		partition: this.name,
		content: m.content
	}, function (error) {
		if (error) {
			console.log(error);
		}
		else {
			partition.messages.splice(partition.messages.indexOf(m), 1);
		}

		partition.busy = false;

		partition.queue.serveQueue();
	});

};



var onConnection = function (connection) {

	//console.log('new connection');

	var _callback = null;

	connection.on('data', function (data) {

		//console.log(data.toString('hex'));

		var length = data.readUInt32BE(0);

		var message = serializer.deserialize(data.slice(4, length + 4));
		//console.log(message);

		if (message.opCode === 1) {

			var queue = queues[message.app];
			if (!queue) {
				queue = new Queue(message.app);
				queues[message.app] = queue;
			}

			queue.enqueue(message);

			var response = { opCode: 2 };
			var frame = serializer.serialize(response);

			connection.write(frame);

		}
		else if (message.opCode === 3) {

			var queue = queues[message.app];
			if (!queue) {
				queue = new Queue(message.app);
				queues[message.app] = queue;
			}

			queue.dequeue(function (error, m, callback) {

				if (error) {
					throw error;
				}

				var response = {
					opCode: 4,
					app: message.app,
					partition: m.partition,
					content: m.content,
				};
				var frame = serializer.serialize(response);

				_callback = callback;
				connection.write(frame);

			});
		}
		else if (message.opCode === 5) {

			var cb = _callback;
			_callback = null;
			cb(null);
		}
		else if (message.opCode === 6) {

			var cb = _callback;
			_callback = null;
			cb(new Error(message.error));
		}

		else {
			throw new Error('Unknown opCode :' + message.opCode);
		}

	});

	connection.on('end', function () {
		//console.log('connection end');
	});

	connection.on('error', function (error) {
		//console.log('connection error');

		if (_callback) {
			var cb = _callback;
			_callback = null;
			cb(error);
		}

	});

};

var server = net.createServer(onConnection);

server.listen(8008, function () {
	console.log('queue listening for connections on port 8008');
});
