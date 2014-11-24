
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
		partition: partition.name,
		content: m.content
	}, function (error) {
		if (error) {
			if (error.message === 'Connection closed') {
			} else {
				console.log(error);
			}
		}
		else {
			partition.messages.splice(partition.messages.indexOf(m), 1);
		}

		partition.busy = false;
		partition.queue.serveQueue();
	});

};



var onConnection = function (connection) {
	var _callbacks = {};
	var oldChunk = null;
	var closed = false;

	var onFrame = function (data) {

		var message = serializer.deserialize(data);

		if (message.opCode === 1) {

			var queue = queues[message.app];
			if (!queue) {
				queue = new Queue(message.app);
				queues[message.app] = queue;
			}

			queue.enqueue(message);

			var response = {
				opCode: 2,
				stream: message.stream,
			};
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
				if (closed) {
					return callback(new Error('Connection closed'));
				}

				if (error) {
					throw error;
				}

				var response = {
					opCode: 4,
					stream: message.stream,
					app: message.app,
					partition: m.partition,
					content: m.content,
				};
				var frame = serializer.serialize(response);

				_callbacks[message.stream] = callback;
				connection.write(frame);
			});
		}
		else if (message.opCode === 5) {

			_callbacks[message.stream](null);
			_callbacks[message.stream] = null;
		}
		else if (message.opCode === 6) {

			_callbacks[message.stream](new Error(message.error));
			_callbacks[message.stream] = null;
		}

		else {
			throw new Error('Unknown opCode :' + message.opCode);
		}
	};

	connection.on('data', function (chunk) {
		console.log(chunk.toString('hex'));

		if (oldChunk) {
			chunk = Buffer.concat([oldChunk, chunk]);
			oldChunk = null;
		}

		while (chunk.length > 0) {

			if (chunk.length < 4) {
				oldChunk = chunk;
				return;
			}

			var length = chunk.readUInt32BE(0);

			if (chunk.length - 4 < length) {
				oldChunk = chunk;
				return;
			}

			var frame = chunk.slice(4, length + 4);
			onFrame(frame);

			chunk = chunk.slice(length + 4);
		}
	});

	connection.on('end', function () {
		//console.log('connection end');
		closed = true;
	});

	connection.on('error', function (error) {
		console.log('connection error');
		closed = true;

		for (var i in _callbacks) {
			if (_callbacks[i]) {
				_callbacks[i](error);
				_callbacks[i] = null;
			}
		}

	});

};

var server = net.createServer(onConnection);

server.listen(8008, function () {
	console.log('queue listening for connections on port 8008');
});
