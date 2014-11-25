
var net = require('net');
var serializer = require('./serializer');
var eOpCode = require('./eOpCode');


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

	this.first = null;
	this.last = null;

	this.queue = queue;
	this.busy = false;

	this.freshTimeout = null;
	this.errorTimeout = null;

};

Partition.prototype._insertMessage = function (m) {
	var partition = this;

	if (partition.first === null) {
		partition.first = m;
		partition.last = m;
	}
	else {
		if (partition.first.timestamp > m.timestamp) {
			m.next = partition.first;
			partition.first = m;
		}
		else {
			var i = partition.first;
			while (i.next !== null && i.next.timestamp <= m.timestamp) {
				i = i.next;
			}
			m.next = i.next;
			i.next = m;

			if (partition.last === i) {
				partition.last = m;
			}
		}
	}
};

Partition.prototype._removeMessage = function (m) {
	var partition = this;

	if (partition.first === m) {
		partition.first = m.next;
		if (partition.last === m) {
			partition.last = null;
		}
	}
	else {
		var i = partition.first;
		while (i.next !== m) {
			i = i.next;
		}

		i.next = m.next;
		if (partition.last === m) {
			partition.last = i;
		}
	}
};

Partition.prototype.isActive = function () {
	return this.first !== null && !this.busy && !this.freshTimeout && !this.errorTimeout;
};

Partition.prototype.enqueue = function (message) {
	var partition = this;

	var m = {
		timestamp: message.timestamp,
		content: message.message,
		failCount: 0,
		next: null
	};

	partition._insertMessage(m);

	if (this.freshTimeout) {
		clearTimeout(this.freshTimeout);
	}

	partition.freshTimeout = setTimeout(function () {
		partition.freshTimeout = null;
		setImmediate(partition.queue.serveQueue());
	}, 1000);

};

Partition.prototype.dequeue = function (callback) {
	
	var partition = this;
	partition.busy = true;

	var m = partition.first;

	callback(null, {
		partition: partition.name,
		content: m.content
	}, function (error) {
		partition.busy = false;

		if (error) {
			if (error.message === 'Connection closed') {
				partition.queue.serveQueue();
			}
			else {
				console.log(error);

				m.failCount++;
				if (m.failCount >= 3) {
					partition._removeMessage(m);
				}

				partition.errorTimeout = setTimeout(function () {
					partition.errorTimeout = null;
					setImmediate(partition.queue.serveQueue());
				}, 1000);
			}
		}
		else {
			partition._removeMessage(m);
			partition.queue.serveQueue();
		}

	});

};



var onConnection = function (connection) {
	var _callbacks = {};
	var oldChunk = null;
	var closed = false;

	var onFrame = function (data) {

		var message = serializer.deserialize(data);

		if (message.opCode === eOpCode.ENQUEUE) {

			var queue = queues[message.app];
			if (!queue) {
				queue = new Queue(message.app);
				queues[message.app] = queue;
			}

			queue.enqueue(message);

			var response = {
				opCode: eOpCode.ENQUEUE_OK,
				stream: message.stream,
			};
			var frame = serializer.serialize(response);

			connection.write(frame);

		}
		else if (message.opCode === eOpCode.DEQUEUE) {

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
					opCode: eOpCode.MESSAGE,
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
		else if (message.opCode === eOpCode.MESSAGE_ACK) {

			_callbacks[message.stream](null);
			_callbacks[message.stream] = null;
		}
		else if (message.opCode === eOpCode.ERROR) {

			_callbacks[message.stream](new Error(message.error));
			_callbacks[message.stream] = null;
		}

		else {
			throw new Error('Unknown opCode :' + message.opCode);
		}
	};

	connection.on('data', function (chunk) {

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
