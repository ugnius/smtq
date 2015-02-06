
var net = require('net');
var serializer = require('./serializer');
var eOpCode = require('./eOpCode');

var Client = function (address) {
	this._address = address;
	this._connection = null;
	this._close = false;

	this._stream = 0;
	this._callbacks = {};

	this.connect(function () { });
};

Client.prototype.connect = function (callback) {
	var client = this;
	var oldChunk = null;

	var connection = net.connect(8008, this._address);

	connection.on('connect', function () {
		console.log('connection.connect');
		client._connection = connection;
		callback(null);

		if (client._close) {
			client.close();
		}

	});

	connection.on('error', function (error) {
		client._connection = null;

		if (error.syscall === 'connect') {
			return callback(error);
		}

	});

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
			client._onFrame(frame);

			chunk = chunk.slice(length + 4);
		}

	});

	connection.on('timeout', function () {
		console.log('connection.timeout');
	});

	connection.on('close', function () {
		console.log('connection.close');
		client._connection = null;

		for (var stream in client._callbacks) {
			var c = client._callbacks[stream];
			if (c) {
				c(new Error('Connection closed!'));
				client._callbacks[stream] = null;
			}
		}
		
		if (!client._close) {
			// TODO: reconnect delay interval increase
			console.log('reconnect');

			setTimeout(function () {
				client.connect(function () { });
			}, 1000);
		}

	});

	connection.on('end', function () {
		console.log('connection.end');
	});

};


Client.prototype._onFrame = function (data) {
	var client = this;

	var message = serializer.deserialize(data);

	if (message.opCode === eOpCode.ENQUEUE_OK) {
		client._callbacks[message.stream](null);
		client._callbacks[message.stream] = null;
	}
	else if (message.opCode === eOpCode.MESSAGE) {
		client._callbacks[message.stream](null, message, function (error, ackCallback) {
			client.commit(error, message.stream, ackCallback);
		});
		client._callbacks[message.stream] = null;
	}
	else {
		throw new Error('Unknown opCode :' + message.opCode);
	}

};

// TODO reuse smaller number of streams. Use inactive stream queue?
Client.prototype._nextStream = function () {
	this._stream++;
	if (this._stream > 65535) {
		this._stream = 0;
	}
	return this._stream;
};


Client.prototype.enqueue = function (app, partition, timestamp, message, callback) {
	var client = this;

	client._enqueue(app, partition, timestamp, message, function (error) {
		if (error) {
			if (error.message === 'Not connected to queue') {

				setTimeout(function () {
					client.enqueue(app, partition, timestamp, message, callback);
				}, 1000);

			}
			else {
				callback(error);
			}

			return;
		}

		callback(null);
	});
};


Client.prototype._enqueue = function (app, partition, timestamp, message, callback) {
	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	var message = {
		opCode: eOpCode.ENQUEUE,
		stream: this._nextStream(),
		app: app,
		partition: partition,
		timestamp: timestamp,
		message: message
	};

	var frame = serializer.serialize(message);

	this._connection.write(frame);
	this._callbacks[message.stream] = callback;
};


Client.prototype.dequeue = function (app, callback) {
	var client = this;

	client._dequeue(app, function (error, message, ackCallback) {
		if (error) {
			if (error.message === 'Not connected to queue') {
				setTimeout(function () {
					client.dequeue(app, callback);
				}, 1000);

			}
			else {
				callback(error);
			}

			return;
		}
		callback(error, message, ackCallback);
	});
};


Client.prototype._dequeue = function (app, callback) {
	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	var message = {
		opCode: eOpCode.DEQUEUE,
		stream: this._nextStream(),
		app: app
	};

	var frame = serializer.serialize(message);

	this._callbacks[message.stream] = callback;
	this._connection.write(frame);
};


Client.prototype.commit = function (error, stream, callback) {
	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	var message = {
		opCode: error ? eOpCode.ERROR : eOpCode.MESSAGE_ACK,
		stream: stream,
		error: error ? error.message : null
	};

	var frame = serializer.serialize(message);

	this._connection.write(frame);

	callback(null);
};


Client.prototype.close = function () {
	this._close = true;

	if (this._connection !== null) {

		console.log('Client.close');
		this._connection.end();
	}

};


module.exports = Client;
