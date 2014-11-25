
var net = require('net');
var serializer = require('./serializer');
var eOpCode = require('./eOpCode');

var Client = function (address) {
	this._address = address;
	this._connection = null;

	this._stream = 0;
	this._callbacks = {};
};

Client.prototype.connect = function (callback) {
	var client = this;
	var oldChunk = null;

	var connection = net.connect(8008, this._address, function () {
		client._connection = connection;
		callback(null);
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

};


Client.prototype._onFrame = function (data) {
	var client = this;

	var message = serializer.deserialize(data);

	if (message.opCode === eOpCode.ENQUEUE_OK) {
		client._callbacks[message.stream](null);
		client._callbacks[message.stream] = null;
	}
	else if (message.opCode === eOpCode.MESSAGE) {
		client._callbacks[message.stream](null, message, function (error) {
			client.commit(error, message.stream);
		});
		client._callbacks[message.stream] = null;
	}
	else {
		throw new Error('Unknown opCode :' + message.opCode);
	}

};


Client.prototype._nextStream = function () {
	this._stream++;
	if (this._stream > 65535) {
		this._stream = 0;
	}
	return this._stream;
};


Client.prototype.enqueue = function (app, partition, timestamp, message, callback) {
	
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

	this._callbacks[message.stream] = callback;
	this._connection.write(frame);
};


Client.prototype.dequeue = function (app, callback) {

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


Client.prototype.commit = function (error, stream) {

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
};


Client.prototype.close = function () {
	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	this._connection.end();
};


module.exports = Client;
