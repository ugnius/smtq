
var net = require('net');
var serializer = require('./serializer');

var Client = function (address) {
	this._address = address;
	this._connection = null;

	this._callback = null;
};

Client.prototype.connect = function (callback) {
	var client = this;

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

	connection.on('data', function (data) {
		
		//console.log(data.toString('hex'));

		var length = data.readUInt32BE(0);
		var message = serializer.deserialize(data.slice(4, length + 4));

		//console.log(message);

		if (message.opCode === 2) {

			var cb = client._callback;
			client._callback = null;
			cb(null);
		}
		else if (message.opCode === 4) {
			var cb = client._callback;
			client._callback = null;
			cb(null, message, function (error) {
				client.commit(error);
			});
		}
		else {
			throw new Error('Unknown opCode :' + message.opCode);
		}

	});


};


Client.prototype.enqueue = function (app, partition, timestamp, message, callback) {
	
	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	if (this._callback) {
		return callback(new Error('There is already active request'));
	}

	var message = {
		opCode: 1,
		app: app,
		partition: partition,
		timestamp: timestamp,
		message: message
	};

	var frame = serializer.serialize(message);

	this._callback = callback;
	this._connection.write(frame);
};


Client.prototype.dequeue = function (app, callback) {

	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	if (this._callback) {
		return callback(new Error('There is already active request'));
	}

	var message = {
		opCode: 3,
		app: app
	};

	var frame = serializer.serialize(message);

	this._callback = callback;
	this._connection.write(frame);

};


Client.prototype.commit = function (error) {

	if (!this._connection) {
		return callback(new Error('Not connected to queue'));
	}

	var message = error ? 
		{ opCode: 6, error: error.message } :
		{ opCode: 5 };

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
