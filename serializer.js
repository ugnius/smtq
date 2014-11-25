
var eOpCode = require('./eOpCode');

exports.deserialize = function (buffer) {

	var r = new Reader(buffer);

	var message = {};
	message.opCode = r.readByte();
	message.stream = r.readShort();

	if (message.opCode === eOpCode.ENQUEUE) {

		message.app = r.readString();
		message.partition = r.readString();
		message.timestamp = r.readLong();
		message.message = r.readString();
	}
	else if (message.opCode === eOpCode.ENQUEUE_OK) {

	}
	else if (message.opCode === eOpCode.DEQUEUE) {

		message.app = r.readString();
	}
	else if (message.opCode === eOpCode.MESSAGE) {

		message.app = r.readString();
		message.partition = r.readString();
		message.content = r.readString();
	}
	else if (message.opCode === eOpCode.MESSAGE_ACK) {

	}
	else if (message.opCode === eOpCode.ERROR) {

		message.error = r.readString();
	}
	else {
		throw new Error('Opcode ' + message.opCode + ' deserialize is not implemented');
	}

	return message;
};

exports.serialize = function (message) {

	var w = new Writer();
	w.writeByte(message.opCode);
	w.writeShort(message.stream);

	if (message.opCode === eOpCode.ENQUEUE) {

		w.writeString(message.app);
		w.writeString(message.partition);
		w.writeLong(message.timestamp);
		w.writeString(message.message);
	}
	else if (message.opCode === eOpCode.ENQUEUE_OK) {

	}
	else if (message.opCode === eOpCode.DEQUEUE) {

		w.writeString(message.app);
	}
	else if (message.opCode === eOpCode.MESSAGE) {

		w.writeString(message.app);
		w.writeString(message.partition);
		w.writeString(message.content);
	}
	else if (message.opCode === eOpCode.MESSAGE_ACK) {

	}
	else if (message.opCode === eOpCode.ERROR) {

		w.writeString(message.error);
	}
	else {
		throw new Error('Opcode ' + message.opCode + ' serialize is not implemented');
	}

	var f = new Buffer(4 + w.getLength());
	f.writeUInt32BE(w.getLength(), 0);
	w.getData().copy(f, 4);

	return f;

};


var Reader = function (buffer) {
	this.buffer = buffer;
	this.o = 0;
};

Reader.prototype.readByte = function () {
	var v = this.buffer.readUInt8(this.o);
	this.o += 1;
	return v;
};

Reader.prototype.readShort = function () {
	var v = this.buffer.readUInt16BE(this.o);
	this.o += 2;
	return v;
};

Reader.prototype.readInt = function () {
	var v = this.buffer.readUInt32BE(this.o);
	this.o += 4;
	return v;
};


Reader.prototype.readLong = function () {
	var ms = this.readInt();
	var ls = this.readInt();
	return ms * 4294967296 + ls;
};

Reader.prototype.readString = function () {
	var l = this.readShort();
	var v = this.buffer.toString('utf8', this.o, this.o + l);
	this.o += l;
	return v;
};


var Writer = function () {
	this.buffer = new Buffer(4096);
	this.o = 0;
};

Writer.prototype.getLength = function () {
	return this.o;
};

Writer.prototype.getData = function () {
	return this.buffer.slice(0, this.o);
};

Writer.prototype.writeByte = function (value) {
	this.buffer.writeUInt8(value, this.o);
	this.o += 1;
};

Writer.prototype.writeShort = function (value) {
	this.buffer.writeUInt16BE(value, this.o);
	this.o += 2;
};

Writer.prototype.writeInt = function (value) {
	this.buffer.writeUInt32BE(value, this.o);
	this.o += 4;
};

Writer.prototype.writeLong = function (value) {
	this.writeInt((value / 4294967296) | 0);
	this.writeInt(value % 4294967296);
};

Writer.prototype.writeString = function (value) {
	var l = Buffer.byteLength(value);
	this.writeShort(l);
	this.buffer.write(value, this.o);
	this.o += l;
};