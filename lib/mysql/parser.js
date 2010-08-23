if (global.GENTLY) require = GENTLY.hijack(require);

// see: http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

var sys = require('sys'),
    Buffer = require('buffer').Buffer,
    EventEmitter = require('events').EventEmitter;

function Parser() {
  EventEmitter.call(this);

  this.packet = null;

  this.buffers = [];
  this.bufferPos = 0;
  this.buffersLength = 0;

  this.queue = [];
  this.queueInsertionPoint = 0;

  // We always start with a greeting packet
  this._receiveGreetingPacket();
}
sys.inherits(Parser, EventEmitter);
module.exports = Parser;

Parser.prototype._fetchBytes = function(bytes, callback) {
  this.queue.splice(this.queueInsertionPoint++, 0, {
    callback: callback,
    bytes: bytes
  });
};

Parser.prototype._skipBytes = function(bytes) {
  this.queue.splice(this.queueInsertionPoint++, 0, {
    callback: undefined,
    bytes: bytes
  });
};


Parser.prototype._enqueue = function(callback) {
  this.queue.splice(this.queueInsertionPoint++, 0, {
    callback: callback,
    bytes: undefined
  });
};

Parser.prototype._fetchInt8 = function(callback) {
  this._fetchBytes(1, function(buffer) {
    callback.call(this, buffer[0]);
  });
};

Parser.prototype._fetchInt16 = function(callback) {
  this._fetchBytes(2, function(buffer) {
    callback.call(this,    buffer[0]
                        + (buffer[1] << 8));
  });
};

Parser.prototype._fetchInt24 = function(callback) {
  this._fetchBytes(3, function(buffer) {
    callback.call(this,    buffer[0]
                        + (buffer[1] <<  8)
                        + (buffer[2] << 16));
  });
};

Parser.prototype._fetchInt32 = function(callback) {
  this._fetchBytes(4, function(buffer) {
    callback.call(this,    buffer[0]
                        + (buffer[1] << 8 )
                        + (buffer[2] << 16)
                        + (buffer[3] << 24));
  });
};

Parser.prototype._fetchInt64 = function(callback) {
  this._fetchBytes(8, function(buffer) {
    callback.call(this,    buffer[0]
                        + (buffer[1] << 8 )
                        + (buffer[2] << 16)
                        + (buffer[3] << 24)
                        + (buffer[4] << 32)
                        + (buffer[5] << 40)
                        + (buffer[6] << 48)
                        + (buffer[7] << 56));
  });
};


Parser.prototype._fetchLCInt = function(callback) {
  this._fetchInt8(function(value) {
    switch (value) {
      case Parser.LENGTH_CODED_16BIT_WORD:
        this._fetchInt16(callback);
        break;

      case Parser.LENGTH_CODED_24BIT_WORD:
        this._fetchInt24(callback);
        break;

      case Parser.LENGTH_CODED_64BIT_WORD:
        this._fetchInt64(callback);
        break;

      case Parser.LENGTH_CODED_NULL:
        callback.call(this, null);
        break;

      default:
        callback.call(this, value);
        break;
    }
  });
};

/**
 * The same as _fetchLCInt, but negates the value if a
 * magic value is encountered.
 * It's used for the inital byte(s) of a result packet.
 * NULL is not supported.
 */
Parser.prototype._fetchLCIntStatus = function(callback) {
  this._fetchInt8(function(value) {
    switch (value) {
      case Parser.LENGTH_CODED_ERROR:
      case Parser.LENGTH_CODED_OK:
        callback.call(this, -value);
        break;

      case Parser.LENGTH_CODED_EOF:
      case Parser.LENGTH_CODED_64BIT_WORD:
        // 0xfe is used to indicate both eof and a 64bit word;
        // therefore the packet length is checked; if it's shorter than
        // 9 bytes we see an eof packet, otherwise we see a 64bit word.
        if (this.packet.length < 9) {
          callback.call(this, -value);
        } else {
          this._fetchInt64(callback);
        }
        break;

      case Parser.LENGTH_CODED_16BIT_WORD:
        this._fetchInt16(callback);
        break;

      case Parser.LENGTH_CODED_24BIT_WORD:
        this._fetchInt24(callback);
        break;

      default:
        callback.call(this, value);
        break;
    }
  });
};

Parser.prototype._fetchLCString = function(callback) {
  this._fetchLCInt(function(value) {
    if (value !== null) {
      this._fetchBytes(value, function(buffer) {
        callback.call(this, buffer.toString('ascii'));
      });
    } else {
      callback.call(this, null);
    }
  });
};

Parser.prototype._fetchFixedString = function(length, callback) {
  this._fetchBytes(length, function(buffer) {
    callback.call(this, buffer.toString('ascii'));
  });
};

Parser.prototype._fetchEndString = function(callback) {
  this._enqueue(function() {
    this._fetchBytes(this.packet.length - this.packet.received, function(buffer) {
      callback.call(this, buffer.toString('ascii'));
    });
  });
};

/**
 * Fetches a null-terminated string
 * This implementation isn't very efficient, but it's used only
 * in the handshake stage so that shouldn't be much of problem.
 */
Parser.prototype._fetchNullTerminatedString = function(callback) {
  var result = "";
  function bite(buffer) {
    if (buffer[0] === 0) {
      callback.call(this, result);
    } else {
      result += String.fromCharCode(buffer[0]);
      this._fetchBytes(1, bite);
    }
  }
  this._fetchBytes(1, bite);
};

Parser.prototype._parsePacketHeader = function() {
  // Packet length
  this._fetchInt24(function(length) {
    this.packet.length = length;
  });

  // Packet number
  this._fetchInt8(function(number) {
    this.packet.number = number;
  });

  // Initialize packet index to zero
  this._enqueue(function() {
    this.packet.received = 0;
  });
};

Parser.prototype._optional = function(callback) {
  // Call callback only if there are any bytes left for the current packet
  this._enqueue(function() {
    if (this.packet.received < this.packet.length) {
      callback.call(this);
    }
  });
};

Parser.prototype._endPacket = function(callback) {
  var packet = this.packet;

  this._enqueue(function() {
    // Emit the packet
    this._emitPacket();

    // Make debugging a little easyer by unsetting the last packet
    this.packet = null;

    // Call the callback function if any
    callback && callback.apply(this);
  });
};

Parser.prototype._parseErrorPacket = function() {
  this._fetchInt16(function(errorNumber) {
    this.packet.errorNumber = errorNumber;
  });
  this._fetchFixedString(1, function(sqlStateMarker) {
    // SQL state marker should always be '#'
    this.packet.sqlStateMarker = sqlStateMarker;
  });
  this._fetchFixedString(5, function(sqlState) {
    this.packet.sqlState = sqlState;
  });
  this._fetchEndString(function(errorMessage) {
    this.packet.errorMessage = errorMessage;
  });
};

Parser.prototype._parseEOFPacket = function() {
  this._fetchInt16(function(warningCount) {
    this.packet.warningCount = warningCount;
  });
  this._fetchInt16(function(serverStatus) {
    this.packet.serverStatus = serverStatus;
  });
};

Parser.prototype._receiveGreetingPacket = function() {
  var packet = this.packet = new EventEmitter(),
      scrambleBuffer1;

  packet.type = Parser.GREETING_PACKET;

  this._parsePacketHeader();

  this._fetchInt8(function(protocolVersion) {
    packet.protocolVersion = protocolVersion;
  });
  this._fetchNullTerminatedString(function(serverVersion) {
    packet.serverVersion = serverVersion;
  });
  this._fetchInt32(function(threadId) {
    packet.threadId = threadId;
  });
  this._fetchBytes(8, function(buffer) {
    // First part of scramble buffer (8 bytes)
    scrambleBuffer1 = buffer;
  });
  this._skipBytes(1);
  this._fetchInt16(function(serverCapabilities) {
    packet.serverCapabilities = serverCapabilities;
  });
  this._fetchInt8(function(serverLanguage) {
    packet.serverLanguage = serverLanguage;
  });
  this._fetchInt16(function(serverStatus) {
    packet.serverStatus = serverStatus;
  });
  this._skipBytes(13);
  this._fetchBytes(13, function(scrambleBuffer2) {
    // Second part of scramble buffer (13 bytes)
    var scrambleBuffer = packet.scrambleBuffer = new Buffer(scrambleBuffer1.length + scrambleBuffer2.length);
    scrambleBuffer1.copy(scrambleBuffer, 0, 0);
    scrambleBuffer2.copy(scrambleBuffer, scrambleBuffer1.length, 0);
  });
  this._endPacket(this._receiveResultPacket);
};

Parser.prototype._receiveResultPacket = function() {
  var packet = this.packet = new EventEmitter();

  this._parsePacketHeader();

  this._fetchLCIntStatus(function(status) {
    switch (status) {
      case -Parser.LENGTH_CODED_ERROR:
        // Error packet
        packet.type = Parser.ERROR_PACKET;
        this._parseErrorPacket();
        this._endPacket(this._receiveResultPacket);
        break;

      case -Parser.LENGTH_CODED_OK:
        // OK packet
        packet.type = Parser.OK_PACKET;
        this._fetchLCInt(function(affectedRows) {
          packet.affectedRows = affectedRows;
        });
        this._fetchLCInt(function(insertId) {
          packet.insertId = insertId;
        });
        this._fetchInt16(function(serverStatus) {
          packet.serverStatus = serverStatus;
        });
        this._fetchInt16(function(warningCount) {
          packet.warningCount = warningCount;
        });
        this._fetchEndString(function(message) {
          packet.message = message;
        });
        this._endPacket(this._receiveResultPacket);
        break;

      default:
        // Result set header packet
        packet.type = Parser.RESULT_SET_HEADER_PACKET;
        packet.fieldCount = status;
        this._optional(function(extra) {
          this._fetchLCInt(function(extra) {
            packet.extra = extra;
          });
        });
        this._endPacket(this._receiveFieldPacket);
        break;
    }
  });
};

Parser.prototype._receiveFieldPacket = function() {
  var packet = this.packet = new EventEmitter();

  this._parsePacketHeader();

  this._fetchLCIntStatus(function(status) {
    switch (status) {
      case -Parser.LENGTH_CODED_ERROR:
        packet.type = Parser.ERROR_PACKET;
        this._parseErrorPacket();
        this._endPacket(this._receiveResultPacket);
        break;

      case -Parser.LENGTH_CODED_EOF:
        packet.type = Parser.EOF_PACKET;
        this._parseEOFPacket();
        this._endPacket(this._receiveRowPacket);
        break;

      default:
        packet.type = Parser.FIELD_PACKET;
        this._fetchFixedString(status, function(catalog) {
          packet.catalog = catalog;
        });
        this._fetchLCString(function(db) {
          packet.db = db;
        });
        this._fetchLCString(function(table) {
          packet.table = table;
        });
        this._fetchLCString(function(originalTable) {
          packet.originalTable = originalTable;
        });
        this._fetchLCString(function(name) {
          packet.name = name;
        });
        this._fetchLCString(function(originalName) {
          packet.originalName = originalName;
        });
        this._skipBytes(1);
        this._fetchInt16(function(charsetNumber) {
          packet.charsetNumber = charsetNumber;
        });
        this._fetchInt32(function(fieldLength) {
          packet.fieldLength = fieldLength;
        });
        this._fetchInt8(function(fieldType) {
          packet.fieldType = fieldType;
        });
        this._fetchInt16(function(flags) {
          packet.flags = flags;
        });
        this._fetchInt8(function(decimals) {
          packet.decimals = decimals;
        });
        this._skipBytes(2);
        this._optional(function() {
          // Optional 'default' value
          this._fetchLCInt(function(fieldDefault) {
            packet.fieldDefault = fieldDefault;
          });
        });
        this._endPacket(this._receiveFieldPacket);
        break;
    };
  });
};

Parser.prototype._receiveRowPacket = function() {
  var packet = this.packet = new EventEmitter();

  this._parsePacketHeader();

  this._fetchLCIntStatus(function(status) {
    switch (status) {
      case -Parser.LENGTH_CODED_ERROR:
        packet.type = Parser.ERROR_PACKET;
        this._parseErrorPacket();
        this._endPacket(this._receiveResultPacket);
        break;

      case -Parser.LENGTH_CODED_EOF:
        packet.type = Parser.EOF_PACKET;
        this._parseEOFPacket();
        this._endPacket(this._receiveResultPacket);
        break;

      default:
        var handleLength = function(columnLength) {
          if (columnLength === null) {
            handleData.call(this, null);
          } else {
            this._fetchBytes(columnLength, handleData);
          }
        };
        var handleData = function(data) {
          packet.emit('data', data, 0);
          if (this.packet.received < this.packet.length) {
            this._fetchLCInt(handleLength);
          } else {
            // No more columns
            this._enqueue(this._receiveRowPacket);
          }
        };
        packet.type = Parser.ROW_DATA_PACKET;
        this._emitPacket();
        handleLength.call(this, status);
        break;
    }
  });
};

Parser.prototype._emitPacket = function() {
  this.emit('packet', this.packet);
};

Parser.prototype.write = function(buffer) {
  this.buffers.push(buffer);
  this.buffersLength += buffer.length;

  this._call();
};

Parser.prototype._call = function() {
  var queue = this.queue,
      buffers = this.buffers,
      buffersLength = this.buffersLength,
      bufferPos = this.bufferPos;

  while (queue.length) {
    var queueTop = queue[0],
        bytes = queueTop.bytes;

    // If the next callback isn't interested in receiving bytes at all,
    // don't bother to look at the buffer buffer at all
    if (bytes === undefined) {
      queue.shift();
      this.queueInsertionPoint = 0;
      queueTop.callback.call(this);

    } else if (bytes === 0) {
        queue.shift();
        this.queueInsertionPoint = 0;
        queueTop.callback.call(this, new Buffer(0));

    } else {
      // See if there are enough bytes left in the buffer buffer
      if (buffersLength - bufferPos < bytes) {
        break;
      }
      var result,
          buffers = this.buffers,
          topBuffer = buffers[0],
          topBufferLength = topBuffer.length;

      if (topBufferLength >= bufferPos + bytes) {
        // If all the required bytes come from the first buffer in the buffer buffer,
        // slice will give us what we need

        result = topBuffer.slice(bufferPos, bufferPos + bytes);

        // Update buffer position,
        // remove the top buffer from the buffer buffer if it was completely consumed
        if ((bufferPos += bytes) === topBufferLength) {
          bufferPos = 0;
          buffersLength -= topBufferLength;
          buffers.shift();
        }

      } else {
        // The result comes from different buffers; construct a new buffer
        // and memcpy buffer buffer contents until the result buffer is full
        result = new Buffer(bytes);

        var offset = 0,
            copy;

        // Keep eating buffers until the result buffer is full
        while (offset < bytes) {
          copy = Math.min(bytes - offset, topBufferLength - bufferPos);
          topBuffer.copy(result, offset, bufferPos, bufferPos + copy);
          offset += copy;

          // Update top buffer position,
          // remove the top buffer from the buffer buffer and pick the next buffer if appropriate
          if ((bufferPos += copy) === topBufferLength) {
            buffersLength -= topBufferLength;
            bufferPos = 0;
            buffers.shift();
            topBuffer = buffers[0];
            topBufferLength = topBuffer.length;
          }
        }
      }

      // Increase packet received bytes
      this.packet.received += bytes;

      queue.shift();
      this.queueInsertionPoint = 0;
      queueTop.callback && queueTop.callback.call(this, result);
    }
  }

  this.bufferPos = bufferPos;
  this.buffersLength = buffersLength;
};

Parser.LENGTH_CODED_NULL = 251;
Parser.LENGTH_CODED_16BIT_WORD= 252;
Parser.LENGTH_CODED_24BIT_WORD= 253;
Parser.LENGTH_CODED_64BIT_WORD= 254;

Parser.LENGTH_CODED_OK = 0;
Parser.LENGTH_CODED_EOF = 254;
Parser.LENGTH_CODED_ERROR = 255;

// Packet types
var p                                   = 0;
Parser.GREETING_PACKET                  = p++;
Parser.OK_PACKET                        = p++;
Parser.ERROR_PACKET                     = p++;
Parser.RESULT_SET_HEADER_PACKET         = p++;
Parser.FIELD_PACKET                     = p++;
Parser.EOF_PACKET                       = p++;
Parser.ROW_DATA_PACKET                  = p++;
Parser.ROW_DATA_BINARY_PACKET           = p++;
Parser.OK_FOR_PREPARED_STATEMENT_PACKET = p++;
Parser.PARAMETER_PACKET                 = p++;
