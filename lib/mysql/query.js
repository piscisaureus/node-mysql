if (global.GENTLY) require = GENTLY.hijack(require);

var sys = require('sys'),
    EventEmitter = require('events').EventEmitter,
    Parser = require('./parser'),
    Client;

function Query(properties) {
  EventEmitter.call(this);

  this._paused = false;
  this._resuming = false;
  this._eventBuffer = [];

  this.typeCast = true;

  for (var key in properties) {
    this[key] = properties[key];
  }
};
sys.inherits(Query, EventEmitter);
module.exports = Query;

Query.prototype._handlePacket = function(packet) {
  var self = this,
      userObject;

  // We can't do this require() on top of the file.
  // That's because there is circular dependency and we're overwriting
  // module.exports
  Client = Client || require('./client');

  switch (packet.type) {
    case Parser.OK_PACKET:
      userObject = Client._packetToUserObject(packet);
      if (!this._paused) {
        this.emit('end', userObject);
      } else {
        this._eventBuffer.push(['end', userObject]);
      }
      break;
    case Parser.ERROR_PACKET:
      userObject = Client._packetToUserObject(packet);
      if (!this._paused) {
        this.emit('error', userObject);
      } else {
        this._eventBuffer.push(['error', userObject]);
      }
      break;
    case Parser.FIELD_PACKET:
      if (!this._fields) {
        this._fields = [];
      }

      this._fields.push(packet);

      if (!this._paused) {
        this.emit('field', packet);
      } else {
        this._eventBuffer.push(['field', packet]);
      }
      break;
    case Parser.EOF_PACKET:
      if (!this._eofs) {
        this._eofs = 1;
      } else {
        this._eofs++;
      }

      if (this._eofs == 2) {
        if (!this._paused) {
          this.emit('end');
        } else {
          this._eventBuffer.push(['end']);
        }
      }
      break;
    case Parser.ROW_DATA_PACKET:
      var row = this._row = {},
          field = this._fields[0];

      this._rowIndex = 0;
      row[field.name] = '';

      packet.on('data', function(buffer, remaining) {
        if (buffer) {
          row[field.name] += buffer;
        } else {
          row[field.name] = null;
        }

        if (remaining == 0) {
          self._rowIndex++;
          if (self.typeCast) {
            switch (field.fieldType) {
              case Query.FIELD_TYPE_TIMESTAMP:
              case Query.FIELD_TYPE_DATE:
              case Query.FIELD_TYPE_DATETIME:
              case Query.FIELD_TYPE_NEWDATE:
                row[field.name] = new Date(row[field.name]+'Z');
                break;
              case Query.FIELD_TYPE_TINY:
              case Query.FIELD_TYPE_SHORT:
              case Query.FIELD_TYPE_LONG:
              case Query.FIELD_TYPE_LONGLONG:
              case Query.FIELD_TYPE_INT24:
              case Query.FIELD_TYPE_YEAR:
                row[field.name] = parseInt(row[field.name], 10);
                break;
              case Query.FIELD_TYPE_DECIMAL:
              case Query.FIELD_TYPE_FLOAT:
              case Query.FIELD_TYPE_DOUBLE:
              case Query.FIELD_TYPE_NEWDECIMAL:
                row[field.name] = parseFloat(row[field.name]);
                break;
            }
          }

          if (self._rowIndex == self._fields.length) {
             delete self._row;
             delete self._rowIndex;

             if (!self._paused) {
               self.emit('row', row);
             } else {
               self._eventBuffer.push(['row', row]);
             }
             return;
          }

          field = self._fields[self._rowIndex];
          row[field.name] = '';
        }
      });
      break;
  }
};

Query.prototype.pause = function() {
  if (!this._paused) {
    this._paused = true;

    if (!this._resuming) {
      this.emit('throttle');
    }
  }
};

Query.prototype.resume = function() {
  var eventBuffer = this._eventBuffer,
      event;

  if (this._paused) {
    this._paused = false;

    if (!this._resuming) {
      this._resuming = true;

      while (!this._paused && (event = eventBuffer.shift())) {
        this.emit(event[0], event[1]);
      }

      this._resuming = false;
      if (!this._paused) {
        this.emit('throttle');
      }
    }
  }
};

Query.FIELD_TYPE_DECIMAL     = 0x00;
Query.FIELD_TYPE_TINY        = 0x01;
Query.FIELD_TYPE_SHORT       = 0x02;
Query.FIELD_TYPE_LONG        = 0x03;
Query.FIELD_TYPE_FLOAT       = 0x04;
Query.FIELD_TYPE_DOUBLE      = 0x05;
Query.FIELD_TYPE_NULL        = 0x06;
Query.FIELD_TYPE_TIMESTAMP   = 0x07;
Query.FIELD_TYPE_LONGLONG    = 0x08;
Query.FIELD_TYPE_INT24       = 0x09;
Query.FIELD_TYPE_DATE        = 0x0a;
Query.FIELD_TYPE_TIME        = 0x0b;
Query.FIELD_TYPE_DATETIME    = 0x0c;
Query.FIELD_TYPE_YEAR        = 0x0d;
Query.FIELD_TYPE_NEWDATE     = 0x0e;
Query.FIELD_TYPE_VARCHAR     = 0x0f;
Query.FIELD_TYPE_BIT         = 0x10;
Query.FIELD_TYPE_NEWDECIMAL  = 0xf6;
Query.FIELD_TYPE_ENUM        = 0xf7;
Query.FIELD_TYPE_SET         = 0xf8;
Query.FIELD_TYPE_TINY_BLOB   = 0xf9;
Query.FIELD_TYPE_MEDIUM_BLOB = 0xfa;
Query.FIELD_TYPE_LONG_BLOB   = 0xfb;
Query.FIELD_TYPE_BLOB        = 0xfc;
Query.FIELD_TYPE_VAR_STRING  = 0xfd;
Query.FIELD_TYPE_STRING      = 0xfe;
Query.FIELD_TYPE_GEOMETRY    = 0xff;
