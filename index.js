var stream   = require('stream');
var util     = require('util');
var strftime = require('strftime');
var OSS      = require('ali-oss');


// Public API

function OssStreamLogger(options) {
  stream.Writable.call(this, options);

  if (!(options.bucket || process.env.BUCKET_NAME))
    throw new Error('options.bucket or BUCKET_NAME environment variable is required');

  this.bucket = options.bucket || process.env.BUCKET_NAME;
  this.folder = options.folder || '';
  this.name_format = options.name_format || '%Y-%m-%d-%H-%M-%S-%L-unknown-unknown.log';
  this.rotate_every = options.rotate_every || 60 * 60 * 1000; // default to 60 minutes
  this.max_file_size = options.max_file_size || 200000; // or 200k, whichever is sooner
  this.upload_every = options.upload_every || 20 * 1000; // default to 20 seconds
  this.buffer_size = options.buffer_size || 10000; // or every 10k, which ever is sooner
  // Backwards compatible API changes

  options.config = options.config || {};
  if (options.endpoint) {
    options.config.endpoint = options.endpoint;
  }
  if (options.access_key_id) {
    options.config.accessKeyId = options.access_key_id;
  }
  if (options.secret_access_key) {
    options.config.accessKeySecret = options.secret_access_key;
  }
  if (options.bucket) {
    options.config.bucket = options.bucket;
  }

  this.oss = new OSS(options.config);
  this.timeout = null;
  this.object_name = null;
  this.file_started = null;
  this.last_write = null;
  this.buffers = [];
  this.unwritten = 0;

  this._newFile();
}
util.inherits(OssStreamLogger, stream.Writable);

// write anything outstanding to the current file, and start a new one
OssStreamLogger.prototype.flushFile = function () {
  this._upload(true);
};

// Private API

OssStreamLogger.prototype._upload = function (forceNewFile) {
  if (this.timeout) {
    clearTimeout(this.timeout);
    this.timeout = null;
  }
  this.last_write = new Date();

  const buffer = Buffer.concat(this.buffers);

  this.unwritten = 0;

  const elapsed = new Date().getTime() - this.file_started.getTime();
  const fileStartedDay = new Date(this.file_started).getDate();

  if (typeof this.rotate_every === 'string' && this.rotate_every === 'day') {
    if (forceNewFile || fileStartedDay !== new Date().getDate() || buffer.length > this.max_file_size) {
      this._newFile();
    }
  } else if (typeof this.rotate_every === 'number') {
    if (forceNewFile || elapsed > this.rotate_every || buffer.length > this.max_file_size) {
      this._newFile();
    }
  }

  // do everything else before calling putObject to avoid the
  // possibility that this._write is called again, losing data.
  this.oss.put(this.object_name, buffer);
};

// _newFile should ONLY be called when there is no un-uploaded data (i.e.
// from _upload or initialization), otherwise data will be lost.
OssStreamLogger.prototype._newFile = function () {
  this.buffers = [];
  this.file_started = new Date();
  this.last_write = this.file_started;
  // create a date object with the UTC version of the date to use with
  // strftime, so that the commonly use formatters return the UTC values.
  // This breaks timezone-converting specifiers (as they will convert against
  // the wrong timezone).
  const date_as_utc = new Date(
    this.file_started.getUTCFullYear(),
    this.file_started.getUTCMonth(),
    this.file_started.getUTCDate(),
    this.file_started.getUTCHours(),
    this.file_started.getUTCMinutes(),
    this.file_started.getUTCSeconds(),
    this.file_started.getUTCMilliseconds(),
  );
  this.object_name = (this.folder === '' ? '' : this.folder + '/') + strftime(this.name_format, date_as_utc);
};

OssStreamLogger.prototype._write = function (chunk, encoding, cb) {
  if (typeof chunk === 'string') chunk = new Buffer(chunk, encoding);

  if (chunk) {
    this.buffers.push(chunk);
    this.unwritten += chunk.length;
  }

  if (this.timeout) {
    clearTimeout(this.timeout);
    this.timeout = null;
  }

  if (new Date().getTime() - this.last_write.getTime() > this.upload_every || this.unwritten > this.buffer_size) {
    this._upload();
  } else {
    this.timeout = setTimeout(
      function () {
        this._upload();
      }.bind(this),
      this.upload_every,
    );
  }

  // Call the callback immediately, as we may not actually write for some
  // time. If there is an upload error, we trigger our 'error' event.
  if (cb && typeof cb === 'function') setImmediate(cb);
};

module.exports = {
  OssStreamLogger: OssStreamLogger
};
