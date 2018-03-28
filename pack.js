var constants = require('constants')
var eos = require('end-of-stream')
var util = require('util')

var Readable = require('readable-stream').Readable
var Writable = require('readable-stream').Writable
var StringDecoder = require('string_decoder').StringDecoder

var headers = require('./headers')

var DMODE = parseInt('755', 8)
var FMODE = parseInt('644', 8)

var END_OF_TAR = new Buffer(1024)
END_OF_TAR.fill(0)

var noop = function () {}

var overflow = function (self, size) {
  size &= 511
  if (size) self.push(END_OF_TAR.slice(0, 512 - size))
}

function modeToType (mode) {
  switch (mode & constants.S_IFMT) {
    case constants.S_IFBLK: return 'block-device'
    case constants.S_IFCHR: return 'character-device'
    case constants.S_IFDIR: return 'directory'
    case constants.S_IFIFO: return 'fifo'
    case constants.S_IFLNK: return 'symlink'
  }

  return 'file'
}

var Sink = function (to) {
  Writable.call(this)
  this.written = 0
  this._to = to
  this._destroyed = false
}

util.inherits(Sink, Writable)

Sink.prototype._write = function (data, enc, cb) {
  this.written += data.length
  if (this._to.push(data)) return cb()
  this._to._drain = cb
}

Sink.prototype.destroy = function () {
  if (this._destroyed) return
  this._destroyed = true
  this.emit('close')
}

var LinkSink = function () {
  Writable.call(this)
  this.linkname = ''
  this._decoder = new StringDecoder('utf-8')
  this._destroyed = false
}

util.inherits(LinkSink, Writable)

LinkSink.prototype._write = function (data, enc, cb) {
  this.linkname += this._decoder.write(data)
  cb()
}

LinkSink.prototype.destroy = function () {
  if (this._destroyed) return
  this._destroyed = true
  this.emit('close')
}

var Void = function () {
  Writable.call(this)
  this._destroyed = false
}

util.inherits(Void, Writable)

Void.prototype._write = function (data, enc, cb) {
  cb(new Error('No body allowed for this entry'))
}

Void.prototype.destroy = function () {
  if (this._destroyed) return
  this._destroyed = true
  this.emit('close')
}

var PackJob = function (packer, paused) {
  this._packer = packer
  this._paused = paused
  this._drain = noop
  this._stream = null
  this._buffers = []
}

PackJob.prototype.push = function (buffer) {
  if (this._paused) {
    this._buffers.push(buffer)
    return false
  }

  return this._packer.push(buffer)
}

PackJob.prototype.unpause = function () {
  if (!this._paused) return

  this.write_buffers()
  this._paused = false

  if (this._stream) {
    var drain = this._drain
    this._drain = noop
    drain()
    return
  }

  this._packer._next_job()
}

PackJob.prototype.write_buffers = function () {
  for (var i = 0; i < this._buffers.length; ++i) {
    this._packer.push(this._buffers[i])
  }
  this._buffers = null
}

var Pack = function (opts) {
  if (!(this instanceof Pack)) return new Pack(opts)
  Readable.call(this, opts)

  this._finalized = false
  this._finalizing = false
  this._finalizing_cb = noop
  this._destroyed = false
  this._jobs = []
}

util.inherits(Pack, Readable)

Pack.prototype.entry = function (header, buffer, callback) {
  if (this._finalized || this._destroyed) return

  var paused = !!this._jobs.length
  var job = new PackJob(this, paused)
  this._jobs.push(job)
  return job.entry(header, buffer, callback)
}

PackJob.prototype.entry = function (header, buffer, callback) {
  if (typeof buffer === 'function') {
    callback = buffer
    buffer = null
  }

  if (!callback) callback = noop

  var self = this

  if (!header.size || header.type === 'symlink') header.size = 0
  if (!header.type) header.type = modeToType(header.mode)
  if (!header.mode) header.mode = header.type === 'directory' ? DMODE : FMODE
  if (!header.uid) header.uid = 0
  if (!header.gid) header.gid = 0
  if (!header.mtime) header.mtime = new Date()

  if (typeof buffer === 'string') buffer = new Buffer(buffer)
  if (Buffer.isBuffer(buffer)) {
    header.size = buffer.length
    this._encode(header)
    this.push(buffer)
    overflow(self, header.size)
    if (!this._paused) self._packer._next_job()
    process.nextTick(callback)
    return new Void()
  }

  if (header.type === 'symlink' && !header.linkname) {
    var linkSink = new LinkSink()
    this._stream = linkSink
    eos(linkSink, function (err) {
      self._stream = null
      if (err) { // stream was closed
        self._packer.destroy()
        return callback(err)
      }

      header.linkname = linkSink.linkname
      self._encode(header)
      if (!self._paused) self._packer._next_job()
      callback()
    })

    return linkSink
  }

  this._encode(header)

  if (header.type !== 'file' && header.type !== 'contiguous-file') {
    if (!this._paused) self._packer._next_job()
    process.nextTick(callback)
    return new Void()
  }

  var sink = new Sink(this)

  this._stream = sink

  eos(sink, function (err) {
    self._stream = null

    if (err) { // stream was closed
      self._packer.destroy()
      return callback(err)
    }

    if (sink.written !== header.size) { // corrupting tar
      self._packer.destroy()
      return callback(new Error('size mismatch'))
    }

    overflow(self, header.size)
    if (!self._paused) self._packer._next_job()
    callback()
  })

  return sink
}

Pack.prototype.finalize = function (callback) {
  if (!callback) callback = noop

  if (this._destroyed) return callback(new Error('stream is destroyed'))
  if (this._finalized) return callback(new Error('already finalized'))

  if (this._jobs.length) {
    this._finalizing = true
    this._finalizing_cb = callback
    return
  }

  this._finalized = true
  this.push(END_OF_TAR)
  this.push(null)
  callback()
}

Pack.prototype.destroy = function (err) {
  if (this._destroyed) return
  this._destroyed = true

  if (err) this.emit('error', err)
  this.emit('close')
  for (var i = 0; i < this._jobs.length; ++i) {
    var job = this._jobs[i]
    if (job._stream && job._stream.destroy) job._stream.destroy()
  }
}

PackJob.prototype._encode = function (header) {
  if (!header.pax) {
    var buf = headers.encode(header)
    if (buf) {
      this.push(buf)
      return
    }
  }
  this._encodePax(header)
}

PackJob.prototype._encodePax = function (header) {
  var paxHeader = headers.encodePax({
    name: header.name,
    linkname: header.linkname,
    pax: header.pax
  })

  var newHeader = {
    name: 'PaxHeader',
    mode: header.mode,
    uid: header.uid,
    gid: header.gid,
    size: paxHeader.length,
    mtime: header.mtime,
    type: 'pax-header',
    linkname: header.linkname && 'PaxHeader',
    uname: header.uname,
    gname: header.gname,
    devmajor: header.devmajor,
    devminor: header.devminor
  }

  this.push(headers.encode(newHeader))
  this.push(paxHeader)
  overflow(this, paxHeader.length)

  newHeader.size = header.size
  newHeader.type = header.type
  this.push(headers.encode(newHeader))
}

Pack.prototype._read = function (n) {
  if (this._jobs[0]) {
    var drain = this._jobs[0]._drain
    this._jobs[0]._drain = noop
    drain()
  }
}

Pack.prototype._next_job = function () {
  this._jobs.shift()
  if (this._jobs.length) return this._jobs[0].unpause()
  if (this._finalizing) {
    var cb = this._finalizing_cb
    this._finalizing_cb = noop
    this.finalize(cb)
  }
}

module.exports = Pack
