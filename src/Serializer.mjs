import { EventEmitter } from 'events'
import zlib from 'zlib'
import v8 from 'v8'

export default class Serializer extends EventEmitter {
  constructor(opts = {}) {
    super()
    this.opts = Object.assign({}, opts)
    this.linebreak = Buffer.from([0x0A])
    this.delimiter = Buffer.from([0xFF, 0xFF, 0xFF, 0xFF])
    this.defaultBuffer = Buffer.alloc(4096)
    this.brotliOptions = {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: 4
      }
    }
  }

  async serialize(data, opts={}) {
    let line
    let header = 0x00 // 1 byte de header
    let compressed = false
    if (this.opts.v8 || opts.v8 === true) {
      header |= 0x02 // set V8
      line = v8.serialize(data)
    } else {
      line = Buffer.from(JSON.stringify(data), 'utf-8')
    }
    if (this.opts.compress || opts.compress === true) {
      await new Promise(resolve => {
        zlib.brotliCompress(line, this.brotliOptions, (err, buffer) => {
          if (!err) {
            header |= 0x01 // set compressed
            compressed = true
            line = buffer
          }
          resolve()
        })
      })
    }
    return this.packLineBreaks(line, opts.linebreak !== false, header)
  }  

  async deserialize(data, opts={}) {
    let line
    const header = data.readUInt8(0)
    const valid = header === 0x00 || header === 0x01 || header === 0x02 || header === 0x03
    let isCompressed, isV8, decompresssed
    if(valid) {
      isCompressed = (header & 0x01) === 0x01
      isV8 = (header & 0x02) === 0x02
      line = data.subarray(1) // remove byte header
    } else {
      isCompressed = isV8 = false
      line = data.subarray(0)
    }
    if (isCompressed || !valid) {
      await new Promise(resolve => {
        zlib.brotliDecompress(this.unpackLineBreaks(line), (err, buffer) => {
          if (!err) {
            decompresssed = true
            line = buffer
          }
          resolve()
        })
      })
    }
    if (isV8) {
      try {
        return v8.deserialize(decompresssed ? line : this.unpackLineBreaks(line))
      } catch (e) {
        throw new Error('Failed to deserialize V8 data')
      }
    } else {
      try {
        return JSON.parse(line.toString('utf-8'))
      } catch (e) {
        throw new Error('Failed to deserialize JSON data')
      }
    }
  }

  packLineBreaks(buffer, endWithLineBreak, header) {
    if ((buffer.length + this.delimiter.length) > this.defaultBuffer.length) {
      this.defaultBuffer = Buffer.alloc(buffer.length + this.delimiter.length)
      return this.packLineBreaks(buffer, endWithLineBreak, header)
    }
    let offset = 0
    const positions = []
    for (let i = 0; i < buffer.length; i++) {
      if(i === 0 && typeof(header) !== 'undefined') {
        this.defaultBuffer[0] = header
        offset++
      }
      if (buffer[i] === 0x0A) {
        positions.push(i)
        this.defaultBuffer[offset + i] = 0xFF
      } else {
        this.defaultBuffer[offset + i] = buffer[i]
      }
    }

    if(positions.length === 0) {
      if(endWithLineBreak === true) {
        const totalLength = buffer.length + offset + 1
        const result = Buffer.alloc(totalLength)
        this.defaultBuffer.copy(result, 0)
        result[totalLength - 1] = 0x0A
        return result
      }
      return buffer
    }

    offset += buffer.length
    this.delimiter.copy(this.defaultBuffer, offset)
    offset += this.delimiter.length

    const bytesWritten = this.defaultBuffer.write(JSON.stringify(positions), offset, 'utf-8')
    const totalLength = offset + bytesWritten + (endWithLineBreak ? 1 : 0)
    if (totalLength > this.defaultBuffer.length) { // predict overflowing buffer
      this.defaultBuffer = Buffer.alloc(totalLength)
      return this.packLineBreaks(buffer, endWithLineBreak, header)
    }
    offset += bytesWritten
    if (endWithLineBreak) {
      this.defaultBuffer[offset] = 0x0A
    }
    const result = Buffer.alloc(totalLength)
    this.defaultBuffer.copy(result, 0)
    return result
  }

  unpackLineBreaks(buffer) {
    const delimiterIndex = buffer.lastIndexOf(this.delimiter)
    if (delimiterIndex === -1) return buffer
    let positions
    try {
      positions = JSON.parse(buffer.subarray(delimiterIndex + this.delimiter.length).toString('utf-8'))
    } catch (e) {
      return buffer // not packed
    }
    if (positions.length === 0) return buffer.subarray(0, delimiterIndex)
    const ret = Buffer.alloc(delimiterIndex)
    buffer.copy(ret, 0, 0, delimiterIndex)
    for (let i = 0; i < positions.length; i++) {
      ret[positions[i]] = 0x0A
    }
    return ret
  }

  async safeDeserialize(json) {
    try {
      return await this.deserialize(json)
    } catch (e) {
      return null
    }
  }
}