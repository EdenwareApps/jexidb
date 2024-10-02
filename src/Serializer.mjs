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

  async serialize(data, opts = {}) {
    let line
    let header = 0x00 // 1 byte de header
    let compressed = false

    if (this.opts.v8) {
      header |= 0x02 // Indica V8
      line = v8.serialize(data)
    } else {
      line = Buffer.from(JSON.stringify(data), 'utf-8')
    }

    if (this.opts.compress || opts.compress === true) {
      await new Promise(resolve => {
        zlib.brotliCompress(line, this.brotliOptions, (err, buffer) => {
          if (!err) {
            compressed = true
            line = buffer
          }
          resolve()
        })
      })
      header |= 0x01 // Indica compressão
    }

    line = this.packLineBreaks(line, opts.linebreak !== false)

    // Prepara o buffer final com o header no início
    const finalBuffer = Buffer.concat([Buffer.from([header]), line])

    return finalBuffer
  }

  async deserialize(data, opts = {}) {
    let header, isCompressed = false, isV8 = false
    if (data[0] === 0x00 || data[0] === 0x01 || data[0] === 0x02 || data[0] === 0x03) { // has valid header
      header = data[0]
      isCompressed = (header & 0x01) === 0x01
      isV8 = (header & 0x02) === 0x02
      data = data.slice(1)
    } else {
      // presume it is not compressed and not V8
      isCompressed = false
      isV8 = false
    }
    let line = data
    if (isCompressed) {
      await new Promise(resolve => {
        zlib.brotliDecompress(this.unpackLineBreaks(line), (err, buffer) => {
          if (!err) {
            line = buffer
          }
          resolve()
        })
      })
    }

    if (isV8) {
      try {
        return v8.deserialize(this.unpackLineBreaks(line))
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


  packLineBreaks(buffer, endWithLineBreak) {
    if (buffer.length > (this.defaultBuffer.length + this.delimiter.length)) {
      this.defaultBuffer = Buffer.alloc(buffer.length)
      return this.packLineBreaks(buffer, endWithLineBreak)
    }
    const positions = []
    for (let i = 0; i < buffer.length; i++) {
      if (buffer[i] === 0x0A) {
        positions.push(i)
        this.defaultBuffer[i] = 0xFF
      } else {
        this.defaultBuffer[i] = buffer[i]
      }
    }
    this.delimiter.copy(this.defaultBuffer, buffer.length)
    let offset = buffer.length + this.delimiter.length

    const bytesWritten = this.defaultBuffer.write(JSON.stringify(positions), offset, 'utf-8')
    const totalLength = offset + bytesWritten + (endWithLineBreak ? 1 : 0)
    if (totalLength > this.defaultBuffer.length) { // predict overflowing buffer
      this.defaultBuffer = Buffer.alloc(totalLength)
      return this.packLineBreaks(buffer, endWithLineBreak)
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
    for (let i = 0; i < positions.length; i++) {
      buffer[positions[i]] = 0x0A
    }
    return buffer.slice(0, delimiterIndex)
  }

  async safeDeserialize(json) {
    try {
      return await this.deserialize(json)
    } catch (e) {
      return null
    }
  }
}