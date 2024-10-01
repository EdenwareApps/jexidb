import { EventEmitter } from 'events'
import zlib from 'zlib'
import v8 from 'v8'

export default class Serializer extends EventEmitter {  
  constructor(opts={}) {
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
    if(this.opts.v8) {
      let compressed
      line = v8.serialize(data)
      if(this.opts.compress || opts.compress === true) {
        await new Promise(resolve => {
          zlib.brotliCompress(line, this.brotliOptions, (err, buffer) => {
            if(!err) {
              compressed = true
              line = this.packLineBreaks(buffer, opts.linebreak !== false)
            }
            resolve()
          })
        })
      }
      if(!compressed && opts.linebreak !== false) {
        line.copy(this.defaultBuffer)
        this.linebreak.copy(this.defaultBuffer, line.length)
        line = this.defaultBuffer.subarray(0, line.length + this.linebreak.length)
      }
    } else {
      let compressed
      line = JSON.stringify(data)
      if(this.opts.compress || opts.compress === true) {
        await new Promise(resolve => {
          zlib.brotliCompress(line, this.brotliOptions, (err, buffer) => {
            if(!err) {
              compressed = true
              line = this.packLineBreaks(buffer, opts.linebreak !== false)
            }
            resolve()
          })
        })
      }
      if(!compressed && opts.linebreak !== false) {
        line += '\n'
      }
    }
    return line
  }  

  async deserialize(data, opts={}) {
    let line, removedLineBreak
    if(this.opts.compress || opts.compress === true) {
      await new Promise(resolve => {
        const n = this.unpackLineBreaks(data)
        zlib.brotliDecompress(n, (err, buffer) => {
          if(!err) {
            removedLineBreak = true
            data = buffer
          }
          resolve()
        })
      })
    }
    if(this.opts.v8) {
      if(removedLineBreak !== true)  {
        data = data.subarray(0, -1)
      }
      line = v8.deserialize(data)
    } else {
      if(Buffer.isBuffer(data)) {
        data = data.toString('utf-8')
      }
      line = JSON.parse(data)
    }
    return line
  }

  packLineBreaks(buffer, endWithLineBreak) {
    if(buffer.length > (this.defaultBuffer.length + this.delimiter.length)) {
      this.defaultBuffer = Buffer.alloc(buffer.length)
      return this.packLineBreaks(buffer, endWithLineBreak)
    }
    let offset = 0
    const positions = []
    for (let i = 0; i < buffer.length; i++) {
      if (buffer[i] === 0x0A) {
        positions.push(i)
      } else {
        this.defaultBuffer[offset++] = buffer[i]
      }
    }
    this.delimiter.copy(this.defaultBuffer, offset)
    offset += this.delimiter.length

    const bytesWritten = this.defaultBuffer.write(JSON.stringify(positions), offset, 'utf-8')
    const totalLength = offset + bytesWritten + (endWithLineBreak ? 1 : 0)    
    if(totalLength > this.defaultBuffer.length) { // abort and retry if the buffer will overflown
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
    const positions = JSON.parse(buffer.subarray(delimiterIndex + this.delimiter.length).toString('utf-8'))
    if (positions.length === 0) return buffer.subarray(0, delimiterIndex)
    let offset = delimiterIndex
    let posIndex = positions.length - 1
    for (let i = delimiterIndex - 1; i >= 0; i--) {
      buffer[offset--] = buffer[i]
      if (posIndex >= 0 && i === positions[posIndex]) { // Insert the line break if the position is found
        buffer[offset--] = 0x0A
        posIndex--
      }
    }
    const start = offset + 1, end = delimiterIndex + positions.length + 1
    const result = Buffer.alloc(end - start)
    buffer.copy(result, 0, start)
    return result
  }

  async safeDeserialize(json) {
    try {
      return await this.deserialize(json)
    } catch (e) {
      return null
    }
  }
}