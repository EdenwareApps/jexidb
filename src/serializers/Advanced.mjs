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
    const useV8 = this.opts.v8 || opts.v8 === true
    const compress = this.opts.compress || opts.compress === true
    if (useV8) {
      header |= 0x02 // set V8
      line = v8.serialize(data)
    } else {
      if(compress) {
        line = Buffer.from(JSON.stringify(data), 'utf-8')
      } else {
        return Buffer.from(JSON.stringify(data) + (opts.linebreak !== false ? '\n' : ''), 'utf-8')
      }
    }
    if (compress) {
      let err
      const buffer = await this.compress(line).catch(e => err = e)
      if(!err) {
        header |= 0x01
        line = buffer
      }
    }
    const totalLength = 1 + line.length + (opts.linebreak !== false ? 1 : 0)
    const result = Buffer.alloc(totalLength)
    result[0] = header
    line.copy(result, 1)
    if (opts.linebreak !== false) {
      result[totalLength - 1] = 0x0A
    }
    return result
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
      line = data
    }
    if (isCompressed) {
      let err
      const buffer = await this.decompress(line).catch(e => err = e)
      if(!err) {
        decompresssed = true
        line = buffer
      }
    }
    if (isV8) {
      try {
        return v8.deserialize(line)
      } catch (e) {
        throw new Error('Failed to deserialize V8 data')
      }
    } else {
      try {
        return JSON.parse(line.toString('utf-8').trim())
      } catch (e) {
        console.error('Failed to deserialize', header, line.toString('utf-8').trim())
        throw new Error('Failed to deserialize JSON data')
      }
    }
  }

  compress(data) {
    return new Promise((resolve, reject) => {
      zlib.brotliCompress(data, this.brotliOptions, (err, buffer) => {
        if (err) {
          reject(err)
        } else {
          resolve(buffer)
        }
      })
    })
  }

  decompress(data) {
    return new Promise((resolve, reject) => {
      zlib.brotliDecompress(data, (err, buffer) => {
        if (err) {
          reject(err)
        } else {
          resolve(buffer)
        }
      })
    })
  }

  async safeDeserialize(json) {
    try {
      return await this.deserialize(json)
    } catch (e) {
      return null
    }
  }
}