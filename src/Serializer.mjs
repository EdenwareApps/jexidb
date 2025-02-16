import { EventEmitter } from 'events'
import zlib from 'zlib'
import v8 from 'v8'

export default class Serializer extends EventEmitter {
  constructor(opts = {}) {
    super()
    this.opts = Object.assign({}, opts)
    this.brotliOptions = {
      params: {
        [zlib.constants.BROTLI_PARAM_QUALITY]: 4
      }
    }
  }

  async serialize(data, opts={}) {
    let line
    let header = 0x00 // 1 byte de header
    const useV8 = (this.opts.v8 && opts.json !== true) || opts.v8 === true
    const compress = (this.opts.compress && opts.compress !== false) || opts.compress === true
    const addLinebreak = opts.linebreak !== false || (!useV8 && !compress && opts.linebreak !== false)
    if (useV8) {
      header |= 0x02 // set V8
      line = v8.serialize(data)
    } else {
      const json = JSON.stringify(data)
      line = Buffer.from(json, 'utf-8')
    }
    if (compress) {
      let err
      const compressionType = useV8 ? 'deflate' : 'brotli'
      const buffer = await this.compress(line, compressionType).catch(e => err = e)
      if(!err && buffer.length && buffer.length < line.length) {
        header |= 0x01
        line = buffer
      }
    }
    const totalLength = 1 + line.length + (addLinebreak ? 1 : 0)
    const result = Buffer.alloc(totalLength)
    result[0] = header
    line.copy(result, 1, 0, line.length)
    if(addLinebreak) {
      result[result.length - 1] = 0x0A
    }
    return result
  }  

  async deserialize(data) {
    let line, isCompressed, isV8
    const header = data.readUInt8(0)
    const valid = header === 0x00 || header === 0x01 || header === 0x02 || header === 0x03
    if(valid) {
      isCompressed = (header & 0x01) === 0x01
      isV8 = (header & 0x02) === 0x02
      line = data.subarray(1) // remove byte header
    } else {
      isCompressed = isV8 = false
      try {
        return JSON.parse(data.toString('utf-8').trim())
      } catch (e) {
        throw new Error('Failed to deserialize legacy JSON data')
      }
    }
    if (isCompressed) {
      const compressionType = isV8 ? 'deflate' : 'brotli'
      line = await this.decompress(line, compressionType).catch(e => err = e)
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
        throw new Error('Failed to deserialize JSON data')
      }
    }
  }

  compress(data, type) {
    return new Promise((resolve, reject) => {
      const callback = (err, buffer) => {
        if (err) {
          reject(err)
        } else {
          resolve(buffer)
        }
      }
      if(type === 'brotli') {
        zlib.brotliCompress(data, this.brotliOptions, callback)
      } else {
        zlib.deflate(data, callback)
      }
    })
  }

  decompress(data, type) {
    return new Promise((resolve, reject) => {
      const callback = (err, buffer) => {
        if (err) {
          reject(err)
        } else {
          resolve(buffer)
        }
      }
      if(type === 'brotli') {
        zlib.brotliDecompress(data, callback)
      } else {
        zlib.inflate(data, callback)
      }
    })
  }
  
}