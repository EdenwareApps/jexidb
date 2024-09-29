import { EventEmitter } from 'events'
import zlib from 'zlib'
import v8 from 'v8'

export default class Serializer extends EventEmitter {
  constructor() {
    super()
  }
    
  async serialize(data, opts={}) {
    let line
    if(this.opts.v8) {
      line = v8.serialize(data)
      if(opts.compress === true) {
        await new Promise(resolve => {
          zlib.brotliCompress(line, (err, buffer) => {
            if(!err) {
              line = buffer
            }
            resolve()
          })
        })
      }
      if(opts.nl !== false) {
        line = Buffer.concat([line, Buffer.from('\n')])
      }
    } else {
      line = JSON.stringify(data)
      if(opts.compress === true) {
        await new Promise(resolve => {
          zlib.brotliCompress(line, (err, buffer) => {
            if(!err) {
              line = buffer
            }
            resolve()
          })
        })
      }
      if(opts.nl !== false) {
        line += '\n'
      }
    }
    return line
  }

  async deserialize(data, opts={}) {
    let line, nlr
    if(opts.compress === true) {
      await new Promise(resolve => {
        console.log('will unzip', data.length)
        zlib.brotliDecompress(data.slice(0, -1), (err, buffer) => {
          console.log({err, buffer})
          if(!err) {
            nlr = true
            data = buffer
          }
          resolve()
        })
      })
    }
    if(this.opts.v8) {
      if(nlr !== true)  {
        data = data.slice(0, -1)
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

  async safeDeserialize(json) {
    try {
      return await this.deserialize(json)
    } catch (e) {
      return null
    }
  }
}