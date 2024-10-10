export default class Serializer {

  constructor(opts={}) {
    this.opts = Object.assign({}, opts)
  }

  async serialize(data, opts={}) {
    return Buffer.from(JSON.stringify(data) + (opts.linebreak !== false ? '\n' : ''), 'utf-8')
  }  

  async deserialize(data, opts={}) {
    const line = data.toString('utf-8')
    try {
      return JSON.parse(line)
    } catch (e) {
      console.error('Failed to deserialize', line)
      throw new Error('Failed to deserialize JSON data')
    }
  }
  
}