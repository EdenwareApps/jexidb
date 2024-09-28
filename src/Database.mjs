import FileHandler from './FileHandler.mjs'
import IndexManager from './IndexManager.mjs'
import { EventEmitter } from 'events'
// import v8 from 'v8'

export class Database extends EventEmitter {
  constructor(filePath, opts={}) {
    super()
    this.opts = Object.assign({
      indexes: {}
      /*
      serializer: {
        parse: b => {
          if(!Buffer.isBuffer(b)) {
            b = Buffer.from(b)
          }
          return v8.deserialize(b)
        },
        stringify: v8.serialize
      }
      serializer: JSON
      */
    }, opts)
    this.shouldSave = false
    this.serialize = JSON.stringify.bind(JSON)
    this.deserialize = JSON.parse.bind(JSON)
    this.fileHandler = new FileHandler(filePath)
    this.indexManager = new IndexManager(this.opts)
    this.indexOffset = 0
    //this.exitListener = this.saveSync.bind(this)
    //process.on('exit', this.exitListener) //code => { console.log('Processo está saindo com o código:', code);
  }

  use(plugin) {
    plugin(this)
  }

  safeDeserialize(json) {
    try {
      return this.deserialize(json)
    } catch (e) {                
      console.error(e)
      return null
    }
  }

  async init() {
    try {
      const lastLine = await this.fileHandler.readLastLine()
      const offsets = this.deserialize(lastLine)
      this.indexOffset = offsets[offsets.length - 2]
      this.shouldTruncate = true
      this.offsets = offsets.slice(0, -2)
      const indexLine = await this.fileHandler.readRange(...this.locate(this.offsets.length - 2))
      const index = this.deserialize(indexLine)
      this.indexManager.index = index
    } catch (e) {
      this.offsets = []
      this.indexOffset = 0
      console.error('Error loading database:', e)
    }
    this.initialized = true
    this.emit('init')
  }

  async save() {
    const index = this.indexManager.index
    for(const field in index.data) {
        for(const term in index.data[field]) {
            index.data[field][term] = [...index.data[field][term]] // set to array
        }
    }
    const offsets = this.offsets.slice(0)
    const indexString = Buffer.from(this.serialize(index) +'\n')
    offsets.push(this.indexOffset)
    offsets.push(this.indexOffset + indexString.length)
    const offsetsString = Buffer.from(this.serialize(offsets))
    if (this.shouldTruncate) {
        await this.fileHandler.truncate(this.indexOffset)
        this.shouldTruncate = false
    }
    await this.fileHandler.writeData(indexString)
    await this.fileHandler.writeData(offsetsString, true)
    this.shouldSave = false
  }

  async ready() {
    if (!this.initialized) {
      await new Promise(resolve => this.once('init', resolve))
    }
  }

  locate(n) {
    const ret = {}
    if (!this.offsets[n]) throw new Error(`Invalid line map at position ${n}`);
    return [this.offsets[n], this.offsets[n + 1] || Number.MAX_SAFE_INTEGER]
  }
  
  getRanges(map) {
    return map.map(n => {
        if(this.offsets[n] === undefined) return
        const end = this.offsets[n + 1] ? (this.offsets[n + 1] - 1) : this.indexOffset
        console.log('getRanges', {n, start: this.offsets[n], offset: this.indexOffset, next: this.offsets[n+1], end})
        return {
          start: this.offsets[n],
          end
        }
    }).filter(n => n !== undefined)
  }

  async readLines(map) {
    console.log('map', map, this.offsets)
    const ranges = this.getRanges(map)
    console.log('ranges', ranges)
    const lines = await this.fileHandler.readRanges(ranges)
    console.log('lines', lines)
    return Object.values(lines).map(l => this.safeDeserialize(l)).filter(s => s)
  }

  async insert(data) {
    const position = this.offsets.length
    const line = Buffer.from(this.serialize(data) +'\n') // using Buffer for offsets accuracy
    if (this.shouldTruncate) {
        await this.fileHandler.truncate(this.indexOffset)
        this.shouldTruncate = false
    }
    await this.fileHandler.writeData(line)
    this.offsets.push(this.indexOffset)
    this.indexOffset += line.length
    this.indexManager.add(data, position)
    this.shouldSave = true
  }

  async *iterate(map, options={}) {
    if(this.indexOffset === 0) return
    if(!Array.isArray(map)) {
      map = this.indexManager.query(map, options.matchAny)
    }
    const rl = this.fileHandler.iterate(map)
    for await (const line of rl) {
      const e = this.safeDeserialize(line)
      e && (yield e)
    }
  }
       
  async query(criteria, options={}) {
    if(Array.isArray(criteria)) {
      let results = await this.readLines(criteria)
      if (options.orderBy) {
          const [field, direction = 'asc'] = options.orderBy.split(' ')
          results.sort((a, b) => {
              if (a[field] > b[field]) return direction === 'asc' ? 1 : -1
              if (a[field] < b[field]) return direction === 'asc' ? -1 : 1
              return 0;
          })
      }
      if (options.limit) {
          results = results.slice(0, options.limit);
      }
      return results
    } else {
      const matchingLines = await this.indexManager.query(criteria, options.matchAny)
      if (!matchingLines || !matchingLines.size) {
          return []
      }
      return await this.query([...matchingLines], options)
    }
  }

  async update(criteria, data, options={}) {
    const matchingLines = await this.indexManager.query(criteria)
    if (!matchingLines || !matchingLines.size) {
        return []
    }
    const ranges = this.getRanges([...matchingLines])
    const entries = await this.readLines([...matchingLines])
    const lines = entries.map(entry => Object.assign(entry, data)).map(e => Buffer.from(this.serialize(e) +'\n'))
    const offsets = []
    let byteOffset = 0, k = 0
    this.offsets.forEach((n, i) => {
      const prevByteOffset = byteOffset
      if (matchingLines.has(i)) {
        const r = ranges[k]
        byteOffset += lines[k].length - (r.end - r.start) - 1
        k++
      }
      offsets.push(n + prevByteOffset)
    })
    this.offsets = offsets
    this.indexOffset += byteOffset
    console.log('replacingd', ranges, JSON.stringify(lines.map(b => String(b))))
    await this.fileHandler.replaceLines(ranges, lines);
    [...matchingLines].forEach((lineNumber, i) => this.indexManager.add(entries[i], lineNumber))
    this.shouldSave = true
    return entries
  }

  async delete(criteria, options={}) {
    console.log('delete')
    const matchingLines = await this.indexManager.query(criteria)
    if (!matchingLines || !matchingLines.size) {
        return 0
    }
    const ranges = this.getRanges([...matchingLines])
    await this.fileHandler.replaceLines(ranges, [])
    const replaces = new Map()
    const offsets = []
    let positionOffset = 0, byteOffset = 0, k = 0
    this.offsets.forEach((n, i) => {
      let skip
      if (matchingLines.has(i)) {
        const r = ranges[k]
        positionOffset--
        byteOffset -= (r.end - r.start) + 1
        console.log({byteOffset, positionOffset})
        k++
        skip = true
      } else {
        if(positionOffset !== 0) {
          replaces.set(n, n + positionOffset)
        }
        offsets.push(n + byteOffset)
      }
    })
    console.log('offsets~', {offsets, previous: this.offsets})
    this.offsets = offsets
    this.indexOffset += byteOffset
    this.indexManager.replace(replaces)
    this.shouldSave = true
    return ranges.length
  }

  async destroy() {
    this.shouldSave && await this.save()
    this.indexOffset = 0
    this.indexManager.index = {}
    this.initialized = false
    this.fileHandler.destroy()
  }

}
