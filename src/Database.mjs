import FileHandler from './FileHandler.mjs'
import IndexManager from './IndexManager.mjs'
import Serializer from './Serializer.mjs'

export class Database extends Serializer {
  constructor(filePath, opts={}) {
    super()
    this.opts = Object.assign({
      v8: false,
      indexes: {},
      compress: false,
      compressIndex: false
    }, opts)
    this.shouldSave = false
    this.serializer = new Serializer(this.opts)
    this.fileHandler = new FileHandler(filePath)
    this.indexManager = new IndexManager(this.opts)
    this.indexOffset = 0
  }

  use(plugin) {
    plugin(this)
  }

  async init() {
    try {
      if(this.opts.clear) {
        await this.fileHandler.truncate(0).catch(console.error)
        throw new Error('Cleared, empty file')
      }
      const lastLine = await this.fileHandler.readLastLine()
      if(!lastLine || !lastLine.length) {
        throw new Error('File does not exists or is a empty file')
      }
      const offsets = await this.serializer.deserialize(lastLine, {compress: this.opts.compressIndex})
      if(!Array.isArray(offsets)) {
        throw new Error('File to parse offsets, expected an array')
      }
      this.indexOffset = offsets[offsets.length - 2]
      this.offsets = offsets
      const ptr = this.locate(offsets.length - 2)
      this.offsets = this.offsets.slice(0, -2)
      this.shouldTruncate = true
      let indexLine = await this.fileHandler.readRange(...ptr)
      const index = await this.serializer.deserialize(indexLine, {compress: this.opts.compressIndex})
      if(index) {
        this.indexManager.index = index
        if (!this.indexManager.index.data) {
          this.indexManager.index.data = {}
        }
      }
    } catch (e) {
      if(!this.offsets) {
        this.offsets = []
      }
      this.indexOffset = 0
      if(!String(e).includes('empty file')) {
        console.error('Error loading database:', e)
      }
    }
    this.initialized = true
    this.emit('init')
  }

  async save() {
    this.emit('before-save')
    const index = {data: {}}
    for(const field in this.indexManager.index.data) {
      if(typeof(index.data[field]) === 'undefined') index.data[field] = {}
      for(const term in this.indexManager.index.data[field]) {
        if(typeof(index.data[field][term]) === 'undefined') index.data[field][term] = {}
        index.data[field][term] = [...this.indexManager.index.data[field][term]] // set to array 
      }
    }
    const offsets = this.offsets.slice(0)
    const indexString = await this.serializer.serialize(index, {compress: this.opts.compressIndex})
    offsets.push(this.indexOffset)
    offsets.push(this.indexOffset + indexString.length)
    const offsetsString = await this.serializer.serialize(offsets, {compress: this.opts.compressIndex, linebreak: false})
    if (this.shouldTruncate) {
        await this.fileHandler.truncate(this.indexOffset)
        this.shouldTruncate = false
    }
    await this.fileHandler.writeData(indexString)
    await this.fileHandler.writeData(offsetsString, true)
    this.shouldTruncate = true
    this.shouldSave = false
  }

  async ready() {
    if (!this.initialized) {
      await new Promise(resolve => this.once('init', resolve))
    }
  }

  locate(n) {
    if (this.offsets[n] === undefined) {
      if(this.offsets[n - 1]) {
        return [this.indexOffset, Number.MAX_SAFE_INTEGER]
      }
      return
    }
    let end = this.offsets[n + 1] || this.indexOffset || Number.MAX_SAFE_INTEGER
    return [this.offsets[n], end]
  }
  
  getRanges(map) {
    return map.map(n => {
        const ret = this.locate(n)
        if(ret !== undefined) return {start: ret[0], end: ret[1], index: n}
    }).filter(n => n !== undefined)
  }

  async readLines(map, ranges) {
    if(!ranges) {
      ranges = this.getRanges(map)
    }
    const results = []
    const lines = await this.fileHandler.readRanges(ranges)
    for(const l of Object.values(lines)) {
      let err
      const ret = await this.serializer.safeDeserialize(l).catch(e => err = e)
      err || results.push(ret)
    }
    return results
  }

  async insert(data) {
    const position = this.offsets.length
    const line = await this.serializer.serialize(data, {compress: this.opts.compress}) // using Buffer for offsets accuracy
    if (this.shouldTruncate) {
        await this.fileHandler.truncate(this.indexOffset)
        this.shouldTruncate = false
    }
    await this.fileHandler.writeData(line)
    this.offsets.push(this.indexOffset)
    this.indexOffset += line.length
    this.indexManager.add(data, position)
    this.shouldSave = true
    this.emit('insert', data, position)
  }

  async *walk(map, options={}) {
    if(this.indexOffset === 0) return
    if(!Array.isArray(map)) {
      if(map && typeof map === 'object') {
        map = this.indexManager.query(map, options.matchAny)
      } else {
        map = [...Array(this.offsets.length).keys()]
      }
    }
    const ranges = this.getRanges(map)
    for await (const line of this.fileHandler.walk(ranges)) {
      let err
      const e = await this.serializer.safeDeserialize(line).catch(e => err = e)
      err || (yield e)
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
    const matchingLines = await this.indexManager.query(criteria, options.matchAny)
    if (!matchingLines || !matchingLines.size) {
        return []
    }
    const ranges = this.getRanges([...matchingLines])
    const validMatchingLines = new Set(ranges.map(r => r.index))
    if (!validMatchingLines.size) {
      return []
    }
    const entries = await this.readLines([...validMatchingLines], ranges)
    const lines = []
    for(const entry of entries) {
      let err
      const updated = Object.assign(entry, data)
      const ret = await this.serializer.serialize(updated).catch(e => err = e)
      err || lines.push(ret)
    }
    const offsets = []
    let byteOffset = 0, k = 0
    this.offsets.forEach((n, i) => {
      const prevByteOffset = byteOffset
      if (validMatchingLines.has(i) && ranges[k]) {
        const r = ranges[k]
        byteOffset += lines[k].length - (r.end - r.start)
        k++
      }
      offsets.push(n + prevByteOffset)
    })
    this.offsets = offsets
    this.indexOffset += byteOffset
    await this.fileHandler.replaceLines(ranges, lines);
    [...validMatchingLines].forEach((lineNumber, i) => this.indexManager.add(entries[i], lineNumber))
    this.shouldSave = true
    return entries
  }

  async delete(criteria, options={}) {
    const matchingLines = await this.indexManager.query(criteria, options.matchAny)
    if (!matchingLines || !matchingLines.size) {
        return 0
    }
    const ranges = this.getRanges([...matchingLines])
    const validMatchingLines = new Set(ranges.map(r => r.index))
    await this.fileHandler.replaceLines(ranges, [])
    const replaces = new Map()
    const offsets = []
    let positionOffset = 0, byteOffset = 0, k = 0
    this.offsets.forEach((n, i) => {
      let skip
      if (validMatchingLines.has(i)) {
        const r = ranges[k]
        positionOffset--
        byteOffset -= (r.end - r.start)
        k++
        skip = true
      } else {
        if(positionOffset !== 0) {
          replaces.set(n, n + positionOffset)
        }
        offsets.push(n + byteOffset)
      }
    })
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

  get length() {
    return this.offsets.length
  }

  get index() {
    return this.indexManager.index
  }

}
