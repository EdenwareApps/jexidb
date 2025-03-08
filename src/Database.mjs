import { EventEmitter } from 'events'
import FileHandler from './FileHandler.mjs'
import IndexManager from './IndexManager.mjs'
import Serializer from './Serializer.mjs'
import { Mutex } from 'async-mutex'
import fs from 'fs'

export class Database extends EventEmitter {
  constructor(file, opts = {}) {
    super()
    this.opts = Object.assign({
      v8: false,
      indexes: {},
      index: { data: {} },
      includeLinePosition: true,
      compress: false,
      compressIndex: false,
      maxMemoryUsage: 64 * 1024 // 64KB
    }, opts)
    this.offsets = []
    this.shouldSave = false
    this.serializer = new Serializer(this.opts)
    this.fileHandler = new FileHandler(file)
    this.indexManager = new IndexManager(this.opts)
    this.indexOffset = 0
    this.writeBuffer = []
    this.mutex = new Mutex()
  }

  use(plugin) {
    if (this.destroyed) throw new Error('Database is destroyed')
    plugin(this)
  }

  async init() {
    if (this.destroyed) throw new Error('Database is destroyed')
    if (this.initialized) return
    if (this.initlializing) return await new Promise(resolve => this.once('init', resolve))
    this.initializing = true
    try {
      if (this.opts.clear) {
        await this.fileHandler.truncate(0).catch(console.error)
        throw new Error('Cleared, empty file')
      }
      const lastLine = await this.fileHandler.readLastLine()
      if (!lastLine || !lastLine.length) {
        throw new Error('File does not exists or is a empty file')
      }
      const offsets = await this.serializer.deserialize(lastLine, { compress: this.opts.compressIndex })
      if (!Array.isArray(offsets)) {
        throw new Error('File to parse offsets, expected an array')
      }
      this.indexOffset = offsets[offsets.length - 2]
      this.offsets = offsets
      const ptr = this.locate(offsets.length - 2)
      this.offsets = this.offsets.slice(0, -2)
      this.shouldTruncate = true
      let indexLine = await this.fileHandler.readRange(...ptr)
      const index = await this.serializer.deserialize(indexLine, { compress: this.opts.compressIndex })
      index && this.indexManager.load(index)
    } catch (e) {
      if (Array.isArray(this.offsets)) {
        this.offsets = []
      }
      this.indexOffset = 0
      if (!String(e).includes('empty file')) {
        console.error('Error loading database:', e)
      }
    } finally {
      this.initializing = false
      this.initialized = true
      this.emit('init')
    }
  }

  async save() {
    if (this.destroyed) throw new Error('Database is destroyed')
    if (!this.initialized) throw new Error('Database not initialized')
    if (this.saving) return new Promise(resolve => this.once('save', resolve))
    this.saving = true
    await this.flush()
    if (!this.shouldSave) return
    this.emit('before-save')
    const index = Object.assign({ data: {} }, this.indexManager.index)
    for (const field in this.indexManager.index.data) {
      for (const term in this.indexManager.index.data[field]) {
        index.data[field][term] = [...this.indexManager.index.data[field][term]] // set to array 
      }
    }
    const offsets = this.offsets.slice(0)
    const indexString = await this.serializer.serialize(index, { compress: this.opts.compressIndex, linebreak: true }) // force linebreak here to allow 'init' to read last line as offsets correctly
    for (const field in this.indexManager.index.data) {
      for (const term in this.indexManager.index.data[field]) {
        this.indexManager.index.data[field][term] = new Set(index.data[field][term]) // set back to set because of serialization
      }
    }
    offsets.push(this.indexOffset)
    offsets.push(this.indexOffset + indexString.length)
    // save offsets as JSON always to prevent linebreaks on last line, which breaks 'init()'
    const offsetsString = await this.serializer.serialize(offsets, { json: true, compress: false, linebreak: false })
    this.writeBuffer.push(indexString)
    this.writeBuffer.push(offsetsString)
    await this.flush() // write the index and offsets
    this.shouldTruncate = true
    this.shouldSave = false
    this.saving = false
    this.emit('save')
  }

  async ready() {
    if (!this.initialized) {
      await new Promise(resolve => this.once('init', resolve))
    }
  }

  locate(n) {
    if (this.offsets[n] === undefined) {
      if (this.offsets[n - 1]) {
        return [this.indexOffset, Number.MAX_SAFE_INTEGER]
      }
      return
    }
    let end = (this.offsets[n + 1] || this.indexOffset || Number.MAX_SAFE_INTEGER)
    return [this.offsets[n], end]
  }

  getRanges(map) {
    return (map || Array.from(this.offsets.keys())).map(n => {
      const ret = this.locate(n)
      if (ret !== undefined) return { start: ret[0], end: ret[1], index: n }
    }).filter(n => n !== undefined)
  }

  async insert(data) {
    if (this.destroyed) throw new Error('Database is destroyed')
    if (!this.initialized) await this.init()
    if (this.shouldTruncate) {
      this.writeBuffer.push(this.indexOffset)
      this.shouldTruncate = false
    }
    const line = await this.serializer.serialize(data, { compress: this.opts.compress, v8: this.opts.v8 }) // using Buffer for offsets accuracy
    const position = this.offsets.length
    this.offsets.push(this.indexOffset)
    this.indexOffset += line.length
    this.emit('insert', data, position)
    this.writeBuffer.push(line)
    if (!this.flushing && this.currentWriteBufferSize() > this.opts.maxMemoryUsage) {
      await this.flush()
    }
    this.indexManager.add(data, position)
    this.shouldSave = true
  }

  currentWriteBufferSize() {
    const lengths = this.writeBuffer.filter(b => Buffer.isBuffer(b)).map(b => b.length)
    return lengths.reduce((a, b) => a + b, 0)
  }

  flush() {
    if (this.flushing) {
      return this.flushing
    }
    return this.flushing = new Promise((resolve, reject) => {
      if (this.destroyed) return reject(new Error('Database is destroyed'))
      if (!this.writeBuffer.length) return resolve()
      let err
      this._flush().catch(e => err = e).finally(() => {
        err ? reject(err) : resolve()
        this.flushing = false
      })
    })
  }

  async _flush() {
    const release = await this.mutex.acquire()
    let fd = await fs.promises.open(this.fileHandler.file, 'a')
    try {
      while (this.writeBuffer.length) {
        let data
        const pos = this.writeBuffer.findIndex(b => typeof b === 'number')
        if (pos === 0) {
          await fd.close()
          await this.fileHandler.truncate(this.writeBuffer.shift())
          fd = await fs.promises.open(this.fileHandler.file, 'a')
          continue
        } else if (pos === -1) {
          data = Buffer.concat(this.writeBuffer)
          this.writeBuffer.length = 0
        } else {
          data = Buffer.concat(this.writeBuffer.slice(0, pos))
          this.writeBuffer.splice(0, pos)
        }
        await fd.write(data)
      }
      this.shouldSave = true
    } catch (err) {
      console.error('Error flushing:', err)
    } finally {
      let err
      await fd.close().catch(e => err = e)
      release()
      err && console.error('Error closing file:', err)
    }
  }

  async *walk(map, options = {}) {
    if (this.destroyed) throw new Error('Database is destroyed')
    if (!this.initialized) await this.init()
    this.shouldSave && await this.save().catch(console.error)
    if (this.indexOffset === 0) return
    if (!Array.isArray(map)) {
      if (map instanceof Set) {
        map = [...map]
      } else if (map && typeof map === 'object') {
        map = [...this.indexManager.query(map, options)]
      } else {
        map = [...Array(this.offsets.length).keys()]
      }
    }
    const ranges = this.getRanges(map)
    const groupedRanges = await this.fileHandler.groupedRanges(ranges)
    const fd = await fs.promises.open(this.fileHandler.file, 'r')
    try {
      for (const groupedRange of groupedRanges) {
        for await (const row of this.fileHandler.readGroupedRange(groupedRange, fd)) {
          const entry = await this.serializer.deserialize(row.line, { compress: this.opts.compress, v8: this.opts.v8 })
          if (entry === null) continue
          if (options.includeOffsets) {
            yield { entry, start: row.start, _: row._ || this.offsets.findIndex(n => n === row.start) }
          } else {
            if (this.opts.includeLinePosition) {
              entry._ = row._ || this.offsets.findIndex(n => n === row.start)
            }
            yield entry
          }
        }
      }
    } finally {
      await fd.close()
    }
  }

  async query(criteria, options = {}) {
    if (this.destroyed) throw new Error('Database is destroyed')
    if (!this.initialized) await this.init()
    this.shouldSave && await this.save().catch(console.error)
    if (!Array.isArray(criteria)) {
      const matchingLines = await this.indexManager.query(criteria, options)
      if (!matchingLines || !matchingLines.size) {
        return []
      }
      criteria = [...matchingLines]
    }
    let results = []
    for await (const entry of this.walk(criteria, options)) results.push(entry)
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
  }

  async update(criteria, data, options={}) {
    if (this.shouldTruncate) {
        this.writeBuffer.push(this.indexOffset)
        this.shouldTruncate = false
    }
    if(this.destroyed) throw new Error('Database is destroyed')
    if(!this.initialized) await this.init()
    this.shouldSave && await this.save().catch(console.error)
    const matchingLines = await this.indexManager.query(criteria, options)
    if (!matchingLines || !matchingLines.size) {
        return []
    }
    const ranges = this.getRanges([...matchingLines])
    const validMatchingLines = new Set(ranges.map(r => r.index))
    if (!validMatchingLines.size) {
      return []
    }
    let entries = []
    for await (const entry of this.walk(criteria, options)) entries.push(entry)
    const lines = []
    for(const entry of entries) {
      const updated = Object.assign(entry, data)
      const ret = await this.serializer.serialize(updated)
      lines.push(ret)
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
    [...validMatchingLines].forEach((lineNumber, i) => {
      this.indexManager.dryRemove(lineNumber)
      this.indexManager.add(entries[i], lineNumber)
    })
    this.shouldSave = true
    return entries
  }

  async delete(criteria, options = {}) {
    if (this.shouldTruncate) {
      this.writeBuffer.push(this.indexOffset)
      this.shouldTruncate = false
    }
    if (this.destroyed) throw new Error('Database is destroyed')
    if (!this.initialized) await this.init()
    this.shouldSave && await this.save().catch(console.error)
    const matchingLines = await this.indexManager.query(criteria, options)
    if (!matchingLines || !matchingLines.size) {
      return 0
    }
    const ranges = this.getRanges([...matchingLines])
    const validMatchingLines = new Set(ranges.map(r => r.index))
    await this.fileHandler.replaceLines(ranges, [])
    const offsets = []
    let byteOffset = 0, k = 0
    this.offsets.forEach((n, i) => {
      if (validMatchingLines.has(i)) {
        const r = ranges[k]
        byteOffset -= (r.end - r.start)
        k++
      } else {
        offsets.push(n + byteOffset)
      }
    })
    this.offsets = offsets
    this.indexOffset += byteOffset
    this.indexManager.remove([...validMatchingLines])
    this.shouldSave = true
    return ranges.length
  }

  async destroy() {
    this.shouldSave && await this.save().catch(console.error)
    this.destroyed = true
    this.indexOffset = 0
    this.indexManager.index = {}
    this.writeBuffer.length = 0
    this.initialized = false
    this.fileHandler.destroy()
  }

  get length() {
    return this?.offsets?.length || 0
  }

  get index() {
    return this.indexManager.index
  }

}
