import { EventEmitter } from 'events'
import fs from 'fs'
import readline from 'readline'

/**
 * StreamingProcessor - Efficient streaming processing for large datasets
 * 
 * Features:
 * - Memory-efficient processing of large files
 * - Configurable batch sizes
 * - Progress tracking
 * - Error handling and recovery
 * - Transform pipelines
 * - Backpressure control
 */
export class StreamingProcessor extends EventEmitter {
  constructor(opts = {}) {
    super()
    
    this.opts = {
      batchSize: opts.batchSize || 1000,
      maxConcurrency: opts.maxConcurrency || 5,
      bufferSize: opts.bufferSize || 64 * 1024, // 64KB
      enableProgress: opts.enableProgress !== false,
      progressInterval: opts.progressInterval || 1000, // 1 second
      enableBackpressure: opts.enableBackpressure !== false,
      maxPendingBatches: opts.maxPendingBatches || 10,
      ...opts
    }
    
    this.isProcessing = false
    this.currentBatch = 0
    this.totalBatches = 0
    this.processedItems = 0
    this.totalItems = 0
    this.pendingBatches = 0
    this.stats = {
      startTime: 0,
      endTime: 0,
      totalProcessingTime: 0,
      averageBatchTime: 0,
      itemsPerSecond: 0,
      memoryUsage: 0
    }
    
    this.progressTimer = null
    this.transformPipeline = []
  }

  /**
   * Add a transform function to the pipeline
   */
  addTransform(transformFn) {
    this.transformPipeline.push(transformFn)
    return this
  }

  /**
   * Process a file stream
   */
  async processFileStream(filePath, processorFn) {
    if (this.isProcessing) {
      throw new Error('Streaming processor is already running')
    }
    
    this.isProcessing = true
    this.stats.startTime = Date.now()
    this.currentBatch = 0
    this.processedItems = 0
    
    try {
      // Get file size for progress tracking
      const stats = await fs.promises.stat(filePath)
      this.totalItems = Math.ceil(stats.size / this.opts.bufferSize)
      
      // Start progress tracking
      if (this.opts.enableProgress) {
        this._startProgressTracking()
      }
      
      // Create read stream
      const fileStream = fs.createReadStream(filePath, {
        encoding: 'utf8',
        highWaterMark: this.opts.bufferSize
      })
      
      // Create readline interface
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
      })
      
      let batch = []
      let lineCount = 0
      
      // Process lines in batches
      for await (const line of rl) {
        if (line.trim()) {
          batch.push(line)
          lineCount++
          
          // Process batch when it reaches the configured size
          if (batch.length >= this.opts.batchSize) {
            await this._processBatch(batch, processorFn)
            batch = []
          }
        }
      }
      
      // Process remaining items in the last batch
      if (batch.length > 0) {
        await this._processBatch(batch, processorFn)
      }
      
      this.stats.endTime = Date.now()
      this.stats.totalProcessingTime = this.stats.endTime - this.stats.startTime
      this.stats.itemsPerSecond = this.processedItems / (this.stats.totalProcessingTime / 1000)
      
      this.emit('complete', {
        totalItems: this.processedItems,
        totalBatches: this.currentBatch,
        processingTime: this.stats.totalProcessingTime,
        itemsPerSecond: this.stats.itemsPerSecond
      })
      
    } catch (error) {
      this.emit('error', error)
      throw error
    } finally {
      this.isProcessing = false
      this._stopProgressTracking()
    }
  }

  /**
   * Process an array of items in streaming fashion
   */
  async processArray(items, processorFn) {
    if (this.isProcessing) {
      throw new Error('Streaming processor is already running')
    }
    
    this.isProcessing = true
    this.stats.startTime = Date.now()
    this.currentBatch = 0
    this.processedItems = 0
    this.totalItems = items.length
    this.totalBatches = Math.ceil(items.length / this.opts.batchSize)
    
    try {
      // Start progress tracking
      if (this.opts.enableProgress) {
        this._startProgressTracking()
      }
      
      // Process items in batches
      for (let i = 0; i < items.length; i += this.opts.batchSize) {
        const batch = items.slice(i, i + this.opts.batchSize)
        await this._processBatch(batch, processorFn)
      }
      
      this.stats.endTime = Date.now()
      this.stats.totalProcessingTime = this.stats.endTime - this.stats.startTime
      this.stats.itemsPerSecond = this.processedItems / (this.stats.totalProcessingTime / 1000)
      
      this.emit('complete', {
        totalItems: this.processedItems,
        totalBatches: this.currentBatch,
        processingTime: this.stats.totalProcessingTime,
        itemsPerSecond: this.stats.itemsPerSecond
      })
      
    } catch (error) {
      this.emit('error', error)
      throw error
    } finally {
      this.isProcessing = false
      this._stopProgressTracking()
    }
  }

  /**
   * Process a generator function
   */
  async processGenerator(generatorFn, processorFn) {
    if (this.isProcessing) {
      throw new Error('Streaming processor is already running')
    }
    
    this.isProcessing = true
    this.stats.startTime = Date.now()
    this.currentBatch = 0
    this.processedItems = 0
    this.totalItems = 0 // Unknown for generators
    
    try {
      // Start progress tracking
      if (this.opts.enableProgress) {
        this._startProgressTracking()
      }
      
      const generator = generatorFn()
      let batch = []
      
      for await (const item of generator) {
        batch.push(item)
        this.totalItems++
        
        // Process batch when it reaches the configured size
        if (batch.length >= this.opts.batchSize) {
          await this._processBatch(batch, processorFn)
          batch = []
        }
      }
      
      // Process remaining items in the last batch
      if (batch.length > 0) {
        await this._processBatch(batch, processorFn)
      }
      
      this.stats.endTime = Date.now()
      this.stats.totalProcessingTime = this.stats.endTime - this.stats.startTime
      this.stats.itemsPerSecond = this.processedItems / (this.stats.totalProcessingTime / 1000)
      
      this.emit('complete', {
        totalItems: this.processedItems,
        totalBatches: this.currentBatch,
        processingTime: this.stats.totalProcessingTime,
        itemsPerSecond: this.stats.itemsPerSecond
      })
      
    } catch (error) {
      this.emit('error', error)
      throw error
    } finally {
      this.isProcessing = false
      this._stopProgressTracking()
    }
  }

  /**
   * Process a single batch
   */
  async _processBatch(batch, processorFn) {
    if (this.opts.enableBackpressure && this.pendingBatches >= this.opts.maxPendingBatches) {
      // Wait for backpressure to reduce
      await this._waitForBackpressure()
    }
    
    this.pendingBatches++
    this.currentBatch++
    
    try {
      const startTime = Date.now()
      
      // Apply transform pipeline
      let transformedBatch = batch
      for (const transform of this.transformPipeline) {
        transformedBatch = await transform(transformedBatch)
      }
      
      // Process the batch
      const result = await processorFn(transformedBatch, this.currentBatch)
      
      const batchTime = Date.now() - startTime
      this.stats.averageBatchTime = 
        (this.stats.averageBatchTime + batchTime) / 2
      
      this.processedItems += batch.length
      
      this.emit('batchComplete', {
        batchNumber: this.currentBatch,
        batchSize: batch.length,
        processingTime: batchTime,
        result
      })
      
    } catch (error) {
      this.emit('batchError', {
        batchNumber: this.currentBatch,
        batchSize: batch.length,
        error
      })
      throw error
    } finally {
      this.pendingBatches--
    }
  }

  /**
   * Wait for backpressure to reduce
   */
  async _waitForBackpressure() {
    return new Promise((resolve) => {
      const checkBackpressure = () => {
        if (this.pendingBatches < this.opts.maxPendingBatches) {
          resolve()
        } else {
          setTimeout(checkBackpressure, 10)
        }
      }
      checkBackpressure()
    })
  }

  /**
   * Start progress tracking
   */
  _startProgressTracking() {
    this.progressTimer = setInterval(() => {
      const progress = {
        currentBatch: this.currentBatch,
        totalBatches: this.totalBatches,
        processedItems: this.processedItems,
        totalItems: this.totalItems,
        percentage: this.totalItems > 0 ? (this.processedItems / this.totalItems) * 100 : 0,
        itemsPerSecond: this.stats.itemsPerSecond,
        averageBatchTime: this.stats.averageBatchTime,
        memoryUsage: process.memoryUsage().heapUsed / 1024 / 1024 // MB
      }
      
      this.emit('progress', progress)
    }, this.opts.progressInterval)
    this.progressTimer.unref(); // Allow process to exit without waiting for this timer
  }

  /**
   * Stop progress tracking
   */
  _stopProgressTracking() {
    if (this.progressTimer) {
      clearInterval(this.progressTimer)
      this.progressTimer = null
    }
  }

  /**
   * Get current statistics
   */
  getStats() {
    return {
      ...this.stats,
      isProcessing: this.isProcessing,
      currentBatch: this.currentBatch,
      totalBatches: this.totalBatches,
      processedItems: this.processedItems,
      totalItems: this.totalItems,
      pendingBatches: this.pendingBatches,
      transformPipelineLength: this.transformPipeline.length
    }
  }

  /**
   * Stop processing
   */
  stop() {
    this.isProcessing = false
    this._stopProgressTracking()
    this.emit('stopped')
  }

  /**
   * Reset the processor
   */
  reset() {
    this.stop()
    this.currentBatch = 0
    this.totalBatches = 0
    this.processedItems = 0
    this.totalItems = 0
    this.pendingBatches = 0
    this.stats = {
      startTime: 0,
      endTime: 0,
      totalProcessingTime: 0,
      averageBatchTime: 0,
      itemsPerSecond: 0,
      memoryUsage: 0
    }
    this.transformPipeline = []
  }
}

/**
 * Predefined transform functions
 */
export const Transforms = {
  // Parse JSON lines
  parseJSON: (batch) => {
    return batch.map(line => {
      try {
        return JSON.parse(line)
      } catch (error) {
        console.warn('Failed to parse JSON line:', line)
        return null
      }
    }).filter(item => item !== null)
  },
  
  // Filter out null/undefined values
  filterNull: (batch) => {
    return batch.filter(item => item !== null && item !== undefined)
  },
  
  // Transform to specific format
  toFormat: (format) => (batch) => {
    switch (format) {
      case 'string':
        return batch.map(item => String(item))
      case 'number':
        return batch.map(item => Number(item))
      case 'object':
        return batch.map(item => typeof item === 'object' ? item : { value: item })
      default:
        return batch
    }
  },
  
  // Add metadata
  addMetadata: (metadata) => (batch) => {
    return batch.map(item => ({
      ...item,
      ...metadata,
      processedAt: Date.now()
    }))
  }
}

export default StreamingProcessor
