/**
 * OperationQueue - Queue system for database operations
 * Resolves race conditions between concurrent operations
 */

export class OperationQueue {
  constructor(debugMode = false) {
    this.queue = []
    this.processing = false
    this.operationId = 0
    this.debugMode = debugMode
    this.stats = {
      totalOperations: 0,
      completedOperations: 0,
      failedOperations: 0,
      averageProcessingTime: 0,
      maxProcessingTime: 0,
      totalProcessingTime: 0
    }
  }
  
  /**
   * Adds an operation to the queue
   * @param {Function} operation - Asynchronous function to be executed
   * @returns {Promise} - Promise that resolves when the operation is completed
   */
  async enqueue(operation) {
    const id = ++this.operationId
    const startTime = Date.now()
    
    if (this.debugMode) {
      console.log(`🔄 Queue: Enqueuing operation ${id}, queue length: ${this.queue.length}`)
    }
    
    this.stats.totalOperations++
    
    return new Promise((resolve, reject) => {
      // Capture stack trace for debugging stuck operations
      const stackTrace = new Error().stack
      
      this.queue.push({ 
        id, 
        operation, 
        resolve, 
        reject,
        timestamp: startTime,
        stackTrace: stackTrace,
        startTime: Date.now()
      })
      
      // Process immediately if not already processing
      this.process().catch(reject)
    })
  }
  
  /**
   * Processes all operations in the queue sequentially
   */
  async process() {
    if (this.processing || this.queue.length === 0) {
      return
    }
    
    this.processing = true
    
    if (this.debugMode) {
      console.log(`🔄 Queue: Starting to process ${this.queue.length} operations`)
    }
    
    try {
      while (this.queue.length > 0) {
        const { id, operation, resolve, reject, timestamp } = this.queue.shift()
        
        if (this.debugMode) {
          console.log(`🔄 Queue: Processing operation ${id}`)
        }
        
        try {
          const result = await operation()
          const processingTime = Date.now() - timestamp
          
          // Atualizar estatísticas
          this.stats.completedOperations++
          this.stats.totalProcessingTime += processingTime
          this.stats.averageProcessingTime = this.stats.totalProcessingTime / this.stats.completedOperations
          this.stats.maxProcessingTime = Math.max(this.stats.maxProcessingTime, processingTime)
          
          resolve(result)
          
          if (this.debugMode) {
            console.log(`✅ Queue: Operation ${id} completed in ${processingTime}ms`)
          }
        } catch (error) {
          const processingTime = Date.now() - timestamp
          
          // Atualizar estatísticas
          this.stats.failedOperations++
          this.stats.totalProcessingTime += processingTime
          this.stats.averageProcessingTime = this.stats.totalProcessingTime / (this.stats.completedOperations + this.stats.failedOperations)
          this.stats.maxProcessingTime = Math.max(this.stats.maxProcessingTime, processingTime)
          
          reject(error)
          
          if (this.debugMode) {
            console.error(`❌ Queue: Operation ${id} failed in ${processingTime}ms:`, error)
          }
        }
      }
    } finally {
      this.processing = false
      
      if (this.debugMode) {
        console.log(`🔄 Queue: Finished processing, remaining: ${this.queue.length}`)
      }
    }
  }
  
  /**
   * Waits for all pending operations to be processed
   * @param {number|null} maxWaitTime - Maximum wait time in ms (null = wait indefinitely)
   * @returns {Promise<boolean>} - true if all operations were processed, false if a timeout occurred
   */
  async waitForCompletion(maxWaitTime = 5000) {
    const startTime = Date.now()
    
    // CRITICAL FIX: Support infinite wait when maxWaitTime is null
    const hasTimeout = maxWaitTime !== null && maxWaitTime !== undefined
    
    while (this.queue.length > 0) {
      // Check timeout only if we have one
      if (hasTimeout && (Date.now() - startTime) >= maxWaitTime) {
        break
      }
      
      await new Promise(resolve => setTimeout(resolve, 1))
    }
    
    const completed = this.queue.length === 0
    if (!completed && hasTimeout) {
      // CRITICAL: Don't leave operations hanging - fail fast with detailed error
      const pendingOperations = this.queue.map(op => ({
        id: op.id,
        stackTrace: op.stackTrace,
        startTime: op.startTime,
        waitTime: Date.now() - op.startTime
      }))
      
      // Clear the queue to prevent memory leaks
      this.queue = []
      
      const error = new Error(`OperationQueue: Operations timed out after ${maxWaitTime}ms. ${pendingOperations.length} operations were stuck and have been cleared.`)
      error.pendingOperations = pendingOperations
      error.queueStats = this.getStats()
      
      if (this.debugMode) {
        console.error(`❌ Queue: Operations timed out, clearing ${pendingOperations.length} stuck operations:`)
        pendingOperations.forEach(op => {
          console.error(`  - Operation ${op.id} (waiting ${op.waitTime}ms):`)
          console.error(`    Stack: ${op.stackTrace}`)
        })
      }
      
      throw error
    }
    
    return completed
  }
  
  /**
   * Returns the current queue length
   */
  getQueueLength() {
    return this.queue.length
  }
  
  /**
   * Checks whether operations are currently being processed
   */
  isProcessing() {
    return this.processing
  }
  
  /**
   * Returns queue statistics
   */
  getStats() {
    return {
      ...this.stats,
      queueLength: this.queue.length,
      isProcessing: this.processing,
      successRate: this.stats.totalOperations > 0 ? 
        (this.stats.completedOperations / this.stats.totalOperations) * 100 : 0
    }
  }
  
  /**
   * Clears the queue (for emergency situations)
   */
  clear() {
    const clearedCount = this.queue.length
    this.queue = []
    
    if (this.debugMode) {
      console.log(`🧹 Queue: Cleared ${clearedCount} pending operations`)
    }
    
    return clearedCount
  }

  /**
   * Detects stuck operations and returns detailed information
   * @param {number} stuckThreshold - Time in ms to consider an operation stuck
   * @returns {Array} - List of stuck operations with stack traces
   */
  detectStuckOperations(stuckThreshold = 10000) {
    const now = Date.now()
    const stuckOperations = this.queue.filter(op => (now - op.startTime) > stuckThreshold)
    
    return stuckOperations.map(op => ({
      id: op.id,
      waitTime: now - op.startTime,
      stackTrace: op.stackTrace,
      timestamp: op.timestamp
    }))
  }

  /**
   * Force-cleans stuck operations (last resort)
   * @param {number} stuckThreshold - Time in ms to consider an operation stuck
   * @returns {number} - Number of operations removed
   */
  forceCleanupStuckOperations(stuckThreshold = 10000) {
    const stuckOps = this.detectStuckOperations(stuckThreshold)
    
    if (stuckOps.length > 0) {
      // Reject all stuck operations
      stuckOps.forEach(stuckOp => {
        const opIndex = this.queue.findIndex(op => op.id === stuckOp.id)
        if (opIndex !== -1) {
          const op = this.queue[opIndex]
          op.reject(new Error(`Operation ${op.id} was stuck for ${stuckOp.waitTime}ms and has been force-cleaned. Stack: ${stuckOp.stackTrace}`))
          this.queue.splice(opIndex, 1)
        }
      })
      
      if (this.debugMode) {
        console.error(`🧹 Queue: Force-cleaned ${stuckOps.length} stuck operations`)
        stuckOps.forEach(op => {
          console.error(`  - Operation ${op.id} (stuck for ${op.waitTime}ms)`)
        })
      }
    }
    
    return stuckOps.length
  }
  
  /**
   * Checks whether the queue is empty
   */
  isEmpty() {
    return this.queue.length === 0
  }
  
  /**
   * Returns information about the next operation in the queue
   */
  peekNext() {
    if (this.queue.length === 0) {
      return null
    }
    
    const next = this.queue[0]
    return {
      id: next.id,
      timestamp: next.timestamp,
      waitTime: Date.now() - next.timestamp
    }
  }
}
