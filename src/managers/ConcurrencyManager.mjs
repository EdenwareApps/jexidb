/**
 * ConcurrencyManager - Handles all concurrency control and synchronization
 * 
 * Responsibilities:
 * - _acquireMutexWithTimeout()
 * - Mutex and fileMutex management
 * - Concurrent operations control
 */

export class ConcurrencyManager {
  constructor(database) {
    this.database = database
    this.opts = database.opts
    this.mutex = database.mutex
    this.fileMutex = database.fileMutex
    this.operationQueue = database.operationQueue
    this.pendingOperations = database.pendingOperations || 0
    this.pendingPromises = database.pendingPromises || new Set()
  }

  /**
   * Acquire mutex with timeout
   * @param {Mutex} mutex - Mutex to acquire
   * @param {number} timeout - Timeout in milliseconds
   * @returns {Promise<Function>} - Release function
   */
  async _acquireMutexWithTimeout(mutex, timeout = null) {
    const timeoutMs = timeout || this.opts.mutexTimeout
    const startTime = Date.now()
    
    try {
      const release = await Promise.race([
        mutex.acquire(),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error(`Mutex acquisition timeout after ${timeoutMs}ms`)), timeoutMs)
        )
      ])
      
      if (this.opts.debugMode) {
        const acquireTime = Date.now() - startTime
        if (acquireTime > 1000) {
          console.warn(`⚠️ Slow mutex acquisition: ${acquireTime}ms`)
        }
      }
      
      // Wrap release function to track mutex usage
      const originalRelease = release
      return () => {
        try {
          originalRelease()
        } catch (error) {
          console.error(`❌ Error releasing mutex: ${error.message}`)
        }
      }
    } catch (error) {
      if (this.opts.debugMode) {
        console.error(`❌ Mutex acquisition failed: ${error.message}`)
      }
      throw error
    }
  }


  /**
   * Execute operation with queue management
   * @param {Function} operation - Operation to execute
   * @returns {Promise} - Operation result
   */
  async executeWithQueue(operation) {
    if (!this.operationQueue) {
      return operation()
    }
    
    return this.operationQueue.enqueue(operation)
  }

  /**
   * Wait for all pending operations to complete
   * @returns {Promise<void>}
   */
  async waitForPendingOperations() {
    if (this.pendingOperations === 0) {
      return
    }
    
    const pendingPromisesArray = Array.from(this.pendingPromises)
    
    if (pendingPromisesArray.length === 0) {
      return
    }
    
    try {
      await Promise.allSettled(pendingPromisesArray)
      this.pendingPromises.clear()
      this.pendingOperations = 0
    } catch (error) {
      console.warn('Error waiting for pending operations:', error)
      this.pendingPromises.clear()
      this.pendingOperations = 0
    }
  }

  /**
   * Get concurrency statistics
   * @returns {Object} - Concurrency statistics
   */
  getConcurrencyStats() {
    return {
      pendingOperations: this.pendingOperations,
      pendingPromises: this.pendingPromises.size,
      mutexTimeout: this.opts.mutexTimeout,
      hasOperationQueue: !!this.operationQueue
    }
  }

  /**
   * Check if system is under high concurrency load
   * @returns {boolean} - True if under high load
   */
  isUnderHighLoad() {
    const maxOperations = this.opts.maxConcurrentOperations || 10
    return this.pendingOperations >= maxOperations * 0.8 // 80% of max capacity
  }

  /**
   * Get recommended timeout based on current load
   * @returns {number} - Recommended timeout in milliseconds
   */
  getRecommendedTimeout() {
    const baseTimeout = this.opts.mutexTimeout || 15000 // Reduced from 30000 to 15000
    const loadFactor = this.pendingOperations / 10 // Use fixed limit of 10
    
    // Increase timeout based on load
    return Math.min(baseTimeout * (1 + loadFactor), baseTimeout * 3)
  }

  /**
   * Acquire multiple mutexes in order to prevent deadlocks
   * @param {Array<Mutex>} mutexes - Mutexes to acquire in order
   * @param {number} timeout - Timeout in milliseconds
   * @returns {Promise<Array<Function>>} - Array of release functions
   */
  async acquireMultipleMutexes(mutexes, timeout = null) {
    const releases = []
    
    try {
      for (const mutex of mutexes) {
        const release = await this._acquireMutexWithTimeout(mutex, timeout)
        releases.push(release)
      }
      
      return releases
    } catch (error) {
      // Release already acquired mutexes on error
      for (const release of releases) {
        try {
          release()
        } catch (releaseError) {
          console.warn('Error releasing mutex:', releaseError)
        }
      }
      throw error
    }
  }

  /**
   * Execute operation with automatic mutex management
   * @param {Function} operation - Operation to execute
   * @param {Object} options - Options for execution
   * @returns {Promise} - Operation result
   */
  async executeWithMutex(operation, options = {}) {
    const {
      mutex = this.mutex,
      timeout = null,
      retries = 0,
      retryDelay = 100
    } = options
    
    let lastError = null
    
    for (let attempt = 0; attempt <= retries; attempt++) {
      try {
        const release = await this._acquireMutexWithTimeout(mutex, timeout)
        
        try {
          const result = await operation()
          return result
        } finally {
          release()
        }
      } catch (error) {
        lastError = error
        
        if (attempt < retries) {
          // Wait before retry with exponential backoff
          const delay = retryDelay * Math.pow(2, attempt)
          await new Promise(resolve => setTimeout(resolve, delay))
        }
      }
    }
    
    throw lastError
  }

  /**
   * Create a semaphore for limiting concurrent operations
   * @param {number} limit - Maximum concurrent operations
   * @returns {Object} - Semaphore object
   */
  createSemaphore(limit) {
    let current = 0
    const queue = []
    
    return {
      async acquire() {
        return new Promise((resolve) => {
          if (current < limit) {
            current++
            resolve()
          } else {
            queue.push(resolve)
          }
        })
      },
      
      release() {
        if (queue.length > 0) {
          const next = queue.shift()
          next()
        } else {
          current--
        }
      },
      
      getCurrent() {
        return current
      },
      
      getQueueLength() {
        return queue.length
      }
    }
  }

  /**
   * Cleanup concurrency resources
   */
  cleanup() {
    this.pendingPromises.clear()
    this.pendingOperations = 0
    
    if (this.opts.debugMode) {
      console.log('🧹 Concurrency manager cleaned up')
    }
  }
}
