/**
 * OperationQueue - Sistema de fila para operações do banco de dados
 * Resolve race conditions entre operações concorrentes
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
   * Adiciona uma operação à fila
   * @param {Function} operation - Função assíncrona a ser executada
   * @returns {Promise} - Promise que resolve quando a operação é concluída
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
      
      // Processar imediatamente se não estiver processando
      this.process().catch(reject)
    })
  }
  
  /**
   * Processa todas as operações na fila sequencialmente
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
   * Aguarda todas as operações pendentes serem processadas
   * @param {number|null} maxWaitTime - Tempo máximo de espera em ms (null = wait indefinitely)
   * @returns {Promise<boolean>} - true se todas foram processadas, false se timeout
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
   * Retorna o tamanho atual da fila
   */
  getQueueLength() {
    return this.queue.length
  }
  
  /**
   * Verifica se está processando operações
   */
  isProcessing() {
    return this.processing
  }
  
  /**
   * Retorna estatísticas da fila
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
   * Limpa a fila (para casos de emergência)
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
   * Detecta operações travadas e retorna informações detalhadas
   * @param {number} stuckThreshold - Tempo em ms para considerar uma operação travada
   * @returns {Array} - Lista de operações travadas com stack traces
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
   * Força a limpeza de operações travadas (último recurso)
   * @param {number} stuckThreshold - Tempo em ms para considerar uma operação travada
   * @returns {number} - Número de operações removidas
   */
  forceCleanupStuckOperations(stuckThreshold = 10000) {
    const stuckOps = this.detectStuckOperations(stuckThreshold)
    
    if (stuckOps.length > 0) {
      // Rejeitar todas as operações travadas
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
   * Verifica se a fila está vazia
   */
  isEmpty() {
    return this.queue.length === 0
  }
  
  /**
   * Retorna informações sobre a próxima operação na fila
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
