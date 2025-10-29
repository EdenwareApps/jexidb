/**
 * StatisticsManager - Handles all statistics and metrics collection
 * 
 * Responsibilities:
 * - getJournalStats()
 * - Performance metrics
 * - Usage statistics
 */

export class StatisticsManager {
  constructor(database) {
    this.database = database
    this.opts = database.opts
    this.usageStats = database.usageStats || {
      totalQueries: 0,
      streamingQueries: 0,
      indexedQueries: 0,
      streamingAverageTime: 0,
      indexedAverageTime: 0
    }
    this.performanceMetrics = {
      startTime: Date.now(),
      lastResetTime: Date.now(),
      totalOperations: 0,
      totalErrors: 0,
      averageOperationTime: 0,
      peakMemoryUsage: 0,
      cacheHits: 0,
      cacheMisses: 0
    }
  }

  /**
   * Get journal statistics
   * @returns {Object} - Journal statistics
   */
  getJournalStats() {
    return {
      enabled: false,
      message: 'Journal mode has been removed'
    }
  }

  /**
   * Get performance metrics
   * @returns {Object} - Performance metrics
   */
  getPerformanceMetrics() {
    const now = Date.now()
    const uptime = now - this.performanceMetrics.startTime
    
    return {
      uptime: uptime,
      totalOperations: this.performanceMetrics.totalOperations,
      totalErrors: this.performanceMetrics.totalErrors,
      averageOperationTime: this.performanceMetrics.averageOperationTime,
      operationsPerSecond: this.performanceMetrics.totalOperations / (uptime / 1000),
      errorRate: this.performanceMetrics.totalErrors / Math.max(1, this.performanceMetrics.totalOperations),
      peakMemoryUsage: this.performanceMetrics.peakMemoryUsage,
      cacheHitRate: this.performanceMetrics.cacheHits / Math.max(1, this.performanceMetrics.cacheHits + this.performanceMetrics.cacheMisses),
      lastResetTime: this.performanceMetrics.lastResetTime
    }
  }

  /**
   * Get usage statistics
   * @returns {Object} - Usage statistics
   */
  getUsageStats() {
    return {
      totalQueries: this.usageStats.totalQueries,
      streamingQueries: this.usageStats.streamingQueries,
      indexedQueries: this.usageStats.indexedQueries,
      streamingAverageTime: this.usageStats.streamingAverageTime,
      indexedAverageTime: this.usageStats.indexedAverageTime,
      queryDistribution: {
        streaming: this.usageStats.streamingQueries / Math.max(1, this.usageStats.totalQueries),
        indexed: this.usageStats.indexedQueries / Math.max(1, this.usageStats.totalQueries)
      }
    }
  }

  /**
   * Get database statistics
   * @returns {Object} - Database statistics
   */
  getDatabaseStats() {
    return {
      totalRecords: this.database.offsets?.length || 0,
      indexOffset: this.database.indexOffset || 0,
      writeBufferSize: this.database.writeBuffer?.length || 0,
      indexedFields: Object.keys(this.database.indexManager?.index?.data || {}),
      totalIndexedFields: Object.keys(this.database.indexManager?.index?.data || {}).length,
      isInitialized: this.database.initialized || false,
      isDestroyed: this.database.destroyed || false
    }
  }

  /**
   * Get comprehensive statistics
   * @returns {Object} - All statistics combined
   */
  getComprehensiveStats() {
    return {
      database: this.getDatabaseStats(),
      performance: this.getPerformanceMetrics(),
      usage: this.getUsageStats(),
      journal: this.getJournalStats(),
      timestamp: Date.now()
    }
  }

  /**
   * Record operation performance
   * @param {string} operation - Operation name
   * @param {number} duration - Duration in milliseconds
   * @param {boolean} success - Whether operation was successful
   */
  recordOperation(operation, duration, success = true) {
    this.performanceMetrics.totalOperations++
    
    if (!success) {
      this.performanceMetrics.totalErrors++
    }
    
    // Update average operation time
    const currentAverage = this.performanceMetrics.averageOperationTime
    const totalOps = this.performanceMetrics.totalOperations
    this.performanceMetrics.averageOperationTime = (currentAverage * (totalOps - 1) + duration) / totalOps
    
    // Update peak memory usage (if available)
    if (typeof process !== 'undefined' && process.memoryUsage) {
      const memoryUsage = process.memoryUsage()
      this.performanceMetrics.peakMemoryUsage = Math.max(
        this.performanceMetrics.peakMemoryUsage,
        memoryUsage.heapUsed
      )
    }
  }

  /**
   * Record cache hit
   */
  recordCacheHit() {
    this.performanceMetrics.cacheHits++
  }

  /**
   * Record cache miss
   */
  recordCacheMiss() {
    this.performanceMetrics.cacheMisses++
  }

  /**
   * Update query statistics
   * @param {string} type - Query type ('streaming' or 'indexed')
   * @param {number} duration - Query duration in milliseconds
   */
  updateQueryStats(type, duration) {
    this.usageStats.totalQueries++
    
    if (type === 'streaming') {
      this.usageStats.streamingQueries++
      this.updateAverageTime('streaming', duration)
    } else if (type === 'indexed') {
      this.usageStats.indexedQueries++
      this.updateAverageTime('indexed', duration)
    }
  }

  /**
   * Update average time for a query type
   * @param {string} type - Query type
   * @param {number} time - Time taken
   */
  updateAverageTime(type, time) {
    const key = `${type}AverageTime`
    if (!this.usageStats[key]) {
      this.usageStats[key] = 0
    }
    
    const currentAverage = this.usageStats[key]
    const count = this.usageStats[`${type}Queries`] || 1
    
    // Calculate running average
    this.usageStats[key] = (currentAverage * (count - 1) + time) / count
  }

  /**
   * Reset all statistics
   */
  resetStats() {
    this.usageStats = {
      totalQueries: 0,
      streamingQueries: 0,
      indexedQueries: 0,
      streamingAverageTime: 0,
      indexedAverageTime: 0
    }
    
    this.performanceMetrics = {
      startTime: Date.now(),
      lastResetTime: Date.now(),
      totalOperations: 0,
      totalErrors: 0,
      averageOperationTime: 0,
      peakMemoryUsage: 0,
      cacheHits: 0,
      cacheMisses: 0
    }
    
    if (this.opts.debugMode) {
      console.log('📊 Statistics reset')
    }
  }

  /**
   * Export statistics to JSON
   * @returns {string} - JSON string of statistics
   */
  exportStats() {
    return JSON.stringify(this.getComprehensiveStats(), null, 2)
  }

  /**
   * Get statistics summary for logging
   * @returns {string} - Summary string
   */
  getStatsSummary() {
    const stats = this.getComprehensiveStats()
    return `
📊 Database Statistics Summary:
  Records: ${stats.database.totalRecords}
  Queries: ${stats.usage.totalQueries} (${Math.round(stats.usage.queryDistribution.streaming * 100)}% streaming, ${Math.round(stats.usage.queryDistribution.indexed * 100)}% indexed)
  Operations: ${stats.performance.totalOperations}
  Errors: ${stats.performance.totalErrors}
  Uptime: ${Math.round(stats.performance.uptime / 1000)}s
  Cache Hit Rate: ${Math.round(stats.performance.cacheHitRate * 100)}%
    `.trim()
  }

  /**
   * Check if statistics collection is enabled
   * @returns {boolean} - True if enabled
   */
  isEnabled() {
    return this.opts.collectStatistics !== false
  }

  /**
   * Enable or disable statistics collection
   * @param {boolean} enabled - Whether to enable statistics
   */
  setEnabled(enabled) {
    this.opts.collectStatistics = enabled
    
    if (this.opts.debugMode) {
      console.log(`📊 Statistics collection ${enabled ? 'enabled' : 'disabled'}`)
    }
  }
}
