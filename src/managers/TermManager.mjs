/**
 * TermManager - Manages term-to-ID mapping for efficient storage
 * 
 * Responsibilities:
 * - Map terms to numeric IDs for space efficiency
 * - Track term usage counts for cleanup
 * - Load/save terms from/to index file
 * - Clean up orphaned terms
 */
export default class TermManager {
  constructor() {
    this.termToId = new Map()     // "bra" -> 1
    this.idToTerm = new Map()     // 1 -> "bra"
    this.termCounts = new Map()   // 1 -> 1500 (how many times used)
    this.nextId = 1
  }

  /**
   * Get ID for a term (create if doesn't exist)
   * @param {string} term - Term to get ID for
   * @returns {number} - Numeric ID for the term
   */
  getTermId(term) {
    if (this.termToId.has(term)) {
      const id = this.termToId.get(term)
      this.termCounts.set(id, (this.termCounts.get(id) || 0) + 1)
      return id
    }
    
    const id = this.nextId++
    this.termToId.set(term, id)
    this.idToTerm.set(id, term)
    this.termCounts.set(id, 1)
    
    return id
  }

  /**
   * Get term ID without incrementing count (for IndexManager use)
   * @param {string} term - Term to get ID for
   * @returns {number} - Numeric ID for the term
   */
  getTermIdWithoutIncrement(term) {
    if (this.termToId.has(term)) {
      return this.termToId.get(term)
    }
    
    const id = this.nextId++
    this.termToId.set(term, id)
    this.idToTerm.set(id, term)
    this.termCounts.set(id, 0) // Start with 0 count
    
    return id
  }

  /**
   * Get term by ID
   * @param {number} id - Numeric ID
   * @returns {string|null} - Term or null if not found
   */
  getTerm(id) {
    return this.idToTerm.get(id) || null
  }

  /**
   * Bulk get term IDs for multiple terms (optimized for performance)
   * @param {string[]} terms - Array of terms to get IDs for
   * @returns {number[]} - Array of term IDs in the same order
   */
  bulkGetTermIds(terms) {
    if (!Array.isArray(terms) || terms.length === 0) {
      return []
    }

    const termIds = new Array(terms.length)
    
    // Process all terms in a single pass
    for (let i = 0; i < terms.length; i++) {
      const term = terms[i]
      if (this.termToId.has(term)) {
        const id = this.termToId.get(term)
        this.termCounts.set(id, (this.termCounts.get(id) || 0) + 1)
        termIds[i] = id
      } else {
        const id = this.nextId++
        this.termToId.set(term, id)
        this.idToTerm.set(id, term)
        this.termCounts.set(id, 1)
        termIds[i] = id
      }
    }
    
    return termIds
  }

  /**
   * Load terms from file data
   * @param {Object} termsData - Terms data from file
   */
  loadTerms(termsData) {
    if (!termsData || typeof termsData !== 'object') {
      return
    }

    for (const [id, term] of Object.entries(termsData)) {
      const numericId = parseInt(id)
      if (!isNaN(numericId) && term) {
        this.termToId.set(term, numericId)
        this.idToTerm.set(numericId, term)
        this.nextId = Math.max(this.nextId, numericId + 1)
        // Initialize count to 0 - will be updated as terms are used
        this.termCounts.set(numericId, 0)
      }
    }
  }

  /**
   * Save terms to file format
   * @returns {Object} - Terms data for file
   */
  saveTerms() {
    const termsData = {}
    for (const [id, term] of this.idToTerm) {
      termsData[id] = term
    }
    return termsData
  }

  /**
   * Clean up orphaned terms (terms with count 0)
   * @param {boolean} forceCleanup - Force cleanup even if conditions not met
   * @param {Object} options - Cleanup options
   * @returns {number} - Number of orphaned terms removed
   */
  cleanupOrphanedTerms(forceCleanup = false, options = {}) {
    const {
      intelligentCleanup = true,
      minOrphanCount = 10,
      orphanPercentage = 0.15,
      checkSystemState = true
    } = options

    // INTELLIGENT CLEANUP: Check if cleanup should be performed
    if (!forceCleanup && intelligentCleanup) {
      const stats = this.getStats()
      const orphanedCount = stats.orphanedTerms
      const totalTerms = stats.totalTerms
      
      // Only cleanup if conditions are met
      const shouldCleanup = (
        orphanedCount >= minOrphanCount &&           // Minimum orphan count
        orphanedCount > totalTerms * orphanPercentage && // Orphans > percentage of total
        (!checkSystemState || this.isSystemSafe())   // System is safe (if check enabled)
      )
      
      if (!shouldCleanup) {
        return 0 // Don't cleanup if conditions not met
      }
    } else if (!forceCleanup) {
      return 0 // Don't remove anything during normal operations
    }
    
    // PERFORM CLEANUP: Remove orphaned terms
    const orphanedIds = []
    
    for (const [id, count] of this.termCounts) {
      if (count === 0) {
        orphanedIds.push(id)
      }
    }
    
    // Remove orphaned terms with additional safety checks
    for (const id of orphanedIds) {
      const term = this.idToTerm.get(id)
      if (term && typeof term === 'string') { // Extra safety: only remove string terms
        this.termToId.delete(term)
        this.idToTerm.delete(id)
        this.termCounts.delete(id)
      }
    }
    
    return orphanedIds.length
  }

  /**
   * Check if system is safe for cleanup operations
   * @returns {boolean} - True if system is safe for cleanup
   */
  isSystemSafe() {
    // This method should be overridden by the database instance
    // to provide system state information
    return true // Default to safe for backward compatibility
  }

  /**
   * Perform intelligent automatic cleanup
   * @param {Object} options - Cleanup options
   * @returns {number} - Number of orphaned terms removed
   */
  performIntelligentCleanup(options = {}) {
    return this.cleanupOrphanedTerms(false, {
      intelligentCleanup: true,
      minOrphanCount: 5,        // Lower threshold for automatic cleanup
      orphanPercentage: 0.1,    // 10% of total terms
      checkSystemState: true,
      ...options
    })
  }

  /**
   * Decrement term count (when term is removed from index)
   * @param {number} termId - Term ID to decrement
   */
  decrementTermCount(termId) {
    const count = this.termCounts.get(termId) || 0
    this.termCounts.set(termId, Math.max(0, count - 1))
  }

  /**
   * Increment term count (when term is added to index)
   * @param {number} termId - Term ID to increment
   */
  incrementTermCount(termId) {
    const count = this.termCounts.get(termId) || 0
    this.termCounts.set(termId, count + 1)
  }

  /**
   * Get statistics about terms
   * @returns {Object} - Term statistics
   */
  getStats() {
    return {
      totalTerms: this.termToId.size,
      nextId: this.nextId,
      orphanedTerms: Array.from(this.termCounts.entries()).filter(([_, count]) => count === 0).length
    }
  }

  /**
   * Check if a term exists
   * @param {string} term - Term to check
   * @returns {boolean} - True if term exists
   */
  hasTerm(term) {
    return this.termToId.has(term)
  }

  /**
   * Get all terms
   * @returns {Array} - Array of all terms
   */
  getAllTerms() {
    return Array.from(this.termToId.keys())
  }

  /**
   * Get all term IDs
   * @returns {Array} - Array of all term IDs
   */
  getAllTermIds() {
    return Array.from(this.idToTerm.keys())
  }

  /**
   * Get statistics about term mapping
   * @returns {Object} - Statistics object
   */
  getStatistics() {
    return {
      totalTerms: this.termToId.size,
      nextId: this.nextId,
      termCounts: Object.fromEntries(this.termCounts),
      sampleTerms: Array.from(this.termToId.entries()).slice(0, 5)
    }
  }

}
