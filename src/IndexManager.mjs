export default class IndexManager {
  constructor(opts) {
    this.opts = Object.assign({}, opts)
    this.index = this.opts.index || {}
    if(typeof(this.index.data) != 'object') {
      this.index.data = {}
    }    
    Object.keys(this.opts.indexes).forEach(field => {
      this.index.data[field] = {}
    })    
  }

  add(row, lineNumber) {
    if (typeof row !== 'object' || !row) {
      throw new Error('Invalid \'row\' parameter, it must be an object')
    }
    if (typeof lineNumber !== 'number') {
      throw new Error('Invalid line number')
    }
    for (const field in this.index.data) {
      if (row[field]) {
        const values = Array.isArray(row[field]) ? row[field] : [row[field]]
        for (const value of values) {
          if (!this.index.data[field][value]) {
            this.index.data[field][value] = new Set()
          }
          if (!this.index.data[field][value].has(lineNumber)) {
            this.index.data[field][value].add(lineNumber)
          }
        }
      }
    }
  }

  remove(lineNumber) {
    for (const field in this.index.data) {
      for (const value in this.index.data[field]) {
        this.index.data[field][value].delete(lineNumber)
        if (this.index.data[field][value].size === 0) {
          delete this.index.data[field][value]
        }
      }
    }
  }

  replace(map) {
    for (const field in this.index.data) {
      for (const value in this.index.data[field]) {
        for(const lineNumber of this.index.data[field][value]) {
          if (map.has(lineNumber)) {
            this.index.data[field][value].delete(lineNumber)
            this.index.data[field][value].add(map.get(lineNumber))
          }
        }
      }
    }
  }

  query(criteria, matchAny=false) {
    if (!criteria) throw new Error('No query criteria provided')
    const fields = Object.keys(criteria)
    if (!fields.length) throw new Error('No valid query criteria provided')
    let matchingLines = matchAny ? new Set() : null
    for (const field of fields) {
      if (typeof(this.index.data[field]) == 'undefined') continue
      const criteriaValue = criteria[field]
      let lineNumbersForField = new Set()
      const isNumericField = this.opts.indexes[field] === 'number'
      if (typeof(criteriaValue) === 'object' && !Array.isArray(criteriaValue)) {
        const fieldIndex = this.index.data[field];
        for (const value in fieldIndex) {
          let includeValue = true
          if (isNumericField) {
            const numericValue = parseFloat(value);
            if (!isNaN(numericValue)) { 
              if (criteriaValue['>'] !== undefined && numericValue <= criteriaValue['>']) {
                includeValue = false;
              }
              if (criteriaValue['>='] !== undefined && numericValue < criteriaValue['>=']) {
                includeValue = false;
              }
              if (criteriaValue['<'] !== undefined && numericValue >= criteriaValue['<']) {
                includeValue = false;
              }
              if (criteriaValue['<='] !== undefined && numericValue > criteriaValue['<=']) {
                includeValue = false;
              }
              if (criteriaValue['!='] !== undefined) {
                const excludeValues = Array.isArray(criteriaValue['!=']) ? criteriaValue['!='] : [criteriaValue['!=']];
                if (excludeValues.includes(numericValue)) {
                  includeValue = false;
                }
              }
            }
          } else {
            if (criteriaValue['contains'] !== undefined && typeof value === 'string') {
              if (!value.includes(criteriaValue['contains'])) {
                includeValue = false;
              }
            }
            if (criteriaValue['regex'] !== undefined && typeof value === 'string') {
              const regex = new RegExp(criteriaValue['regex']);
              if (!regex.test(value)) {
                includeValue = false;
              }
            }
            if (criteriaValue['!='] !== undefined) {
              const excludeValues = Array.isArray(criteriaValue['!=']) ? criteriaValue['!='] : [criteriaValue['!=']];
              if (excludeValues.includes(value)) {
                includeValue = false;
              }
            }
          }

          if (includeValue) {
            for (const lineNumber of fieldIndex[value]) {
              lineNumbersForField.add(lineNumber);
            }
          }
        }
      } else {
        const values = Array.isArray(criteriaValue) ? criteriaValue : [criteriaValue];
        for (const value of values) {
          if (this.index.data[field][value]) {
            for (const lineNumber of this.index.data[field][value]) {
              lineNumbersForField.add(lineNumber);
            }
          }
        }
      }
      if (matchAny) {
        matchingLines = new Set([...matchingLines, ...lineNumbersForField]);
      } else {
        if (matchingLines === null) {
          matchingLines = lineNumbersForField
        } else {
          matchingLines = new Set([...matchingLines].filter(n => lineNumbersForField.has(n)));
        }
        if (!matchingLines.size) {
          return new Set()
        }
      }
    }
    return matchingLines || new Set();
  }

}
