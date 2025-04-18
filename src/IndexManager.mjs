export default class IndexManager {
  constructor(opts) {
    this.opts = Object.assign({}, opts)
    this.index = Object.assign({data: {}}, this.opts.index)
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

  dryRemove(ln) { // remove line numbers from index without adjusting the rest
    for (const field in this.index.data) {
      for (const value in this.index.data[field]) {
        if (this.index.data[field][value].has(ln)) {
          this.index.data[field][value].delete(ln)
        }
        if (this.index.data[field][value].size === 0) {
          delete this.index.data[field][value]
        }
      }
    }
  }

  remove(lineNumbers) { // remove line numbers from index and adjust the rest
    lineNumbers.sort((a, b) => a - b) // Sort ascending to make calculations easier
    for (const field in this.index.data) {
      for (const value in this.index.data[field]) {
        const newSet = new Set()  
        for (const ln of this.index.data[field][value]) {
          let offset = 0
          for (const lineNumber of lineNumbers) {
            if (lineNumber < ln) {
              offset++
            } else if (lineNumber === ln) {
              offset = -1 // Marca para remoção
              break
            }
          }  
          if (offset >= 0) {
            newSet.add(ln - offset) // Atualiza o valor
          }
        }  
        if (newSet.size > 0) {
          this.index.data[field][value] = newSet
        } else {
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

  query(criteria, options = {}) {
    if (typeof options === 'boolean') {
      options = { matchAny: options };
    }
    const { matchAny = false, caseInsensitive = false } = options;
    if (!criteria) throw new Error('No query criteria provided');
    const fields = Object.keys(criteria);
    if (!fields.length) throw new Error('No valid query criteria provided');
    let matchingLines = matchAny ? new Set() : null;
  
    for (const field of fields) {
      if (typeof this.index.data[field] === 'undefined') continue;
      const criteriaValue = criteria[field];
      let lineNumbersForField = new Set();
      const isNumericField = this.opts.indexes[field] === 'number';
  
      if (typeof criteriaValue === 'object' && !Array.isArray(criteriaValue)) {
        const fieldIndex = this.index.data[field];
        for (const value in fieldIndex) {
          let includeValue = true;
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
                const excludeValues = Array.isArray(criteriaValue['!='])
                  ? criteriaValue['!=']
                  : [criteriaValue['!=']];
                if (excludeValues.includes(numericValue)) {
                  includeValue = false;
                }
              }
            }
          } else {
            if (criteriaValue['contains'] !== undefined && typeof value === 'string') {
              const term = String(criteriaValue['contains']);
              if (caseInsensitive) {
                if (!value.toLowerCase().includes(term.toLowerCase())) {
                  includeValue = false;
                }
              } else {
                if (!value.includes(term)) {
                  includeValue = false;
                }
              }
            }
            if (criteriaValue['regex'] !== undefined) {
              let regex;
              if (typeof criteriaValue['regex'] === 'string') {
                regex = new RegExp(criteriaValue['regex'], caseInsensitive ? 'i' : '');
              } else if (criteriaValue['regex'] instanceof RegExp) {
                if (caseInsensitive && !criteriaValue['regex'].ignoreCase) {
                  const flags = criteriaValue['regex'].flags.includes('i')
                    ? criteriaValue['regex'].flags
                    : criteriaValue['regex'].flags + 'i';
                  regex = new RegExp(criteriaValue['regex'].source, flags);
                } else {
                  regex = criteriaValue['regex'];
                }
              }
              if (regex && !regex.test(value)) {
                includeValue = false;
              }
            }
            if (criteriaValue['!='] !== undefined) {
              const excludeValues = Array.isArray(criteriaValue['!='])
                ? criteriaValue['!=']
                : [criteriaValue['!=']];
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
        // Comparação simples de igualdade
        const values = Array.isArray(criteriaValue) ? criteriaValue : [criteriaValue];
        const fieldData = this.index.data[field];
        for (const searchValue of values) {
          for (const key in fieldData) {
            let match = false;
            if (isNumericField) {
              // Converter ambas as partes para número
              match = Number(key) === Number(searchValue);
            } else {
              match = caseInsensitive
                ? key.toLowerCase() === String(searchValue).toLowerCase()
                : key === searchValue;
            }
            if (match) {
              for (const lineNumber of fieldData[key]) {
                lineNumbersForField.add(lineNumber);
              }
            }
          }
        }
      }
  
      // Consolida os resultados de cada campo
      if (matchAny) {
        matchingLines = new Set([...matchingLines, ...lineNumbersForField]);
      } else {
        if (matchingLines === null) {
          matchingLines = lineNumbersForField;
        } else {
          matchingLines = new Set([...matchingLines].filter(n => lineNumbersForField.has(n)));
        }
        if (!matchingLines.size) {
          return new Set();
        }
      }
    }
    return matchingLines || new Set();
  } 
 
  load(index) {
    for(const field in index.data) {
      for(const term in index.data[field]) {
        index.data[field][term] = new Set(index.data[field][term]) // set to array 
      }
    }
    this.index = index
  }
  
  readColumnIndex(column) {
    return new Set((this.index.data && this.index.data[column]) ? Object.keys(this.index.data[column]) : [])
  }
}
