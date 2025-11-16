const aliasToCanonical = {
  '>': '$gt',
  '>=': '$gte',
  '<': '$lt',
  '<=': '$lte',
  '!=': '$ne',
  '=': '$eq',
  '==': '$eq',
  eq: '$eq',
  equals: '$eq',
  in: '$in',
  nin: '$nin',
  regex: '$regex',
  contains: '$contains',
  all: '$all',
  exists: '$exists',
  size: '$size',
  not: '$not'
}

const canonicalToLegacy = {
  '$gt': '>',
  '$gte': '>=',
  '$lt': '<',
  '$lte': '<=',
  '$ne': '!=',
  '$eq': '=',
  '$contains': 'contains',
  '$regex': 'regex'
}

/**
 * Normalize an operator to its canonical Mongo-style representation (prefixed with $)
 * @param {string} operator
 * @returns {string}
 */
export function normalizeOperator(operator) {
  if (typeof operator !== 'string') {
    return operator
  }

  if (operator.startsWith('$')) {
    return operator
  }

  if (aliasToCanonical[operator] !== undefined) {
    return aliasToCanonical[operator]
  }

  const lowerCase = operator.toLowerCase()
  if (aliasToCanonical[lowerCase] !== undefined) {
    return aliasToCanonical[lowerCase]
  }

  return operator
}

/**
 * Convert an operator to its legacy (non-prefixed) alias when available
 * @param {string} operator
 * @returns {string}
 */
export function operatorToLegacy(operator) {
  if (typeof operator !== 'string') {
    return operator
  }

  const canonical = normalizeOperator(operator)
  if (canonicalToLegacy[canonical]) {
    return canonicalToLegacy[canonical]
  }

  return operator
}

/**
 * Normalize operator keys in a criteria object
 * @param {Object} criteriaValue
 * @param {Object} options
 * @param {'canonical'|'legacy'} options.target - Preferred operator style
 * @param {boolean} [options.preserveOriginal=false] - Whether to keep the original keys alongside normalized ones
 * @returns {Object}
 */
export function normalizeCriteriaOperators(criteriaValue, { target = 'canonical', preserveOriginal = false } = {}) {
  if (!criteriaValue || typeof criteriaValue !== 'object' || Array.isArray(criteriaValue)) {
    return criteriaValue
  }

  const normalized = preserveOriginal ? { ...criteriaValue } : {}

  for (const [operator, value] of Object.entries(criteriaValue)) {
    const canonical = normalizeOperator(operator)

    if (target === 'canonical') {
      normalized[canonical] = value
      if (preserveOriginal && canonical !== operator) {
        normalized[operator] = value
      }
    } else if (target === 'legacy') {
      const legacy = operatorToLegacy(operator)
      normalized[legacy] = value

      if (preserveOriginal) {
        if (legacy !== canonical) {
          normalized[canonical] = value
        }
        if (operator !== legacy && operator !== canonical) {
          normalized[operator] = value
        }
      }
    }
  }

  return normalized
}

