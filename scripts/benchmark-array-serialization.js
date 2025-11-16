import { performance } from 'node:perf_hooks'
import Serializer from '../src/Serializer.mjs'

const ITERATIONS = parseInt(process.env.BENCH_ITERATIONS ?? '200', 10)
const RECORDS_PER_ITERATION = parseInt(process.env.BENCH_RECORDS ?? '2000', 10)

const serializer = new Serializer({
  enableAdvancedSerialization: true,
  enableArraySerialization: true,
  debugMode: false
})

// Ensure schema is initialized to trigger array serialization path
const schemaFields = ['id', 'name', 'value', 'tags', 'metadata', 'score', 'createdAt']
serializer.schemaManager.initializeSchema(schemaFields)

function createRecord(index) {
  return {
    id: `rec-${index}`,
    name: `Record ${index}`,
    value: index,
    tags: [`tag-${index % 10}`, `tag-${(index + 3) % 10}`],
    metadata: {
      active: index % 2 === 0,
      category: `category-${index % 5}`,
      flags: [index % 7 === 0, index % 11 === 0]
    },
    score: Math.sin(index) * 100,
    createdAt: new Date(1700000000000 + index * 60000).toISOString()
  }
}

function prepareData() {
  const raw = new Array(RECORDS_PER_ITERATION)
  const arrayFormat = new Array(RECORDS_PER_ITERATION)
  const normalized = new Array(RECORDS_PER_ITERATION)

  for (let i = 0; i < RECORDS_PER_ITERATION; i++) {
    const record = createRecord(i)
    raw[i] = record
    const arr = serializer.convertToArrayFormat(record)
    arrayFormat[i] = arr
    normalized[i] = serializer.deepNormalizeEncoding(arr)
  }

  return { raw, arrayFormat, normalized }
}

function benchFastPath(normalizedArrays) {
  let totalLength = 0
  for (let i = 0; i < normalizedArrays.length; i++) {
    const json = serializer._stringifyNormalizedArray(normalizedArrays[i])
    totalLength += json.length
  }
  return totalLength
}

function benchLegacyPath(normalizedArrays) {
  let totalLength = 0
  for (let i = 0; i < normalizedArrays.length; i++) {
    const json = JSON.stringify(normalizedArrays[i])
    totalLength += json.length
  }
  return totalLength
}

function runBenchmarks() {
  const { normalized } = prepareData()

  // Warm-up
  benchFastPath(normalized)
  benchLegacyPath(normalized)

  let fastDuration = 0
  let legacyDuration = 0
  let fastTotal = 0
  let legacyTotal = 0

  for (let i = 0; i < ITERATIONS; i++) {
    const startFast = performance.now()
    fastTotal += benchFastPath(normalized)
    fastDuration += performance.now() - startFast

    const startLegacy = performance.now()
    legacyTotal += benchLegacyPath(normalized)
    legacyDuration += performance.now() - startLegacy
  }

  console.log(`Benchmark settings:`)
  console.log(`  iterations: ${ITERATIONS}`)
  console.log(`  records/iteration: ${RECORDS_PER_ITERATION}`)
  console.log('')
  console.log(`Fast path total time:   ${fastDuration.toFixed(2)} ms`)
  console.log(`Legacy path total time: ${legacyDuration.toFixed(2)} ms`)
  console.log('')
  console.log(`Average fast path per iteration:   ${(fastDuration / ITERATIONS).toFixed(4)} ms`)
  console.log(`Average legacy path per iteration: ${(legacyDuration / ITERATIONS).toFixed(4)} ms`)
  console.log('')
  console.log(`Output size parity check: fast=${fastTotal} legacy=${legacyTotal}`)
}

try {
  runBenchmarks()
} catch (error) {
  console.error('Benchmark failed:', error)
  process.exit(1)
}

