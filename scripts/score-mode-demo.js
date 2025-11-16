import { Database } from '../src/Database.mjs'
import path from 'path'
import fs from 'fs'

const dbPath = path.join(process.cwd(), 'temp-score-demo.jdb')
const idxPath = dbPath.replace('.jdb', '.idx.jdb')

for (const file of [dbPath, idxPath]) {
  if (fs.existsSync(file)) {
    fs.unlinkSync(file)
  }
}

const db = new Database(dbPath, {
  indexes: { terms: 'array:string' }
})

await db.init()

await db.insert({ id: 1, title: 'Ação', terms: ['action'] })
await db.insert({ id: 2, title: 'Comédia', terms: ['comedy'] })
await db.insert({ id: 3, title: 'Ação + Comédia', terms: ['action', 'comedy'] })
await db.save()

const weights = { action: 2.0, comedy: 1.0 }

const modes = ['sum', 'max', 'avg', 'first']

for (const mode of modes) {
  const results = await db.score('terms', weights, { mode })
  console.log(`\nmode=${mode}`)
  for (const entry of results) {
    console.log(`  ${entry.title.padEnd(16)} score=${entry.score}`)
  }
}

await db.destroy()

for (const file of [dbPath, idxPath]) {
  if (fs.existsSync(file)) {
    fs.unlinkSync(file)
  }
}


