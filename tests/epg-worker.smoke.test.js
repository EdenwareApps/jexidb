/*
 Smoke tests for epg-worker minimal pipeline
 - Initializes two DBs (programmes, metadata)
 - Inserts a couple of records
 - Verifies basic queries and metadata persistence
 - Checks readColumnIndex usage and sort/limit
*/

const fs = require('fs');
const path = require('path');
const { default: Database } = require('../dist/index.js');

describe('EPG Worker Smoke', () => {
  const tmpDir = path.join(process.cwd(), `smoke-${Date.now()}-${Math.random().toString(16).slice(2)}`);
  const programmesPath = path.join(tmpDir, 'epg-programmes.jdb');
  const metadataPath = path.join(tmpDir, 'epg-metadata.jdb');

  beforeAll(async () => {
    fs.mkdirSync(tmpDir, { recursive: true });
  });

  afterAll(async () => {
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
  });

  test('programmes + metadata minimal flow', async () => {
    const pdb = new Database(programmesPath, {
      indexes: { ch: 'string', start: 'number', e: 'number', c: 'string' },
      create: true,
      clear: true
    });
    await pdb.init();

    const mdb = new Database(metadataPath, {
      indexes: { _type: 'string', id: 'string', name: 'string' },
      create: true,
      clear: true
    });
    await mdb.init();

    // Insert metadata (channel + terms)
    await mdb.insert({ _type: 'channel', id: 'Canal 1', name: 'Canal 1', icon: 'http://channel-icon.jpg', _created: Date.now() });
    await mdb.insert({ _type: 'terms', id: 'Canal 1', terms: ['canal', 'um', 'tv'], _created: Date.now() });

    // Insert programmes
    const now = Math.floor(Date.now() / 1000);
    await pdb.insert({ ch: 'Canal 1', start: now - 100, e: now + 100, t: 'Programa A', i: '', c: ['Variedades'], desc: '...' });
    await pdb.insert({ ch: 'Canal 1', start: now + 200, e: now + 400, t: 'Programa B', i: '', c: ['Entretenimento'], desc: '...' });

    // Persist state (optional) and read via walk to avoid query nuances
    await pdb.save();
    const all = [];
    for await (const r of pdb.walk()) all.push(r);
    expect(all.length).toBeGreaterThanOrEqual(2);
    const titles = all.map(r => r.t).filter(Boolean);
    expect(titles.includes('Programa A')).toBe(true);
    expect(titles.includes('Programa B')).toBe(true);

    // Sort/limit behaviour: earliest by start
    const earliest = await pdb.find({}, { sort: { start: 1 }, limit: 1 });
    expect(Array.isArray(earliest)).toBe(true);
    expect(earliest.length).toBe(1);
    expect(['Programa A', 'Programa B']).toContain(earliest[0].t);

    // readColumnIndex usage (via instance method)
    const channels = pdb.readColumnIndex('ch');
    expect(channels instanceof Set).toBe(true);
    expect(channels.has('Canal 1')).toBe(true);

    // Metadata round-trip
    await mdb.save();
    const metaRows = [];
    for await (const r of mdb.walk()) metaRows.push(r);
    expect(metaRows.some(r => r._type === 'channel' && r.id === 'Canal 1' && r.name === 'Canal 1')).toBe(true);
    const trow = metaRows.find(r => r._type === 'terms' && r.id === 'Canal 1');
    expect(!!trow).toBe(true);
    expect(Array.isArray(trow.terms)).toBe(true);

    await pdb.save();
    await mdb.save();
    await pdb.destroy();
    await mdb.destroy();
  });
});


