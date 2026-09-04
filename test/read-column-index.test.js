/**
 * Tests for the restored IndexManager.readColumnIndex() public API.
 *
 * Contract under test:
 *  - Reads ONLY the in-memory index (this.index.data[column]).
 *  - NEVER triggers a lazy reload and NEVER performs any disk I/O.
 *  - ALWAYS returns the actual term strings (words/values), never numeric term IDs
 *    (term-mapped columns are translated back through the TermManager).
 *  - Returns an empty Set when the column is not indexed, or when the index
 *    is idle-unloaded (indexLoaded === false) without attempting to reload.
 */
import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { Database } from '../src/Database.mjs';
import IndexManager from '../src/managers/IndexManager.mjs';
import TermManager from '../src/managers/TermManager.mjs';
import fs from 'fs';

describe('IndexManager.readColumnIndex()', () => {
  const createTermMappedManager = () => {
    const termManager = new TermManager();
    termManager.termMappingFields = ['nameTerms'];
    return termManager;
  };
  describe('pure in-memory index (no disk involved)', () => {
    it('returns the terms (keys) of an indexed column as a Set', async () => {
      const im = new IndexManager({
        indexes: { nameTerms: 'array:string', group: 'string' },
        indexIdleUnloadMs: 0
      });

      await im.addBatch([
        { nameTerms: ['tv', 'cultura'], group: 'g1' },
        { nameTerms: ['tv', 'esporte'], group: 'g2' }
      ], 1);

      expect([...im.readColumnIndex('group')].sort()).toEqual(['g1', 'g2']);
      expect([...im.readColumnIndex('nameTerms')].sort()).toEqual(['cultura', 'esporte', 'tv']);
    });

    it('returns a new Set instance on each call (no shared reference)', async () => {
      const im = new IndexManager({
        indexes: { group: 'string' },
        indexIdleUnloadMs: 0
      });

      await im.addBatch([{ group: 'g1' }], 1);

      const first = im.readColumnIndex('group');
      const second = im.readColumnIndex('group');
      expect(first).toEqual(second);
      expect(first).not.toBe(second);
    });

    it('returns an empty Set for a column that is not indexed or unknown', async () => {
      const im = new IndexManager({
        indexes: { group: 'string' },
        indexIdleUnloadMs: 0
      });

      await im.addBatch([{ group: 'g1' }], 1);

      expect(im.readColumnIndex('nonexistent')).toEqual(new Set());
      expect(im.readColumnIndex('group')).not.toEqual(new Set());
    });
  });

  describe('memory-only contract (never triggers disk I/O / lazy reload)', () => {
    let db;
    const testDbPath = 'test-read-column-index';

    beforeEach(async () => {
      const files = [
        `${testDbPath}.jdb`,
        `${testDbPath}.idx.jdb`,
        `${testDbPath}.terms.jdb`
      ];
      for (const file of files) {
        if (fs.existsSync(file)) {
          fs.unlinkSync(file);
        }
      }

      db = new Database(testDbPath, {
        debugMode: false,
        termMapping: true,
        termMappingFields: ['nameTerms'],
        fields: { name: 'string', nameTerms: 'array:string', group: 'string' },
        indexes: { nameTerms: 'array:string', group: 'string' }
      });

      await db.init();
    });

    afterEach(async () => {
      await db.close();
      const files = [
        `${testDbPath}.jdb`,
        `${testDbPath}.idx.jdb`,
        `${testDbPath}.terms.jdb`
      ];
      for (const file of files) {
        if (fs.existsSync(file)) {
          try {
            fs.unlinkSync(file);
          } catch (error) {
            // ignore cleanup errors
          }
        }
      }
    });

    it('reads the keys currently present in the loaded in-memory index', async () => {
      // Populate the in-memory index directly (simulates a loaded index).
      await db.indexManager.addBatch([
        { group: 'g1', nameTermsIds: [1, 2] },
        { group: 'g2', nameTermsIds: [1, 3] }
      ], 1);

      expect([...db.indexManager.readColumnIndex('group')].sort()).toEqual(['g1', 'g2']);
    });

    it('returns empty and does NOT reload when the index was idle-unloaded (no disk read)', async () => {
      // Put the index in a loaded state, then idle-unload it (clears in-memory data).
      await db.indexManager.addBatch([{ group: 'g1' }], 1);
      db.indexManager.indexLoaded = true;
      expect([...db.indexManager.readColumnIndex('group')]).toEqual(['g1']);

      db.indexManager.unload();

      const result = db.indexManager.readColumnIndex('group');
      expect(result).toEqual(new Set());
      // Memory-only: the call must NOT have triggered a lazy reload from disk.
      expect(db.indexManager.indexLoaded).toBe(false);
    });

    it('does not force-load a fresh/lazy index from disk when called', async () => {
      // Fresh lazy DB: the index is not loaded yet and holds no in-memory data.
      expect(db.indexManager.indexLoaded).toBe(false);

      const result = db.indexManager.readColumnIndex('group');

      expect(result).toEqual(new Set());
      // Still not loaded => no disk I/O happened as a side effect of the call.
      expect(db.indexManager.indexLoaded).toBe(false);
    });
  });

  describe('term-mapped columns return words (not numeric term IDs)', () => {
    it('translates numeric term-ID keys back to the actual words', async () => {
      const termManager = createTermMappedManager();
      const im = new IndexManager(
        { indexes: { nameTerms: 'array:string' }, indexIdleUnloadMs: 0 },
        null,
        { termManager }
      );

      await im.addBatch([{ nameTerms: ['tv', 'cultura'] }], 1);
      await im.addBatch([{ nameTerms: ['tv', 'esporte'] }], 2);

      const result = im.readColumnIndex('nameTerms');
      expect(result).toEqual(new Set(['tv', 'cultura', 'esporte']));
      // No numeric term ID may leak into the result.
      for (const value of result) {
        expect(value).not.toMatch(/^\d+$/);
      }
    });

    it('translates keys even when records carry explicit term IDs', async () => {
      const termManager = createTermMappedManager();
      // Create the reverse mapping the way a real DB would after processing records.
      const idTv = termManager.getTermId('tv');
      const idCultura = termManager.getTermId('cultura');

      const im = new IndexManager(
        { indexes: { nameTerms: 'array:string' }, indexIdleUnloadMs: 0 },
        null,
        { termManager }
      );
      await im.addBatch([{ nameTermsIds: [idTv, idCultura] }], 1);

      expect(im.readColumnIndex('nameTerms')).toEqual(new Set(['tv', 'cultura']));
    });

    it('returns an empty Set when the term-mapped column has no data', () => {
      const termManager = createTermMappedManager();
      const im = new IndexManager(
        { indexes: { nameTerms: 'array:string' }, indexIdleUnloadMs: 0 },
        null,
        { termManager }
      );

      expect(im.readColumnIndex('nameTerms')).toEqual(new Set());
    });

    it('keeps raw values for non-term-mapped columns side by side', async () => {
      const termManager = createTermMappedManager();
      const im = new IndexManager(
        { indexes: { nameTerms: 'array:string', group: 'string' }, indexIdleUnloadMs: 0 },
        null,
        { termManager }
      );

      await im.addBatch([{ nameTerms: ['tv'], group: 'g1' }], 1);

      expect(im.readColumnIndex('nameTerms')).toEqual(new Set(['tv']));
      expect(im.readColumnIndex('group')).toEqual(new Set(['g1']));
    });
  });
});
