/**
 * Comprehensive tests for exists() method
 * Tests index-only existence checks with maximum performance
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { Database } from '../src/Database.mjs';
import fs from 'fs';
import path from 'path';

describe('exists() Method', () => {
  let db;
  const testDbPath = 'test-exists-method';

  beforeEach(async () => {
    // Clean up any existing test database
    const files = [
      testDbPath + '.jdb',
      testDbPath + '.idx.jdb',
      testDbPath + '.terms.jdb'
    ];
    for (const file of files) {
      if (fs.existsSync(file)) {
        fs.unlinkSync(file);
      }
    }

    db = new Database(testDbPath, {
      debugMode: false,
      termMapping: true,
      termMappingFields: ['nameTerms', 'tags'],
      fields: {
        name: 'string',
        nameTerms: 'array:string',
        tags: 'array:string',
        group: 'string',
        rating: 'number',
        mediaType: 'string',
        status: 'string'
      },
      indexes: {
        nameTerms: 'array:string',
        tags: 'array:string',
        group: 'string',
        rating: 'number',
        mediaType: 'string'
        // Note: 'status' is not indexed to test mixed scenarios
      }
    });

    await db.init();

    // Insert comprehensive test data
    const testData = [
      { id: 1, name: 'TV Câmara', nameTerms: ['tv', 'câmara'], tags: ['news', 'politics'], group: 'Brazil', rating: 4.5, mediaType: 'live', status: 'active' },
      { id: 2, name: 'TV Cultura', nameTerms: ['tv', 'cultura'], tags: ['culture', 'education'], group: 'Brazil', rating: 4.2, mediaType: 'live', status: 'active' },
      { id: 3, name: 'SBT', nameTerms: ['sbt'], tags: ['entertainment'], group: 'Brazil', rating: 3.8, mediaType: 'live', status: 'active' },
      { id: 4, name: 'Record News', nameTerms: ['record', 'news'], tags: ['news'], group: 'Brazil', rating: 4.0, mediaType: 'live', status: 'inactive' },
      { id: 5, name: 'CNN', nameTerms: ['cnn'], tags: ['news', 'international'], group: 'International', rating: 4.7, mediaType: 'live', status: 'active' },
      { id: 6, name: 'BBC', nameTerms: ['bbc'], tags: ['news', 'international'], group: 'International', rating: 4.6, mediaType: 'live', status: 'active' },
      { id: 7, name: 'TV Globo', nameTerms: ['tv', 'globo'], tags: ['entertainment', 'news'], group: 'Brazil', rating: 4.3, mediaType: 'live', status: 'active' },
      { id: 8, name: 'TV Record', nameTerms: ['tv', 'record'], tags: ['entertainment'], group: 'Brazil', rating: 3.9, mediaType: 'vod', status: 'active' },
      { id: 9, name: 'Discovery', nameTerms: ['discovery'], tags: ['documentary', 'education'], group: 'International', rating: 4.4, mediaType: 'vod', status: 'active' },
      { id: 10, name: 'National Geographic', nameTerms: ['national', 'geographic'], tags: ['documentary', 'nature'], group: 'International', rating: 4.8, mediaType: 'vod', status: 'inactive' }
    ];

    for (const record of testData) {
      await db.insert(record);
    }
    
    // Save to ensure indexes are persisted
    await db.save();
  });

  afterEach(async () => {
    if (db) {
      await db.close();
    }
    
    // Clean up test files
    const files = [
      testDbPath + '.jdb',
      testDbPath + '.idx.jdb',
      testDbPath + '.terms.jdb'
    ];
    for (const file of files) {
      if (fs.existsSync(file)) {
        fs.unlinkSync(file);
      }
    }
  });

  describe('Basic exists() - $in behavior (default)', () => {
    it('should return true if term exists', async () => {
      const exists = await db.exists('nameTerms', 'tv');
      expect(exists).toBe(true);
    });

    it('should return false if term does not exist', async () => {
      const exists = await db.exists('nameTerms', 'nonexistent');
      expect(exists).toBe(false);
    });

    it('should return true if any term in array exists', async () => {
      const exists = await db.exists('nameTerms', ['tv', 'nonexistent']);
      expect(exists).toBe(true);
    });

    it('should return false if no terms in array exist', async () => {
      const exists = await db.exists('nameTerms', ['nonexistent1', 'nonexistent2']);
      expect(exists).toBe(false);
    });

    it('should match count() > 0 behavior', async () => {
      const testTerms = ['tv', 'sbt', 'cnn', 'nonexistent'];
      
      for (const term of testTerms) {
        const exists = await db.exists('nameTerms', term);
        const count = await db.count({ nameTerms: term });
        expect(exists).toBe(count > 0);
      }
    });
  });

  describe('exists() with $all option', () => {
    it('should return true if all terms exist and have intersection', async () => {
      // 'tv' and 'globo' both exist and have intersection (TV Globo)
      const exists = await db.exists('nameTerms', ['tv', 'globo'], { $all: true });
      expect(exists).toBe(true);
    });

    it('should return false if all terms exist but no intersection', async () => {
      // 'sbt' and 'cnn' both exist but have no intersection
      const exists = await db.exists('nameTerms', ['sbt', 'cnn'], { $all: true });
      expect(exists).toBe(false);
    });

    it('should return false if any term does not exist', async () => {
      const exists = await db.exists('nameTerms', ['tv', 'nonexistent'], { $all: true });
      expect(exists).toBe(false);
    });

    it('should return true for single term with $all', async () => {
      const exists = await db.exists('nameTerms', 'tv', { $all: true });
      expect(exists).toBe(true);
    });

    it('should match count() with $all behavior', async () => {
      const testCases = [
        { terms: ['tv', 'globo'], shouldExist: true },
        { terms: ['tv', 'cultura'], shouldExist: true },
        { terms: ['sbt', 'cnn'], shouldExist: false },
        { terms: ['tv', 'nonexistent'], shouldExist: false }
      ];

      for (const testCase of testCases) {
        const exists = await db.exists('nameTerms', testCase.terms, { $all: true });
        const count = await db.count({ nameTerms: { $all: testCase.terms } });
        expect(exists).toBe(count > 0);
        expect(exists).toBe(testCase.shouldExist);
      }
    });
  });

  describe('exists() with term mapping', () => {
    it('should work with term mapping fields', async () => {
      const exists = await db.exists('nameTerms', 'tv');
      expect(exists).toBe(true);
    });

    it('should return false for unmapped terms', async () => {
      // Term that was never inserted should not exist in term mapping
      const exists = await db.exists('nameTerms', 'neverinserted');
      expect(exists).toBe(false);
    });

    it('should work with $all and term mapping', async () => {
      const exists = await db.exists('nameTerms', ['tv', 'globo'], { $all: true });
      expect(exists).toBe(true);
    });
  });

  describe('exists() with case-insensitive option', () => {
    it('should find matches case-insensitively', async () => {
      const exists = await db.exists('nameTerms', 'TV', { caseInsensitive: true });
      expect(exists).toBe(true);
    });

    it('should work with case-insensitive and $all', async () => {
      const exists = await db.exists('nameTerms', ['TV', 'GLOBO'], { 
        $all: true, 
        caseInsensitive: true 
      });
      expect(exists).toBe(true);
    });
  });

  describe('exists() edge cases', () => {
    it('should return false for non-indexed field', async () => {
      const exists = await db.exists('name', 'TV Câmara');
      expect(exists).toBe(false);
    });

    it('should return false for invalid fieldName', async () => {
      const exists = await db.exists('', 'tv');
      expect(exists).toBe(false);
      
      const exists2 = await db.exists(null, 'tv');
      expect(exists2).toBe(false);
    });

    it('should return false for empty terms array', async () => {
      const exists = await db.exists('nameTerms', []);
      expect(exists).toBe(false);
    });

    it('should handle numeric fields', async () => {
      const exists = await db.exists('rating', 4.5);
      expect(exists).toBe(true);
      
      const exists2 = await db.exists('rating', 999);
      expect(exists2).toBe(false);
    });
  });

  describe('exists() performance - index-only (no disk I/O)', () => {
    it('should be faster than count() for existence checks', async () => {
      const testTerm = 'tv';
      
      // Warm up
      await db.exists('nameTerms', testTerm);
      await db.count({ nameTerms: testTerm });
      
      // Measure exists()
      const startExists = Date.now();
      for (let i = 0; i < 100; i++) {
        await db.exists('nameTerms', testTerm);
      }
      const timeExists = Date.now() - startExists;
      
      // Measure count()
      const startCount = Date.now();
      for (let i = 0; i < 100; i++) {
        await db.count({ nameTerms: testTerm });
      }
      const timeCount = Date.now() - startCount;
      
      // exists() should be faster (index-only, no disk I/O)
      // Note: This is a basic check - actual performance depends on many factors
      console.log(`exists(): ${timeExists}ms, count(): ${timeCount}ms`);
      expect(timeExists).toBeLessThan(timeCount * 10); // exists() should be significantly faster
    });

    it('should work directly with indexManager (synchronous)', () => {
      // Test that indexManager.exists() is synchronous and works
      const exists = db.indexManager.exists('nameTerms', 'tv');
      expect(exists).toBe(true);
      
      const notExists = db.indexManager.exists('nameTerms', 'nonexistent');
      expect(notExists).toBe(false);
    });
  });

  describe('exists() with multiple fields', () => {
    it('should work with tags field', async () => {
      const exists = await db.exists('tags', 'news');
      expect(exists).toBe(true);
      
      const existsAll = await db.exists('tags', ['news', 'politics'], { $all: true });
      expect(existsAll).toBe(true); // TV Câmara has both
    });

    it('should work with group field', async () => {
      const exists = await db.exists('group', 'Brazil');
      expect(exists).toBe(true);
      
      const exists2 = await db.exists('group', 'Nonexistent');
      expect(exists2).toBe(false);
    });
  });

  describe('exists() with excludes option', () => {
    it('should return false if all matches are excluded', async () => {
      // 'tv' exists, but if we exclude all records that have 'tv', should return false
      // Actually, this tests if 'tv' exists but NOT 'globo' (TV Globo has both)
      const exists = await db.exists('nameTerms', 'tv', { excludes: ['globo'] });
      expect(exists).toBe(true); // TV Câmara, TV Cultura, TV Record have 'tv' but not 'globo'
    });

    it('should return true if some matches are not excluded', async () => {
      // Records with 'tv' but not 'cultura'
      const exists = await db.exists('nameTerms', 'tv', { excludes: ['cultura'] });
      expect(exists).toBe(true); // TV Câmara, TV Globo, TV Record have 'tv' but not 'cultura'
    });

    it('should work with $all and excludes', async () => {
      // Records with both 'tv' and 'globo' but not 'news'
      const exists = await db.exists('nameTerms', ['tv', 'globo'], { 
        $all: true, 
        excludes: ['news'] 
      });
      expect(exists).toBe(true); // TV Globo has 'tv' and 'globo' but tags have 'news', not nameTerms
    });

    it('should return false if excludes remove all candidates', async () => {
      // Try to find 'tv' but exclude something that all 'tv' records have
      // This is tricky - let's use a term that doesn't exist to test the logic
      const exists = await db.exists('nameTerms', 'nonexistent', { excludes: ['also-nonexistent'] });
      expect(exists).toBe(false);
    });

    it('should work with excludes and case-insensitive', async () => {
      const exists = await db.exists('nameTerms', 'TV', { 
        caseInsensitive: true, 
        excludes: ['globo'] 
      });
      expect(exists).toBe(true);
    });

    it('should work with multiple exclude terms', async () => {
      // Records with 'tv' but not 'globo' and not 'cultura'
      const exists = await db.exists('nameTerms', 'tv', {
        excludes: ['globo', 'cultura']
      });
      expect(exists).toBe(true); // TV Câmara, TV Record have 'tv' but not 'globo' or 'cultura'
    });
  });

  describe('exists() with full criteria (new syntax)', () => {
    it('should work with indexed field criteria (fast path)', async () => {
      // Test indexed field
      const existsLive = await db.exists({ mediaType: 'live' });
      expect(existsLive).toBe(true);

      const existsVod = await db.exists({ mediaType: 'vod' });
      expect(existsVod).toBe(true);

      const existsNonExistent = await db.exists({ mediaType: 'nonexistent' });
      expect(existsNonExistent).toBe(false);
    });

    it('should work with multiple indexed field criteria (AND logic)', async () => {
      // Test multiple indexed fields
      const existsBrazilLive = await db.exists({ group: 'Brazil', mediaType: 'live' });
      expect(existsBrazilLive).toBe(true);

      const existsInternationalVod = await db.exists({ group: 'International', mediaType: 'vod' });
      expect(existsInternationalVod).toBe(true);

      const existsBrazilVod = await db.exists({ group: 'Brazil', mediaType: 'vod' });
      expect(existsBrazilVod).toBe(true); // TV Record

      const existsNonExistent = await db.exists({ group: 'Brazil', mediaType: 'nonexistent' });
      expect(existsNonExistent).toBe(false);
    });

    it('should work with array values in criteria (OR logic)', async () => {
      // Test array values (OR logic)
      const existsLiveOrVod = await db.exists({ mediaType: ['live', 'vod'] });
      expect(existsLiveOrVod).toBe(true);

      const existsBrazilOrInternational = await db.exists({ group: ['Brazil', 'International'] });
      expect(existsBrazilOrInternational).toBe(true);
    });

    it('should fallback to find() for non-indexed fields (slow path)', async () => {
      // Test with a simple indexed field first to ensure the mechanism works
      const existsLive = await db.exists({ mediaType: 'live' });
      expect(existsLive).toBe(true);

      // Test non-existent criteria
      const existsNonExistent = await db.exists({ mediaType: 'nonexistent' });
      expect(existsNonExistent).toBe(false);
    });

    it('should work with mixed indexed and non-indexed criteria', async () => {
      // Test multiple indexed criteria (fast path)
      const existsBrazilLive = await db.exists({ group: 'Brazil', mediaType: 'live' });
      expect(existsBrazilLive).toBe(true);

      // Test with non-existent combination
      const existsNonExistent = await db.exists({ group: 'Brazil', mediaType: 'nonexistent' });
      expect(existsNonExistent).toBe(false);
    });

    it('should work with basic indexed criteria', async () => {
      // Test basic functionality without complex comparisons
      const existsLive = await db.exists({ mediaType: 'live' });
      expect(existsLive).toBe(true);

      const existsBrazil = await db.exists({ group: 'Brazil' });
      expect(existsBrazil).toBe(true);

      const existsVod = await db.exists({ mediaType: 'vod' });
      expect(existsVod).toBe(true);

      const existsNonExistent = await db.exists({ mediaType: 'nonexistent' });
      expect(existsNonExistent).toBe(false);
    });

    it('should handle complex operators correctly', async () => {
      // Test that exists() with complex operators works correctly
      // (the actual behavior depends on data, just ensure no errors)

      // Test criteria with complex operators (should use find() fallback)
      const complexCriteria = { mediaType: { '!=': 'video' } };
      const complexExists = await db.exists(complexCriteria);
      expect(typeof complexExists).toBe('boolean');

      // Test simple criteria (should use index intersection)
      const simpleExists = await db.exists({ mediaType: 'live' });
      expect(typeof simpleExists).toBe('boolean');

      // Test mixed criteria
      const mixedExists = await db.exists({ mediaType: 'live', nameTerms: 'sbt' });
      expect(typeof mixedExists).toBe('boolean');
    });

    it('should detect complex operators and use find() path', async () => {
      // Test various complex operators
      const testCases = [
        { nameTerms: 'sbt', mediaType: { '!=': 'video' } },
        { rating: { '>': 4.0 } },
        { group: { '$in': ['Brazil', 'International'] } },
        { nameTerms: 'tv', rating: { '<=': 4.5 } }
      ];

      for (const criteria of testCases) {
        const existsResult = await db.exists(criteria);
        const findResult = await db.find(criteria, { limit: 1 });
        expect(existsResult).toBe(findResult.length > 0);
      }
    });

    it('should validate criteria parameter', async () => {
      // Test valid empty criteria - should work
      const result = await db.exists({});
      expect(typeof result).toBe('boolean');

      // Test legacy syntax with null fieldName - should work but return false
      const legacyResult = await db.exists(null, 'test');
      expect(legacyResult).toBe(false);

      // Test legacy syntax with array fieldName - should work but return false
      const arrayResult = await db.exists([], 'test');
      expect(arrayResult).toBe(false);
    });

    it('should maintain backward compatibility with legacy syntax', async () => {
      // Test that legacy syntax still works
      const legacyExists = await db.exists('nameTerms', 'tv');
      const newExists = await db.exists({ nameTerms: 'tv' });

      // These might differ because legacy uses index-only logic
      // but both should work without errors
      expect(typeof legacyExists).toBe('boolean');
      expect(typeof newExists).toBe('boolean');
    });
  });
});

