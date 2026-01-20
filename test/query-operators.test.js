/**
 * Comprehensive tests for query operators
 * Tests the fixes for the $not operator and default operator behavior bugs
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import { Database } from '../src/Database.mjs';
import fs from 'fs';
import path from 'path';

describe('Query Operators', () => {
  let db;
  const testDbPath = 'test-query-operators-comprehensive';

  beforeEach(async () => {
    // Clean up any existing test database
    if (fs.existsSync(testDbPath + '.jdb')) {
      fs.unlinkSync(testDbPath + '.jdb');
    }
    if (fs.existsSync(testDbPath + '.terms.jdb')) {
      fs.unlinkSync(testDbPath + '.terms.jdb');
    }

    db = new Database(testDbPath, {
      fields: { id: 'number', name: 'string', nameTerms: 'array:string', tags: 'array:string', group: 'string', rating: 'number' },
      indexes: { nameTerms: 'array:string', tags: 'array:string' },
      debugMode: false,
      termMapping: true,
      termMappingFields: ['nameTerms', 'tags']
    });

    await db.init();

    // Insert comprehensive test data
    const testData = [
      { id: 1, name: 'TV Câmara', nameTerms: ['tv', 'câmara'], tags: ['news', 'politics'], group: 'Brazil', rating: 4.5 },
      { id: 2, name: 'TV Cultura', nameTerms: ['tv', 'cultura'], tags: ['culture', 'education'], group: 'Brazil', rating: 4.2 },
      { id: 3, name: 'SBT', nameTerms: ['sbt'], tags: ['entertainment'], group: 'Brazil', rating: 3.8 },
      { id: 4, name: 'Record News', nameTerms: ['record', 'news'], tags: ['news'], group: 'Brazil', rating: 4.0 },
      { id: 5, name: 'CNN', nameTerms: ['cnn'], tags: ['news', 'international'], group: 'International', rating: 4.7 },
      { id: 6, name: 'BBC', nameTerms: ['bbc'], tags: ['news', 'international'], group: 'International', rating: 4.6 },
      { id: 7, name: 'TV Globo', nameTerms: ['tv', 'globo'], tags: ['entertainment', 'news'], group: 'Brazil', rating: 4.3 },
      { id: 8, name: 'TV Record', nameTerms: ['tv', 'record'], tags: ['entertainment'], group: 'Brazil', rating: 3.9 },
      { id: 9, name: 'Discovery', nameTerms: ['discovery'], tags: ['documentary', 'education'], group: 'International', rating: 4.4 },
      { id: 10, name: 'National Geographic', nameTerms: ['national', 'geographic'], tags: ['documentary', 'nature'], group: 'International', rating: 4.8 }
    ];

    for (const record of testData) {
      await db.insert(record);
    }
  });

  afterEach(async () => {
    if (db) {
      await db.close();
    }
    
    // Clean up test files
    const files = [testDbPath + '.jdb', testDbPath + '.terms.jdb'];
    for (const file of files) {
      if (fs.existsSync(file)) {
        fs.unlinkSync(file);
      }
    }
  });

  describe('$not Operator Fixes', () => {
    it('should handle $not operator consistently with or without explicit $and', async () => {
      // Test 1: $not without explicit $and (the bug case)
      const result1 = await db.find({nameTerms: 'tv', $not: {nameTerms: { $in: ['cultura'] }}});
      expect(result1).toHaveLength(3); // TV Câmara, TV Globo, TV Record
      expect(result1.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Globo', 'TV Record']));
      expect(result1.map(r => r.name)).not.toContain('TV Cultura');

      // Test 2: $not with explicit $and (should give same result)
      const result2 = await db.find({"$and": [{nameTerms: 'tv'}, {$not: {nameTerms: { $in: ['cultura'] }}}]});
      expect(result2).toHaveLength(3);
      expect(result2.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Globo', 'TV Record']));
      expect(result2.map(r => r.name)).not.toContain('TV Cultura');

      // Both results should be identical
      expect(result1.map(r => r.id).sort()).toEqual(result2.map(r => r.id).sort());
    });

    it('should handle $not with different fields', async () => {
      const result = await db.find({group: 'Brazil', $not: {nameTerms: { $in: ['globo'] }}});
      expect(result).toHaveLength(5); // All Brazil channels except TV Globo
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Cultura', 'SBT', 'Record News', 'TV Record']));
      expect(result.map(r => r.name)).not.toContain('TV Globo');
    });

    it('should handle complex $not queries', async () => {
      const result = await db.find({nameTerms: 'tv', $not: {nameTerms: { $in: ['cultura', 'globo'] }}});
      expect(result).toHaveLength(2); // TV Câmara, TV Record
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Record']));
      expect(result.map(r => r.name)).not.toContain('TV Cultura');
      expect(result.map(r => r.name)).not.toContain('TV Globo');
    });

    it('should handle $not with numeric comparisons', async () => {
      const result = await db.find({group: 'Brazil', $not: {rating: { $gte: 4.0 }}});
      expect(result).toHaveLength(2); // SBT (3.8), TV Record (3.9)
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['SBT', 'TV Record']));
    });

    it('should handle nested $not operators', async () => {
      const result = await db.find({$not: {nameTerms: { $in: ['tv'] }}});
      expect(result).toHaveLength(6); // All channels without 'tv' in nameTerms
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['SBT', 'Record News', 'CNN', 'BBC', 'Discovery', 'National Geographic']));
      expect(result.map(r => r.name)).not.toContain('TV Câmara');
      expect(result.map(r => r.name)).not.toContain('TV Cultura');
      expect(result.map(r => r.name)).not.toContain('TV Globo');
      expect(result.map(r => r.name)).not.toContain('TV Record');
    });
  });

  describe('Default Operator Behavior (AND Logic)', () => {
    it('should use AND logic for multiple conditions at root level', async () => {
      const result = await db.find({nameTerms: 'tv', group: 'Brazil'});
      expect(result).toHaveLength(4); // All TV channels in Brazil
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Cultura', 'TV Globo', 'TV Record']));
    });

    it('should use AND logic for multiple field conditions', async () => {
      const result = await db.find({group: 'International', rating: { $gte: 4.5 }});
      expect(result).toHaveLength(3); // CNN, BBC, National Geographic
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['CNN', 'BBC', 'National Geographic']));
    });

    it('should use AND logic with array field conditions', async () => {
      const result = await db.find({tags: { $in: ['news'] }, group: 'Brazil'});
      expect(result).toHaveLength(3); // TV Câmara, Record News, TV Globo
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'Record News', 'TV Globo']));
    });

    it('should use AND logic with mixed operators', async () => {
      const result = await db.find({
        nameTerms: 'tv',
        group: 'Brazil',
        rating: { $gte: 4.0 },
        tags: { $in: ['news'] }
      });
      expect(result).toHaveLength(2); // TV Câmara, TV Globo
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Globo']));
    });
  });

  describe('Complex Query Combinations', () => {
    it('should handle $and with $not', async () => {
      const result = await db.find({
        "$and": [
          {group: 'Brazil'},
          {$not: {nameTerms: { $in: ['cultura', 'globo'] }}}
        ]
      });
      expect(result).toHaveLength(4); // All Brazil channels except TV Cultura and TV Globo
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'SBT', 'Record News', 'TV Record']));
    });

    it('should handle $or with $not', async () => {
      const result = await db.find({
        "$or": [
          {nameTerms: 'tv'},
          {group: 'International'}
        ],
        $not: {rating: { $lt: 4.0 }}
      });
      // Should include TV channels with rating >= 4.0 OR International channels with rating >= 4.0
      expect(result.length).toBeGreaterThan(0);
      result.forEach(record => {
        expect(record.rating).toBeGreaterThanOrEqual(4.0);
      });
    });

    it('should handle multiple $not conditions', async () => {
      // Note: Multiple $not conditions with the same key will overwrite each other
      // This is expected JavaScript behavior. For multiple conditions, use $and
      const result = await db.find({
        "$and": [
          {group: 'Brazil'},
          {$not: {nameTerms: { $in: ['cultura'] }}},
          {$not: {rating: { $lt: 4.0 }}}
        ]
      });
      // This should be equivalent to: Brazil AND NOT cultura AND NOT rating < 4.0
      expect(result.length).toBeGreaterThan(0);
      result.forEach(record => {
        expect(record.group).toBe('Brazil');
        expect(record.nameTerms).not.toContain('cultura');
        expect(record.rating).toBeGreaterThanOrEqual(4.0);
      });
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty $not condition', async () => {
      const result = await db.find({nameTerms: 'tv', $not: {}});
      expect(result).toHaveLength(4); // All TV channels (empty $not matches nothing, so excludes nothing)
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Cultura', 'TV Globo', 'TV Record']));
    });

    it('should handle $not with non-existent field', async () => {
      const result = await db.find({nameTerms: 'tv', $not: {nonExistentField: 'value'}});
      expect(result).toHaveLength(4); // All TV channels (non-existent field never matches)
      expect(result.map(r => r.name)).toEqual(expect.arrayContaining(['TV Câmara', 'TV Cultura', 'TV Globo', 'TV Record']));
    });

    it('should handle $not with null values', async () => {
      // Insert a record with null value
      await db.insert({ id: 99, name: 'Test Channel', nameTerms: null, group: 'Test' });
      
      const result = await db.find({group: 'Test', $not: {nameTerms: null}});
      expect(result).toHaveLength(0); // No records match (the only Test record has null nameTerms)
    });
  });

  describe('Performance and Consistency', () => {
    it('should produce consistent results across multiple queries', async () => {
      const query = {nameTerms: 'tv', $not: {nameTerms: { $in: ['cultura'] }}};
      
      // Run the same query multiple times
      const results = [];
      for (let i = 0; i < 5; i++) {
        const result = await db.find(query);
        results.push(result.map(r => r.id).sort());
      }
      
      // All results should be identical
      for (let i = 1; i < results.length; i++) {
        expect(results[i]).toEqual(results[0]);
      }
    });

    it('should handle large result sets with $not', async () => {
      // This test ensures the fix works with larger datasets
      const result = await db.find({$not: {group: 'NonExistent'}});
      expect(result).toHaveLength(10); // All records (since no records have group 'NonExistent')
    });
  });
});
