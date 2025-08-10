const Database = require('../dist/index.js').default;
const { utils } = require('../dist/index.js');
const fs = require('fs').promises;
const path = require('path');

describe('JexiDB Utils', () => {
  let testDir;

  beforeEach(async () => {
    // Create unique test directory
    const testId = Math.random().toString(36).substring(7);
    testDir = `./test-utils-${testId}`;
    await fs.mkdir(testDir, { recursive: true });
  });

  afterEach(async () => {
    // Clean up test directory
    try {
      await fs.rm(testDir, { recursive: true, force: true });
    } catch (error) {
      // Ignore cleanup errors
    }
  });

  describe('validateJSONLFile', () => {
    it('should validate a valid JSONL file', async () => {
      const jsonlPath = path.join(testDir, 'valid.jsonl');
      const content = '{"id":1,"name":"John"}\n{"id":2,"name":"Jane"}\n';
      await fs.writeFile(jsonlPath, content);

      const result = await utils.validateJSONLFile(jsonlPath);

      expect(result.isValid).toBe(true);
      expect(result.errors).toHaveLength(0);
      expect(result.lineCount).toBe(2);
    });

    it('should detect invalid JSON in JSONL file', async () => {
      const jsonlPath = path.join(testDir, 'invalid.jsonl');
      const content = '{"id":1,"name":"John"}\n{"id":2,"name":}\n'; // Invalid JSON
      await fs.writeFile(jsonlPath, content);

      const result = await utils.validateJSONLFile(jsonlPath);

      expect(result.isValid).toBe(false);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0]).toContain('Line 2');
    });
  });

  describe('convertJSONToJSONL', () => {
    it('should convert JSON array to JSONL', async () => {
      const jsonPath = path.join(testDir, 'data.json');
      const jsonlPath = path.join(testDir, 'data.jsonl');
      
      const jsonData = [
        { id: 1, name: 'John', email: 'john@example.com' },
        { id: 2, name: 'Jane', email: 'jane@example.com' }
      ];
      await fs.writeFile(jsonPath, JSON.stringify(jsonData));

      const result = await utils.convertJSONToJSONL(jsonPath, jsonlPath);

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(2);

      // Verify JSONL content
      const jsonlContent = await fs.readFile(jsonlPath, 'utf8');
      const lines = jsonlContent.trim().split('\n');
      expect(lines).toHaveLength(2);
      expect(JSON.parse(lines[0])).toEqual(jsonData[0]);
      expect(JSON.parse(lines[1])).toEqual(jsonData[1]);
    });

    it('should convert single JSON object to JSONL', async () => {
      const jsonPath = path.join(testDir, 'single.json');
      const jsonlPath = path.join(testDir, 'single.jsonl');
      
      const jsonData = { id: 1, name: 'John' };
      await fs.writeFile(jsonPath, JSON.stringify(jsonData));

      const result = await utils.convertJSONToJSONL(jsonPath, jsonlPath);

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(1);
    });
  });

  describe('convertJSONLToJSON', () => {
    it('should convert JSONL to JSON array', async () => {
      const jsonlPath = path.join(testDir, 'data.jsonl');
      const jsonPath = path.join(testDir, 'data.json');
      
      const jsonlContent = '{"id":1,"name":"John"}\n{"id":2,"name":"Jane"}\n';
      await fs.writeFile(jsonlPath, jsonlContent);

      const result = await utils.convertJSONLToJSON(jsonlPath, jsonPath);

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(2);

      // Verify JSON content
      const jsonContent = await fs.readFile(jsonPath, 'utf8');
      const jsonData = JSON.parse(jsonContent);
      expect(jsonData).toHaveLength(2);
      expect(jsonData[0]).toEqual({ id: 1, name: 'John' });
      expect(jsonData[1]).toEqual({ id: 2, name: 'Jane' });
    });
  });

  describe('createDatabaseFromJSON', () => {
    it('should create JexiDB database with auto-detected indexes', async () => {
      const jsonPath = path.join(testDir, 'data.json');
      const dbPath = path.join(testDir, 'users.jsonl');
      
      const jsonData = [
        { id: 1, name: 'John', email: 'john@example.com', age: 30 },
        { id: 2, name: 'Jane', email: 'jane@example.com', age: 25 },
        { id: 3, name: 'Bob', email: 'bob@example.com', age: 35 }
      ];
      await fs.writeFile(jsonPath, JSON.stringify(jsonData));

      const result = await utils.createDatabaseFromJSON(jsonPath, dbPath, {
        autoDetectIndexes: true
      });

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(3);

      // Verify database works - use the path that was actually created
      const db = new Database(dbPath, { validateOnInit: false });
      await db.init();
      
      const allRecords = await db.find({});
      expect(allRecords).toHaveLength(3);
      expect(allRecords[0].name).toBe('John');
      
      await db.close();
    });

    it('should create JexiDB database with manual indexes', async () => {
      const jsonPath = path.join(testDir, 'data.json');
      const dbPath = path.join(testDir, 'users.jsonl');
      
      const jsonData = [
        { id: 1, name: 'John', email: 'john@example.com' },
        { id: 2, name: 'Jane', email: 'jane@example.com' }
      ];
      await fs.writeFile(jsonPath, JSON.stringify(jsonData));

      const result = await utils.createDatabaseFromJSON(jsonPath, dbPath, {
        autoDetectIndexes: false,
        indexes: { id: 'number', email: 'string' }
      });

      expect(result.success).toBe(true);
      expect(result.recordCount).toBe(2);
    });

    it('should handle empty JSON file', async () => {
      const jsonPath = path.join(testDir, 'empty.json');
      const dbPath = path.join(testDir, 'users.jsonl');
      
      await fs.writeFile(jsonPath, JSON.stringify([]));

      const result = await utils.createDatabaseFromJSON(jsonPath, dbPath);

      expect(result.success).toBe(false);
      expect(result.error).toBe('No records found in JSON file');
    });
  });

  describe('analyzeJSONForIndexes', () => {
    it('should analyze JSON and suggest optimal indexes', async () => {
      const jsonPath = path.join(testDir, 'data.json');
      
      const jsonData = [
        { id: 1, name: 'John', email: 'john@example.com', age: 30 },
        { id: 2, name: 'Jane', email: 'jane@example.com', age: 25 },
        { id: 3, name: 'Bob', email: 'bob@example.com', age: 35 }
      ];
      await fs.writeFile(jsonPath, JSON.stringify(jsonData));

      const result = await utils.analyzeJSONForIndexes(jsonPath, 3);

      expect(result.success).toBe(true);
      expect(result.totalRecords).toBe(3);
      expect(result.suggestedIndexes).toHaveProperty('id');
      expect(result.suggestedIndexes).toHaveProperty('email');
    });

    it('should handle JSON with missing fields', async () => {
      const jsonPath = path.join(testDir, 'data.json');
      
      const jsonData = [
        { id: 1, name: 'John', email: 'john@example.com' },
        { id: 2, name: 'Jane' }, // Missing email
        { id: 3, name: 'Bob', email: 'bob@example.com', age: 35 }
      ];
      await fs.writeFile(jsonPath, JSON.stringify(jsonData));

      const result = await utils.analyzeJSONForIndexes(jsonPath, 3);

      expect(result.success).toBe(true);
      expect(result.totalRecords).toBe(3);
      expect(result.suggestedIndexes).toHaveProperty('id');
      expect(result.suggestedIndexes).toHaveProperty('name');
    });
  });


}); 