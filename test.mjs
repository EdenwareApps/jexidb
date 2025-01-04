import { StreamDeserializer } from './src/Serializer.mjs';

// Mock para o serializer
class MockSerializer {
    async deserialize(buffer) {
      return JSON.parse(buffer.toString('utf8'));
    }
  }
  
  // Teste para a classe StreamDeserializer
  // Immediately Invoked Function Expression (IIFE) to run the tests asynchronously
  (async () => {
    const assert = (condition, message) => {
      if (!condition) {
        throw new Error(message);
      }
    };
  
    const testStreamDeserializer = async () => {
      console.log("Running tests for StreamDeserializer...");
  
      // Teste 1: Modo JSON
      const jsonLines = [
        Buffer.from('{"id": 1, "value": "a"}'),
        Buffer.from('{"id": 2, "value": "b"}')
      ];

      const deserializerJSON = new StreamDeserializer(false, new MockSerializer());
  
      for (const line of jsonLines) {
        for await (const entry of deserializerJSON.push(line)) {
          // Processar possíveis saídas intermediárias (não esperamos aqui)
        }
      }
  
      const resultJSON = [];
      for await (const entry of deserializerJSON.end()) {
        resultJSON.push(entry);
      }
  
      assert(resultJSON.length === 2, "JSON mode: Number of entries is incorrect.");
      assert(resultJSON[0].id === 1 && resultJSON[1].id === 2, "JSON mode: Entries do not match expected values.");
  
      console.log("JSON mode test passed.");
  
      // Teste 2: Modo de serialização personalizada
      const customLines = [
        Buffer.from('{"id": 3, "value": "c"}'),
        Buffer.from('{"id": 4, "value": "d"}')
      ];
      const mockSerializer = new MockSerializer();
      const deserializerCustom = new StreamDeserializer(true, mockSerializer);
  
      for (const line of customLines) {
        for await (const entry of deserializerCustom.push(line)) {
          // Processar possíveis saídas intermediárias (não esperamos aqui)
        }
      }
  
      const resultCustom = [];
      for await (const entry of deserializerCustom.end()) {
        resultCustom.push(entry);
      }
  
      assert(resultCustom.length === 2, "Custom serialization mode: Number of entries is incorrect.");
      assert(resultCustom[0].id === 3 && resultCustom[1].id === 4, "Custom serialization mode: Entries do not match expected values.");
  
      console.log("Custom serialization mode test passed.");
  
      // Teste 3: Buffer maior que o batch
      const largeBatch = [];
      for (let i = 0; i < 200; i++) {
        largeBatch.push(Buffer.from(`{"id": ${i}, "value": "val${i}"}`));
      }
  
      const resultLarge = [];
      const deserializerLarge = new StreamDeserializer(false, null);
      for (const line of largeBatch) {
        const intermediateResults = [];
        for await (const entry of deserializerLarge.push(line)) {
          intermediateResults.push(entry);
        }
        resultLarge.push(...intermediateResults);
      }      
  
      for await (const entry of deserializerLarge.end()) {
        resultLarge.push(entry);
      }
  
      assert(resultLarge.length === 200, "Large batch mode: Number of entries is incorrect.");
      assert(resultLarge[0].id === 0 && resultLarge[199].id === 199, "Large batch mode: Entries do not match expected values.");
  
      console.log("Large batch mode test passed.");
    };
  
    try {
      await testStreamDeserializer();
      console.log("All tests passed successfully.");
    } catch (error) {
      console.error("Test failed:", error.message);
    }
  })();
  