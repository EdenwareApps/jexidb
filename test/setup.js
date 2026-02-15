// Setup file to add Mocha-style matchers to Jest
import { expect } from '@jest/globals'

// Mock p-retry for tests - just execute once without retry
jest.mock('p-retry', () => jest.fn(async (fn) => {
  return await fn({ attempt: 1, retriesLeft: 0 });
}));

// Add the chai-style API to expect
expect.to = {
  deep: {
    equal: (received, expected) => {
      return expect(received).toEqual(expected)
    }
  },
  equal: (received, expected) => {
    return expect(received).toBe(expected)
  }
}

// Also add to global expect
global.expect = expect
