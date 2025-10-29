// Setup file to add Mocha-style matchers to Jest
import { expect } from '@jest/globals'

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
