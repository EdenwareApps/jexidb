export default {
  testEnvironment: 'node',
  testMatch: ['**/test/**/*.test.js', '**/test/**/*.test.mjs'],
  collectCoverage: true,
  coverageDirectory: 'coverage',
  coverageReporters: ['text', 'lcov', 'html'],
  coveragePathIgnorePatterns: [
    '/node_modules/',
    '/test/',
    '/dist/'
  ],
  verbose: true,
  testTimeout: 10000,
  maxWorkers: '50%', // Allow parallel test execution for better performance
  detectOpenHandles: true, // Detect open handles to identify resource leaks
  transform: {
    '^.+\\.mjs$': 'babel-jest',
    '^.+\\.js$': 'babel-jest'
  },
  transformIgnorePatterns: [
    'node_modules/(?!(.*\\.mjs$|p-limit|yocto-queue))'
  ],
  setupFilesAfterEnv: ['<rootDir>/test/setup.js']
};
