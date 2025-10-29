# Scripts

This directory contains utility scripts for the JexiDB project.

## clean-test-files.js

A Node.js script that automatically cleans up test files generated during test execution.

### Usage

```bash
# Run manually
node scripts/clean-test-files.js

# Run via npm script
npm run clean:test-files
```

### What it cleans

The script removes files matching these patterns:
- `test-db-*` (with or without extensions)
- `test-db-*.jdb`
- `test-db-*.idx.jdb` (contains both index and offsets data)
- `test-normalize*` (with or without extensions)
- `test-confusion*` (with or without extensions)
- `debug-*` (with or without extensions)
- `test-simple-*`
- `test-count.jdb`

### Integration with npm test

The cleanup script is automatically executed after running tests:

```bash
npm test  # Runs jest && npm run clean:test-files
```

This ensures that test files are automatically cleaned up after each test run, keeping the project directory clean.

### Features

- **Pattern-based cleanup**: Uses regex patterns to identify test files
- **Safe deletion**: Only deletes files, not directories
- **Error handling**: Continues execution even if some files can't be deleted
- **Verbose output**: Shows which files were cleaned
- **Summary**: Reports total number of files cleaned
