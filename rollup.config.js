import babel from '@rollup/plugin-babel';
import { nodeResolve } from '@rollup/plugin-node-resolve';
import commonjs from '@rollup/plugin-commonjs';

export default {
  input: 'src/Database.mjs',
  output: {
    file: 'dist/Database.cjs',
    format: 'cjs',
    exports: 'named',
    inlineDynamicImports: true,
  },
  plugins: [
    nodeResolve({
      preferBuiltins: true,
    }),
    commonjs(),
    babel({
      babelHelpers: 'bundled',
      plugins: ['@babel/plugin-transform-async-generator-functions'],
      exclude: 'node_modules/**',
    }),
  ],
  external: [
    'events',
    'fs',
    'readline',
    'path'
  ]
};
