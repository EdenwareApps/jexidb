"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Database = void 0;
var _events = require("events");
var _FileHandler = _interopRequireDefault(require("./FileHandler.mjs"));
var _IndexManager = _interopRequireDefault(require("./IndexManager.mjs"));
var _Serializer = require("./Serializer.mjs");
var _fs = _interopRequireDefault(require("fs"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _createForOfIteratorHelper(r, e) { var t = "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (!t) { if (Array.isArray(r) || (t = _unsupportedIterableToArray(r)) || e && r && "number" == typeof r.length) { t && (r = t); var _n = 0, F = function F() {}; return { s: F, n: function n() { return _n >= r.length ? { done: !0 } : { done: !1, value: r[_n++] }; }, e: function e(r) { throw r; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var o, a = !0, u = !1; return { s: function s() { t = t.call(r); }, n: function n() { var r = t.next(); return a = r.done, r; }, e: function e(r) { u = !0, o = r; }, f: function f() { try { a || null == t["return"] || t["return"](); } finally { if (u) throw o; } } }; }
function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function _toConsumableArray(r) { return _arrayWithoutHoles(r) || _iterableToArray(r) || _unsupportedIterableToArray(r) || _nonIterableSpread(); }
function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _unsupportedIterableToArray(r, a) { if (r) { if ("string" == typeof r) return _arrayLikeToArray(r, a); var t = {}.toString.call(r).slice(8, -1); return "Object" === t && r.constructor && (t = r.constructor.name), "Map" === t || "Set" === t ? Array.from(r) : "Arguments" === t || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(t) ? _arrayLikeToArray(r, a) : void 0; } }
function _iterableToArray(r) { if ("undefined" != typeof Symbol && null != r[Symbol.iterator] || null != r["@@iterator"]) return Array.from(r); }
function _arrayWithoutHoles(r) { if (Array.isArray(r)) return _arrayLikeToArray(r); }
function _arrayLikeToArray(r, a) { (null == a || a > r.length) && (a = r.length); for (var e = 0, n = Array(a); e < a; e++) n[e] = r[e]; return n; }
function _regeneratorRuntime() { "use strict"; /*! regenerator-runtime -- Copyright (c) 2014-present, Facebook, Inc. -- license (MIT): https://github.com/facebook/regenerator/blob/main/LICENSE */ _regeneratorRuntime = function _regeneratorRuntime() { return e; }; var t, e = {}, r = Object.prototype, n = r.hasOwnProperty, o = Object.defineProperty || function (t, e, r) { t[e] = r.value; }, i = "function" == typeof Symbol ? Symbol : {}, a = i.iterator || "@@iterator", c = i.asyncIterator || "@@asyncIterator", u = i.toStringTag || "@@toStringTag"; function define(t, e, r) { return Object.defineProperty(t, e, { value: r, enumerable: !0, configurable: !0, writable: !0 }), t[e]; } try { define({}, ""); } catch (t) { define = function define(t, e, r) { return t[e] = r; }; } function wrap(t, e, r, n) { var i = e && e.prototype instanceof Generator ? e : Generator, a = Object.create(i.prototype), c = new Context(n || []); return o(a, "_invoke", { value: makeInvokeMethod(t, r, c) }), a; } function tryCatch(t, e, r) { try { return { type: "normal", arg: t.call(e, r) }; } catch (t) { return { type: "throw", arg: t }; } } e.wrap = wrap; var h = "suspendedStart", l = "suspendedYield", f = "executing", s = "completed", y = {}; function Generator() {} function GeneratorFunction() {} function GeneratorFunctionPrototype() {} var p = {}; define(p, a, function () { return this; }); var d = Object.getPrototypeOf, v = d && d(d(values([]))); v && v !== r && n.call(v, a) && (p = v); var g = GeneratorFunctionPrototype.prototype = Generator.prototype = Object.create(p); function defineIteratorMethods(t) { ["next", "throw", "return"].forEach(function (e) { define(t, e, function (t) { return this._invoke(e, t); }); }); } function AsyncIterator(t, e) { function invoke(r, o, i, a) { var c = tryCatch(t[r], t, o); if ("throw" !== c.type) { var u = c.arg, h = u.value; return h && "object" == _typeof(h) && n.call(h, "__await") ? e.resolve(h.__await).then(function (t) { invoke("next", t, i, a); }, function (t) { invoke("throw", t, i, a); }) : e.resolve(h).then(function (t) { u.value = t, i(u); }, function (t) { return invoke("throw", t, i, a); }); } a(c.arg); } var r; o(this, "_invoke", { value: function value(t, n) { function callInvokeWithMethodAndArg() { return new e(function (e, r) { invoke(t, n, e, r); }); } return r = r ? r.then(callInvokeWithMethodAndArg, callInvokeWithMethodAndArg) : callInvokeWithMethodAndArg(); } }); } function makeInvokeMethod(e, r, n) { var o = h; return function (i, a) { if (o === f) throw Error("Generator is already running"); if (o === s) { if ("throw" === i) throw a; return { value: t, done: !0 }; } for (n.method = i, n.arg = a;;) { var c = n.delegate; if (c) { var u = maybeInvokeDelegate(c, n); if (u) { if (u === y) continue; return u; } } if ("next" === n.method) n.sent = n._sent = n.arg;else if ("throw" === n.method) { if (o === h) throw o = s, n.arg; n.dispatchException(n.arg); } else "return" === n.method && n.abrupt("return", n.arg); o = f; var p = tryCatch(e, r, n); if ("normal" === p.type) { if (o = n.done ? s : l, p.arg === y) continue; return { value: p.arg, done: n.done }; } "throw" === p.type && (o = s, n.method = "throw", n.arg = p.arg); } }; } function maybeInvokeDelegate(e, r) { var n = r.method, o = e.iterator[n]; if (o === t) return r.delegate = null, "throw" === n && e.iterator["return"] && (r.method = "return", r.arg = t, maybeInvokeDelegate(e, r), "throw" === r.method) || "return" !== n && (r.method = "throw", r.arg = new TypeError("The iterator does not provide a '" + n + "' method")), y; var i = tryCatch(o, e.iterator, r.arg); if ("throw" === i.type) return r.method = "throw", r.arg = i.arg, r.delegate = null, y; var a = i.arg; return a ? a.done ? (r[e.resultName] = a.value, r.next = e.nextLoc, "return" !== r.method && (r.method = "next", r.arg = t), r.delegate = null, y) : a : (r.method = "throw", r.arg = new TypeError("iterator result is not an object"), r.delegate = null, y); } function pushTryEntry(t) { var e = { tryLoc: t[0] }; 1 in t && (e.catchLoc = t[1]), 2 in t && (e.finallyLoc = t[2], e.afterLoc = t[3]), this.tryEntries.push(e); } function resetTryEntry(t) { var e = t.completion || {}; e.type = "normal", delete e.arg, t.completion = e; } function Context(t) { this.tryEntries = [{ tryLoc: "root" }], t.forEach(pushTryEntry, this), this.reset(!0); } function values(e) { if (e || "" === e) { var r = e[a]; if (r) return r.call(e); if ("function" == typeof e.next) return e; if (!isNaN(e.length)) { var o = -1, i = function next() { for (; ++o < e.length;) if (n.call(e, o)) return next.value = e[o], next.done = !1, next; return next.value = t, next.done = !0, next; }; return i.next = i; } } throw new TypeError(_typeof(e) + " is not iterable"); } return GeneratorFunction.prototype = GeneratorFunctionPrototype, o(g, "constructor", { value: GeneratorFunctionPrototype, configurable: !0 }), o(GeneratorFunctionPrototype, "constructor", { value: GeneratorFunction, configurable: !0 }), GeneratorFunction.displayName = define(GeneratorFunctionPrototype, u, "GeneratorFunction"), e.isGeneratorFunction = function (t) { var e = "function" == typeof t && t.constructor; return !!e && (e === GeneratorFunction || "GeneratorFunction" === (e.displayName || e.name)); }, e.mark = function (t) { return Object.setPrototypeOf ? Object.setPrototypeOf(t, GeneratorFunctionPrototype) : (t.__proto__ = GeneratorFunctionPrototype, define(t, u, "GeneratorFunction")), t.prototype = Object.create(g), t; }, e.awrap = function (t) { return { __await: t }; }, defineIteratorMethods(AsyncIterator.prototype), define(AsyncIterator.prototype, c, function () { return this; }), e.AsyncIterator = AsyncIterator, e.async = function (t, r, n, o, i) { void 0 === i && (i = Promise); var a = new AsyncIterator(wrap(t, r, n, o), i); return e.isGeneratorFunction(r) ? a : a.next().then(function (t) { return t.done ? t.value : a.next(); }); }, defineIteratorMethods(g), define(g, u, "Generator"), define(g, a, function () { return this; }), define(g, "toString", function () { return "[object Generator]"; }), e.keys = function (t) { var e = Object(t), r = []; for (var n in e) r.push(n); return r.reverse(), function next() { for (; r.length;) { var t = r.pop(); if (t in e) return next.value = t, next.done = !1, next; } return next.done = !0, next; }; }, e.values = values, Context.prototype = { constructor: Context, reset: function reset(e) { if (this.prev = 0, this.next = 0, this.sent = this._sent = t, this.done = !1, this.delegate = null, this.method = "next", this.arg = t, this.tryEntries.forEach(resetTryEntry), !e) for (var r in this) "t" === r.charAt(0) && n.call(this, r) && !isNaN(+r.slice(1)) && (this[r] = t); }, stop: function stop() { this.done = !0; var t = this.tryEntries[0].completion; if ("throw" === t.type) throw t.arg; return this.rval; }, dispatchException: function dispatchException(e) { if (this.done) throw e; var r = this; function handle(n, o) { return a.type = "throw", a.arg = e, r.next = n, o && (r.method = "next", r.arg = t), !!o; } for (var o = this.tryEntries.length - 1; o >= 0; --o) { var i = this.tryEntries[o], a = i.completion; if ("root" === i.tryLoc) return handle("end"); if (i.tryLoc <= this.prev) { var c = n.call(i, "catchLoc"), u = n.call(i, "finallyLoc"); if (c && u) { if (this.prev < i.catchLoc) return handle(i.catchLoc, !0); if (this.prev < i.finallyLoc) return handle(i.finallyLoc); } else if (c) { if (this.prev < i.catchLoc) return handle(i.catchLoc, !0); } else { if (!u) throw Error("try statement without catch or finally"); if (this.prev < i.finallyLoc) return handle(i.finallyLoc); } } } }, abrupt: function abrupt(t, e) { for (var r = this.tryEntries.length - 1; r >= 0; --r) { var o = this.tryEntries[r]; if (o.tryLoc <= this.prev && n.call(o, "finallyLoc") && this.prev < o.finallyLoc) { var i = o; break; } } i && ("break" === t || "continue" === t) && i.tryLoc <= e && e <= i.finallyLoc && (i = null); var a = i ? i.completion : {}; return a.type = t, a.arg = e, i ? (this.method = "next", this.next = i.finallyLoc, y) : this.complete(a); }, complete: function complete(t, e) { if ("throw" === t.type) throw t.arg; return "break" === t.type || "continue" === t.type ? this.next = t.arg : "return" === t.type ? (this.rval = this.arg = t.arg, this.method = "return", this.next = "end") : "normal" === t.type && e && (this.next = e), y; }, finish: function finish(t) { for (var e = this.tryEntries.length - 1; e >= 0; --e) { var r = this.tryEntries[e]; if (r.finallyLoc === t) return this.complete(r.completion, r.afterLoc), resetTryEntry(r), y; } }, "catch": function _catch(t) { for (var e = this.tryEntries.length - 1; e >= 0; --e) { var r = this.tryEntries[e]; if (r.tryLoc === t) { var n = r.completion; if ("throw" === n.type) { var o = n.arg; resetTryEntry(r); } return o; } } throw Error("illegal catch attempt"); }, delegateYield: function delegateYield(e, r, n) { return this.delegate = { iterator: values(e), resultName: r, nextLoc: n }, "next" === this.method && (this.arg = t), y; } }, e; }
function asyncGeneratorStep(n, t, e, r, o, a, c) { try { var i = n[a](c), u = i.value; } catch (n) { return void e(n); } i.done ? t(u) : Promise.resolve(u).then(r, o); }
function _asyncToGenerator(n) { return function () { var t = this, e = arguments; return new Promise(function (r, o) { var a = n.apply(t, e); function _next(n) { asyncGeneratorStep(a, r, o, _next, _throw, "next", n); } function _throw(n) { asyncGeneratorStep(a, r, o, _next, _throw, "throw", n); } _next(void 0); }); }; }
function _classCallCheck(a, n) { if (!(a instanceof n)) throw new TypeError("Cannot call a class as a function"); }
function _defineProperties(e, r) { for (var t = 0; t < r.length; t++) { var o = r[t]; o.enumerable = o.enumerable || !1, o.configurable = !0, "value" in o && (o.writable = !0), Object.defineProperty(e, _toPropertyKey(o.key), o); } }
function _createClass(e, r, t) { return r && _defineProperties(e.prototype, r), t && _defineProperties(e, t), Object.defineProperty(e, "prototype", { writable: !1 }), e; }
function _toPropertyKey(t) { var i = _toPrimitive(t, "string"); return "symbol" == _typeof(i) ? i : i + ""; }
function _toPrimitive(t, r) { if ("object" != _typeof(t) || !t) return t; var e = t[Symbol.toPrimitive]; if (void 0 !== e) { var i = e.call(t, r || "default"); if ("object" != _typeof(i)) return i; throw new TypeError("@@toPrimitive must return a primitive value."); } return ("string" === r ? String : Number)(t); }
function _callSuper(t, o, e) { return o = _getPrototypeOf(o), _possibleConstructorReturn(t, _isNativeReflectConstruct() ? Reflect.construct(o, e || [], _getPrototypeOf(t).constructor) : o.apply(t, e)); }
function _possibleConstructorReturn(t, e) { if (e && ("object" == _typeof(e) || "function" == typeof e)) return e; if (void 0 !== e) throw new TypeError("Derived constructors may only return object or undefined"); return _assertThisInitialized(t); }
function _assertThisInitialized(e) { if (void 0 === e) throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); return e; }
function _isNativeReflectConstruct() { try { var t = !Boolean.prototype.valueOf.call(Reflect.construct(Boolean, [], function () {})); } catch (t) {} return (_isNativeReflectConstruct = function _isNativeReflectConstruct() { return !!t; })(); }
function _getPrototypeOf(t) { return _getPrototypeOf = Object.setPrototypeOf ? Object.getPrototypeOf.bind() : function (t) { return t.__proto__ || Object.getPrototypeOf(t); }, _getPrototypeOf(t); }
function _inherits(t, e) { if ("function" != typeof e && null !== e) throw new TypeError("Super expression must either be null or a function"); t.prototype = Object.create(e && e.prototype, { constructor: { value: t, writable: !0, configurable: !0 } }), Object.defineProperty(t, "prototype", { writable: !1 }), e && _setPrototypeOf(t, e); }
function _setPrototypeOf(t, e) { return _setPrototypeOf = Object.setPrototypeOf ? Object.setPrototypeOf.bind() : function (t, e) { return t.__proto__ = e, t; }, _setPrototypeOf(t, e); }
function _awaitAsyncGenerator(e) { return new _OverloadYield(e, 0); }
function _wrapAsyncGenerator(e) { return function () { return new AsyncGenerator(e.apply(this, arguments)); }; }
function AsyncGenerator(e) { var r, t; function resume(r, t) { try { var n = e[r](t), o = n.value, u = o instanceof _OverloadYield; Promise.resolve(u ? o.v : o).then(function (t) { if (u) { var i = "return" === r ? "return" : "next"; if (!o.k || t.done) return resume(i, t); t = e[i](t).value; } settle(n.done ? "return" : "normal", t); }, function (e) { resume("throw", e); }); } catch (e) { settle("throw", e); } } function settle(e, n) { switch (e) { case "return": r.resolve({ value: n, done: !0 }); break; case "throw": r.reject(n); break; default: r.resolve({ value: n, done: !1 }); } (r = r.next) ? resume(r.key, r.arg) : t = null; } this._invoke = function (e, n) { return new Promise(function (o, u) { var i = { key: e, arg: n, resolve: o, reject: u, next: null }; t ? t = t.next = i : (r = t = i, resume(e, n)); }); }, "function" != typeof e["return"] && (this["return"] = void 0); }
AsyncGenerator.prototype["function" == typeof Symbol && Symbol.asyncIterator || "@@asyncIterator"] = function () { return this; }, AsyncGenerator.prototype.next = function (e) { return this._invoke("next", e); }, AsyncGenerator.prototype["throw"] = function (e) { return this._invoke("throw", e); }, AsyncGenerator.prototype["return"] = function (e) { return this._invoke("return", e); };
function _OverloadYield(e, d) { this.v = e, this.k = d; }
function _asyncIterator(r) { var n, t, o, e = 2; for ("undefined" != typeof Symbol && (t = Symbol.asyncIterator, o = Symbol.iterator); e--;) { if (t && null != (n = r[t])) return n.call(r); if (o && null != (n = r[o])) return new AsyncFromSyncIterator(n.call(r)); t = "@@asyncIterator", o = "@@iterator"; } throw new TypeError("Object is not async iterable"); }
function AsyncFromSyncIterator(r) { function AsyncFromSyncIteratorContinuation(r) { if (Object(r) !== r) return Promise.reject(new TypeError(r + " is not an object.")); var n = r.done; return Promise.resolve(r.value).then(function (r) { return { value: r, done: n }; }); } return AsyncFromSyncIterator = function AsyncFromSyncIterator(r) { this.s = r, this.n = r.next; }, AsyncFromSyncIterator.prototype = { s: null, n: null, next: function next() { return AsyncFromSyncIteratorContinuation(this.n.apply(this.s, arguments)); }, "return": function _return(r) { var n = this.s["return"]; return void 0 === n ? Promise.resolve({ value: r, done: !0 }) : AsyncFromSyncIteratorContinuation(n.apply(this.s, arguments)); }, "throw": function _throw(r) { var n = this.s["return"]; return void 0 === n ? Promise.reject(r) : AsyncFromSyncIteratorContinuation(n.apply(this.s, arguments)); } }, new AsyncFromSyncIterator(r); }
var Database = exports.Database = /*#__PURE__*/function (_EventEmitter) {
  function Database(file) {
    var _this2;
    var opts = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
    _classCallCheck(this, Database);
    _this2 = _callSuper(this, Database);
    _this2.opts = Object.assign({
      v8: false,
      index: {
        data: {}
      },
      indexes: {},
      compress: false,
      compressIndex: false,
      maxMemoryUsage: 64 * 1024 // 64KB
    }, opts);
    _this2.offsets = [];
    _this2.shouldSave = false;
    _this2.serializer = new _Serializer.Serializer(_this2.opts);
    _this2.fileHandler = new _FileHandler["default"](file);
    _this2.indexManager = new _IndexManager["default"](_this2.opts);
    _this2.indexOffset = 0;
    _this2.writeBuffer = [];
    return _this2;
  }
  _inherits(Database, _EventEmitter);
  return _createClass(Database, [{
    key: "use",
    value: function use(plugin) {
      if (this.destroyed) throw new Error('Database is destroyed');
      plugin(this);
    }
  }, {
    key: "check",
    value: function () {
      var _check = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee() {
        var lastLine, offsets;
        return _regeneratorRuntime().wrap(function _callee$(_context) {
          while (1) switch (_context.prev = _context.next) {
            case 0:
              if (!this.destroyed) {
                _context.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              _context.next = 4;
              return this.fileHandler.readLastLine();
            case 4:
              lastLine = _context.sent;
              if (!(!lastLine || !lastLine.length)) {
                _context.next = 7;
                break;
              }
              throw new Error('File does not exists or is a empty file');
            case 7:
              _context.next = 9;
              return this.serializer.deserialize(lastLine, {
                compress: this.opts.compressIndex
              });
            case 9:
              offsets = _context.sent;
              if (Array.isArray(offsets)) {
                _context.next = 12;
                break;
              }
              throw new Error('File to parse offsets, expected an array');
            case 12:
              return _context.abrupt("return", offsets.length);
            case 13:
            case "end":
              return _context.stop();
          }
        }, _callee, this);
      }));
      function check() {
        return _check.apply(this, arguments);
      }
      return check;
    }()
  }, {
    key: "init",
    value: function () {
      var _init = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee2() {
        var _this3 = this;
        var _this$fileHandler, lastLine, offsets, ptr, indexLine, index;
        return _regeneratorRuntime().wrap(function _callee2$(_context2) {
          while (1) switch (_context2.prev = _context2.next) {
            case 0:
              if (!this.destroyed) {
                _context2.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (!this.initialized) {
                _context2.next = 4;
                break;
              }
              return _context2.abrupt("return");
            case 4:
              if (!this.initializing) {
                _context2.next = 8;
                break;
              }
              _context2.next = 7;
              return new Promise(function (resolve) {
                return _this3.once('init', resolve);
              });
            case 7:
              return _context2.abrupt("return", _context2.sent);
            case 8:
              this.initializing = true;
              _context2.prev = 9;
              if (!this.opts.clear) {
                _context2.next = 14;
                break;
              }
              _context2.next = 13;
              return this.fileHandler.truncate(0)["catch"](function (err) {
                console.error('[jexidb]', err);
              });
            case 13:
              throw new Error('Cleared, empty file');
            case 14:
              _context2.next = 16;
              return this.fileHandler.readLastLine();
            case 16:
              lastLine = _context2.sent;
              if (!(!lastLine || !lastLine.length)) {
                _context2.next = 19;
                break;
              }
              throw new Error('File does not exists or is a empty file');
            case 19:
              _context2.next = 21;
              return this.serializer.deserialize(lastLine, {
                compress: this.opts.compressIndex
              });
            case 21:
              offsets = _context2.sent;
              if (Array.isArray(offsets)) {
                _context2.next = 24;
                break;
              }
              throw new Error('File to parse offsets, expected an array');
            case 24:
              this.indexOffset = offsets[offsets.length - 2];
              this.offsets = offsets;
              ptr = this.locate(offsets.length - 2);
              this.offsets = this.offsets.slice(0, -2);
              this.shouldTruncate = true;
              _context2.next = 31;
              return (_this$fileHandler = this.fileHandler).readRange.apply(_this$fileHandler, _toConsumableArray(ptr));
            case 31:
              indexLine = _context2.sent;
              _context2.next = 34;
              return this.serializer.deserialize(indexLine, {
                compress: this.opts.compressIndex
              });
            case 34:
              index = _context2.sent;
              index && this.indexManager.load(index);
              _context2.next = 43;
              break;
            case 38:
              _context2.prev = 38;
              _context2.t0 = _context2["catch"](9);
              if (Array.isArray(this.offsets)) {
                this.offsets = [];
              }
              this.indexOffset = 0;
              if (!String(_context2.t0).includes('empty file')) {
                console.error('[jexidb] Error loading database:', _context2.t0);
              }
            case 43:
              _context2.prev = 43;
              this.initializing = false;
              this.initialized = true;
              this.emit('init');
              return _context2.finish(43);
            case 48:
            case "end":
              return _context2.stop();
          }
        }, _callee2, this, [[9, 38, 43, 48]]);
      }));
      function init() {
        return _init.apply(this, arguments);
      }
      return init;
    }()
  }, {
    key: "save",
    value: function () {
      var _save = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee3() {
        var _this4 = this;
        var index, field, term, offsets, indexString, _field, _term, offsetsString;
        return _regeneratorRuntime().wrap(function _callee3$(_context3) {
          while (1) switch (_context3.prev = _context3.next) {
            case 0:
              if (!this.destroyed) {
                _context3.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (this.initialized) {
                _context3.next = 4;
                break;
              }
              throw new Error('Database not initialized');
            case 4:
              if (!this.saving) {
                _context3.next = 6;
                break;
              }
              return _context3.abrupt("return", new Promise(function (resolve) {
                return _this4.once('save', resolve);
              }));
            case 6:
              this.saving = true;
              _context3.next = 9;
              return this.flush();
            case 9:
              if (this.shouldSave) {
                _context3.next = 11;
                break;
              }
              return _context3.abrupt("return");
            case 11:
              this.emit('before-save');
              index = Object.assign({
                data: {}
              }, this.indexManager.index);
              for (field in this.indexManager.index.data) {
                for (term in this.indexManager.index.data[field]) {
                  index.data[field][term] = _toConsumableArray(this.indexManager.index.data[field][term]); // set to array 
                }
              }
              offsets = this.offsets.slice(0);
              _context3.next = 17;
              return this.serializer.serialize(index, {
                compress: this.opts.compressIndex,
                linebreak: true
              });
            case 17:
              indexString = _context3.sent;
              // force linebreak here to allow 'init' to read last line as offsets correctly
              for (_field in this.indexManager.index.data) {
                for (_term in this.indexManager.index.data[_field]) {
                  this.indexManager.index.data[_field][_term] = new Set(index.data[_field][_term]); // set back to set because of serialization
                }
              }
              offsets.push(this.indexOffset);
              offsets.push(this.indexOffset + indexString.length);
              // save offsets as JSON always to prevent linebreaks on last line, which breaks 'init()'
              _context3.next = 23;
              return this.serializer.serialize(offsets, {
                json: true,
                compress: false,
                linebreak: false
              });
            case 23:
              offsetsString = _context3.sent;
              this.writeBuffer.push(indexString);
              this.writeBuffer.push(offsetsString);
              _context3.next = 28;
              return this.flush();
            case 28:
              // write the index and offsets
              this.shouldTruncate = true;
              this.shouldSave = false;
              this.saving = false;
              this.emit('save');
            case 32:
            case "end":
              return _context3.stop();
          }
        }, _callee3, this);
      }));
      function save() {
        return _save.apply(this, arguments);
      }
      return save;
    }()
  }, {
    key: "ready",
    value: function () {
      var _ready = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee4() {
        var _this5 = this;
        return _regeneratorRuntime().wrap(function _callee4$(_context4) {
          while (1) switch (_context4.prev = _context4.next) {
            case 0:
              if (this.initialized) {
                _context4.next = 3;
                break;
              }
              _context4.next = 3;
              return new Promise(function (resolve) {
                return _this5.once('init', resolve);
              });
            case 3:
            case "end":
              return _context4.stop();
          }
        }, _callee4, this);
      }));
      function ready() {
        return _ready.apply(this, arguments);
      }
      return ready;
    }()
  }, {
    key: "locate",
    value: function locate(n) {
      if (this.offsets[n] === undefined) {
        if (this.offsets[n - 1]) {
          return [this.indexOffset, Number.MAX_SAFE_INTEGER];
        }
        return;
      }
      var end = this.offsets[n + 1] || this.indexOffset || Number.MAX_SAFE_INTEGER;
      return [this.offsets[n], end];
    }
  }, {
    key: "getRanges",
    value: function getRanges(map) {
      var _this6 = this;
      return (map || Array.from(this.offsets.keys())).map(function (n) {
        var ret = _this6.locate(n);
        if (ret !== undefined) return {
          start: ret[0],
          end: ret[1],
          index: n
        };
      }).filter(function (n) {
        return n !== undefined;
      });
    }
  }, {
    key: "readLines",
    value: function () {
      var _readLines = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee5(map, ranges) {
        var results, i, start;
        return _regeneratorRuntime().wrap(function _callee5$(_context5) {
          while (1) switch (_context5.prev = _context5.next) {
            case 0:
              if (!ranges) ranges = this.getRanges(map);
              _context5.next = 3;
              return this.fileHandler.readRanges(ranges, this.serializer.deserialize.bind(this.serializer));
            case 3:
              results = _context5.sent;
              i = 0;
              _context5.t0 = _regeneratorRuntime().keys(results);
            case 6:
              if ((_context5.t1 = _context5.t0()).done) {
                _context5.next = 14;
                break;
              }
              start = _context5.t1.value;
              if (!(!results[start] || results[start]._ !== undefined)) {
                _context5.next = 10;
                break;
              }
              return _context5.abrupt("continue", 6);
            case 10:
              while (this.offsets[i] != start && i < map.length) i++; // weak comparison as 'start' is a string
              results[start]._ = map[i++];
              _context5.next = 6;
              break;
            case 14:
              return _context5.abrupt("return", Object.values(results).filter(function (r) {
                return r !== undefined;
              }));
            case 15:
            case "end":
              return _context5.stop();
          }
        }, _callee5, this);
      }));
      function readLines(_x, _x2) {
        return _readLines.apply(this, arguments);
      }
      return readLines;
    }()
  }, {
    key: "insert",
    value: function () {
      var _insert = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee6(data) {
        var line, position;
        return _regeneratorRuntime().wrap(function _callee6$(_context6) {
          while (1) switch (_context6.prev = _context6.next) {
            case 0:
              if (!this.destroyed) {
                _context6.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (this.initialized) {
                _context6.next = 5;
                break;
              }
              _context6.next = 5;
              return this.init();
            case 5:
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              _context6.next = 8;
              return this.serializer.serialize(data, {
                compress: this.opts.compress
              });
            case 8:
              line = _context6.sent;
              // using Buffer for offsets accuracy
              position = this.offsets.length;
              this.offsets.push(this.indexOffset);
              this.indexOffset += line.length;
              this.indexManager.add(data, position);
              this.emit('insert', data, position);
              this.writeBuffer.push(line);
              if (!(!this.flushing && this.currentWriteBufferSize() > this.opts.maxMemoryUsage)) {
                _context6.next = 18;
                break;
              }
              _context6.next = 18;
              return this.flush();
            case 18:
              this.shouldSave = true;
            case 19:
            case "end":
              return _context6.stop();
          }
        }, _callee6, this);
      }));
      function insert(_x3) {
        return _insert.apply(this, arguments);
      }
      return insert;
    }()
  }, {
    key: "currentWriteBufferSize",
    value: function currentWriteBufferSize() {
      var lengths = this.writeBuffer.filter(function (b) {
        return Buffer.isBuffer(b);
      }).map(function (b) {
        return b.length;
      });
      return lengths.reduce(function (a, b) {
        return a + b;
      }, 0);
    }
  }, {
    key: "flush",
    value: function flush() {
      var _this7 = this;
      if (this.flushing) {
        return this.flushing;
      }
      this.flushing = new Promise(function (resolve, reject) {
        if (_this7.destroyed) return reject(new Error('Database is destroyed'));
        if (!_this7.writeBuffer.length) return resolve();
        var err;
        _this7._flush()["catch"](function (e) {
          return err = e;
        })["finally"](function () {
          err ? reject(err) : resolve();
          _this7.flushing = false;
        });
      });
      return this.flushing;
    }
  }, {
    key: "_flush",
    value: function () {
      var _flush2 = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee7() {
        var fd, data, pos;
        return _regeneratorRuntime().wrap(function _callee7$(_context7) {
          while (1) switch (_context7.prev = _context7.next) {
            case 0:
              _context7.next = 2;
              return _fs["default"].promises.open(this.fileHandler.file, 'a');
            case 2:
              fd = _context7.sent;
              _context7.prev = 3;
            case 4:
              if (!this.writeBuffer.length) {
                _context7.next = 23;
                break;
              }
              data = void 0;
              pos = this.writeBuffer.findIndex(function (b) {
                return typeof b === 'number';
              });
              if (!(pos === 0)) {
                _context7.next = 18;
                break;
              }
              _context7.next = 10;
              return fd.close();
            case 10:
              _context7.next = 12;
              return this.fileHandler.truncate(this.writeBuffer.shift());
            case 12:
              _context7.next = 14;
              return _fs["default"].promises.open(this.fileHandler.file, 'a');
            case 14:
              fd = _context7.sent;
              return _context7.abrupt("continue", 4);
            case 18:
              if (pos === -1) {
                data = Buffer.concat(this.writeBuffer);
                this.writeBuffer.length = 0;
              } else {
                data = Buffer.concat(this.writeBuffer.slice(0, pos));
                this.writeBuffer.splice(0, pos);
              }
            case 19:
              _context7.next = 21;
              return fd.write(data);
            case 21:
              _context7.next = 4;
              break;
            case 23:
              this.shouldSave = true;
              _context7.next = 29;
              break;
            case 26:
              _context7.prev = 26;
              _context7.t0 = _context7["catch"](3);
              console.error('[jexidb] Error flushing:', _context7.t0);
            case 29:
              _context7.prev = 29;
              _context7.next = 32;
              return fd.close();
            case 32:
              return _context7.finish(29);
            case 33:
            case "end":
              return _context7.stop();
          }
        }, _callee7, this, [[3, 26, 29, 33]]);
      }));
      function _flush() {
        return _flush2.apply(this, arguments);
      }
      return _flush;
    }()
  }, {
    key: "walk",
    value: function walk(map) {
      var _this = this;
      var options = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};
      return _wrapAsyncGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee8() {
        var m, pool, ranges, _iteratorAbruptCompletion, _didIteratorError, _iteratorError, _iterator, _step, line, _iteratorAbruptCompletion3, _didIteratorError3, _iteratorError3, _iterator3, _step3, entry, _iteratorAbruptCompletion2, _didIteratorError2, _iteratorError2, _iterator2, _step2, _entry;
        return _regeneratorRuntime().wrap(function _callee8$(_context8) {
          while (1) switch (_context8.prev = _context8.next) {
            case 0:
              if (!_this.destroyed) {
                _context8.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (_this.initialized) {
                _context8.next = 5;
                break;
              }
              _context8.next = 5;
              return _awaitAsyncGenerator(_this.init());
            case 5:
              _context8.t0 = _this.shouldSave;
              if (!_context8.t0) {
                _context8.next = 9;
                break;
              }
              _context8.next = 9;
              return _awaitAsyncGenerator(_this.save()["catch"](function (err) {
                console.error('[jexidb]', err);
              }));
            case 9:
              if (!(_this.indexOffset === 0)) {
                _context8.next = 11;
                break;
              }
              return _context8.abrupt("return");
            case 11:
              if (!Array.isArray(map)) {
                if (map instanceof Set) {
                  map = _toConsumableArray(map);
                } else if (map && _typeof(map) === 'object') {
                  map = _toConsumableArray(_this.indexManager.query(map, options.matchAny));
                } else {
                  map = _toConsumableArray(Array(_this.offsets.length).keys());
                }
              }
              m = 0;
              pool = new _Serializer.StreamDeserializer(_this.opts.compress || _this.opts.v8, _this.serializer, _this.offsets);
              ranges = _this.getRanges(map);
              _iteratorAbruptCompletion = false;
              _didIteratorError = false;
              _context8.prev = 17;
              _iterator = _asyncIterator(_this.fileHandler.readRangesEach(ranges));
            case 19:
              _context8.next = 21;
              return _awaitAsyncGenerator(_iterator.next());
            case 21:
              if (!(_iteratorAbruptCompletion = !(_step = _context8.sent).done)) {
                _context8.next = 55;
                break;
              }
              line = _step.value;
              _iteratorAbruptCompletion3 = false;
              _didIteratorError3 = false;
              _context8.prev = 25;
              _iterator3 = _asyncIterator(pool.push(line));
            case 27:
              _context8.next = 29;
              return _awaitAsyncGenerator(_iterator3.next());
            case 29:
              if (!(_iteratorAbruptCompletion3 = !(_step3 = _context8.sent).done)) {
                _context8.next = 36;
                break;
              }
              entry = _step3.value;
              _context8.next = 33;
              return entry;
            case 33:
              _iteratorAbruptCompletion3 = false;
              _context8.next = 27;
              break;
            case 36:
              _context8.next = 42;
              break;
            case 38:
              _context8.prev = 38;
              _context8.t1 = _context8["catch"](25);
              _didIteratorError3 = true;
              _iteratorError3 = _context8.t1;
            case 42:
              _context8.prev = 42;
              _context8.prev = 43;
              if (!(_iteratorAbruptCompletion3 && _iterator3["return"] != null)) {
                _context8.next = 47;
                break;
              }
              _context8.next = 47;
              return _awaitAsyncGenerator(_iterator3["return"]());
            case 47:
              _context8.prev = 47;
              if (!_didIteratorError3) {
                _context8.next = 50;
                break;
              }
              throw _iteratorError3;
            case 50:
              return _context8.finish(47);
            case 51:
              return _context8.finish(42);
            case 52:
              _iteratorAbruptCompletion = false;
              _context8.next = 19;
              break;
            case 55:
              _context8.next = 61;
              break;
            case 57:
              _context8.prev = 57;
              _context8.t2 = _context8["catch"](17);
              _didIteratorError = true;
              _iteratorError = _context8.t2;
            case 61:
              _context8.prev = 61;
              _context8.prev = 62;
              if (!(_iteratorAbruptCompletion && _iterator["return"] != null)) {
                _context8.next = 66;
                break;
              }
              _context8.next = 66;
              return _awaitAsyncGenerator(_iterator["return"]());
            case 66:
              _context8.prev = 66;
              if (!_didIteratorError) {
                _context8.next = 69;
                break;
              }
              throw _iteratorError;
            case 69:
              return _context8.finish(66);
            case 70:
              return _context8.finish(61);
            case 71:
              _iteratorAbruptCompletion2 = false;
              _didIteratorError2 = false;
              _context8.prev = 73;
              _iterator2 = _asyncIterator(pool.end());
            case 75:
              _context8.next = 77;
              return _awaitAsyncGenerator(_iterator2.next());
            case 77:
              if (!(_iteratorAbruptCompletion2 = !(_step2 = _context8.sent).done)) {
                _context8.next = 84;
                break;
              }
              _entry = _step2.value;
              _context8.next = 81;
              return _entry;
            case 81:
              _iteratorAbruptCompletion2 = false;
              _context8.next = 75;
              break;
            case 84:
              _context8.next = 90;
              break;
            case 86:
              _context8.prev = 86;
              _context8.t3 = _context8["catch"](73);
              _didIteratorError2 = true;
              _iteratorError2 = _context8.t3;
            case 90:
              _context8.prev = 90;
              _context8.prev = 91;
              if (!(_iteratorAbruptCompletion2 && _iterator2["return"] != null)) {
                _context8.next = 95;
                break;
              }
              _context8.next = 95;
              return _awaitAsyncGenerator(_iterator2["return"]());
            case 95:
              _context8.prev = 95;
              if (!_didIteratorError2) {
                _context8.next = 98;
                break;
              }
              throw _iteratorError2;
            case 98:
              return _context8.finish(95);
            case 99:
              return _context8.finish(90);
            case 100:
            case "end":
              return _context8.stop();
          }
        }, _callee8, null, [[17, 57, 61, 71], [25, 38, 42, 52], [43,, 47, 51], [62,, 66, 70], [73, 86, 90, 100], [91,, 95, 99]]);
      }))();
    }
  }, {
    key: "query",
    value: function () {
      var _query = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee9(criteria) {
        var options,
          entries,
          _iteratorAbruptCompletion4,
          _didIteratorError4,
          _iteratorError4,
          _iterator4,
          _step4,
          entry,
          _args9 = arguments;
        return _regeneratorRuntime().wrap(function _callee9$(_context9) {
          while (1) switch (_context9.prev = _context9.next) {
            case 0:
              options = _args9.length > 1 && _args9[1] !== undefined ? _args9[1] : {};
              if (!this.destroyed) {
                _context9.next = 3;
                break;
              }
              throw new Error('Database is destroyed');
            case 3:
              if (this.initialized) {
                _context9.next = 6;
                break;
              }
              _context9.next = 6;
              return this.init();
            case 6:
              _context9.t0 = this.shouldSave;
              if (!_context9.t0) {
                _context9.next = 10;
                break;
              }
              _context9.next = 10;
              return this.save()["catch"](function (err) {
                console.error('[jexidb]', err);
              });
            case 10:
              entries = [];
              _iteratorAbruptCompletion4 = false;
              _didIteratorError4 = false;
              _context9.prev = 13;
              _iterator4 = _asyncIterator(this.walk(criteria, options));
            case 15:
              _context9.next = 17;
              return _iterator4.next();
            case 17:
              if (!(_iteratorAbruptCompletion4 = !(_step4 = _context9.sent).done)) {
                _context9.next = 23;
                break;
              }
              entry = _step4.value;
              entries.push(entry);
            case 20:
              _iteratorAbruptCompletion4 = false;
              _context9.next = 15;
              break;
            case 23:
              _context9.next = 29;
              break;
            case 25:
              _context9.prev = 25;
              _context9.t1 = _context9["catch"](13);
              _didIteratorError4 = true;
              _iteratorError4 = _context9.t1;
            case 29:
              _context9.prev = 29;
              _context9.prev = 30;
              if (!(_iteratorAbruptCompletion4 && _iterator4["return"] != null)) {
                _context9.next = 34;
                break;
              }
              _context9.next = 34;
              return _iterator4["return"]();
            case 34:
              _context9.prev = 34;
              if (!_didIteratorError4) {
                _context9.next = 37;
                break;
              }
              throw _iteratorError4;
            case 37:
              return _context9.finish(34);
            case 38:
              return _context9.finish(29);
            case 39:
              return _context9.abrupt("return", entries);
            case 40:
            case "end":
              return _context9.stop();
          }
        }, _callee9, this, [[13, 25, 29, 39], [30,, 34, 38]]);
      }));
      function query(_x4) {
        return _query.apply(this, arguments);
      }
      return query;
    }()
  }, {
    key: "update",
    value: function () {
      var _update = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee10(criteria, data) {
        var _this8 = this;
        var options,
          matchingLines,
          ranges,
          validMatchingLines,
          entries,
          lines,
          _iterator5,
          _step5,
          _loop,
          offsets,
          byteOffset,
          k,
          _args11 = arguments;
        return _regeneratorRuntime().wrap(function _callee10$(_context11) {
          while (1) switch (_context11.prev = _context11.next) {
            case 0:
              options = _args11.length > 2 && _args11[2] !== undefined ? _args11[2] : {};
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              if (!this.destroyed) {
                _context11.next = 4;
                break;
              }
              throw new Error('Database is destroyed');
            case 4:
              if (this.initialized) {
                _context11.next = 7;
                break;
              }
              _context11.next = 7;
              return this.init();
            case 7:
              _context11.t0 = this.shouldSave;
              if (!_context11.t0) {
                _context11.next = 11;
                break;
              }
              _context11.next = 11;
              return this.save()["catch"](function (err) {
                console.error('[jexidb]', err);
              });
            case 11:
              _context11.next = 13;
              return this.indexManager.query(criteria, options.matchAny);
            case 13:
              matchingLines = _context11.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context11.next = 16;
                break;
              }
              return _context11.abrupt("return", []);
            case 16:
              ranges = this.getRanges(_toConsumableArray(matchingLines));
              validMatchingLines = new Set(ranges.map(function (r) {
                return r.index;
              }));
              if (validMatchingLines.size) {
                _context11.next = 20;
                break;
              }
              return _context11.abrupt("return", []);
            case 20:
              _context11.next = 22;
              return this.readLines(_toConsumableArray(validMatchingLines), ranges);
            case 22:
              entries = _context11.sent;
              lines = [];
              _iterator5 = _createForOfIteratorHelper(entries);
              _context11.prev = 25;
              _loop = /*#__PURE__*/_regeneratorRuntime().mark(function _loop() {
                var entry, err, updated, ret;
                return _regeneratorRuntime().wrap(function _loop$(_context10) {
                  while (1) switch (_context10.prev = _context10.next) {
                    case 0:
                      entry = _step5.value;
                      updated = Object.assign(entry, data);
                      _context10.next = 4;
                      return _this8.serializer.serialize(updated)["catch"](function (e) {
                        return err = e;
                      });
                    case 4:
                      ret = _context10.sent;
                      err || lines.push(ret);
                    case 6:
                    case "end":
                      return _context10.stop();
                  }
                }, _loop);
              });
              _iterator5.s();
            case 28:
              if ((_step5 = _iterator5.n()).done) {
                _context11.next = 32;
                break;
              }
              return _context11.delegateYield(_loop(), "t1", 30);
            case 30:
              _context11.next = 28;
              break;
            case 32:
              _context11.next = 37;
              break;
            case 34:
              _context11.prev = 34;
              _context11.t2 = _context11["catch"](25);
              _iterator5.e(_context11.t2);
            case 37:
              _context11.prev = 37;
              _iterator5.f();
              return _context11.finish(37);
            case 40:
              offsets = [];
              byteOffset = 0, k = 0;
              this.offsets.forEach(function (n, i) {
                var prevByteOffset = byteOffset;
                if (validMatchingLines.has(i) && ranges[k]) {
                  var r = ranges[k];
                  byteOffset += lines[k].length - (r.end - r.start);
                  k++;
                }
                offsets.push(n + prevByteOffset);
              });
              this.offsets = offsets;
              this.indexOffset += byteOffset;
              _context11.next = 47;
              return this.fileHandler.replaceLines(ranges, lines);
            case 47:
              _toConsumableArray(validMatchingLines).forEach(function (lineNumber, i) {
                _this8.indexManager.dryRemove(lineNumber);
                _this8.indexManager.add(entries[i], lineNumber);
              });
              this.shouldSave = true;
              return _context11.abrupt("return", entries);
            case 50:
            case "end":
              return _context11.stop();
          }
        }, _callee10, this, [[25, 34, 37, 40]]);
      }));
      function update(_x5, _x6) {
        return _update.apply(this, arguments);
      }
      return update;
    }()
  }, {
    key: "delete",
    value: function () {
      var _delete2 = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee11(criteria) {
        var options,
          matchingLines,
          ranges,
          validMatchingLines,
          offsets,
          byteOffset,
          k,
          _args12 = arguments;
        return _regeneratorRuntime().wrap(function _callee11$(_context12) {
          while (1) switch (_context12.prev = _context12.next) {
            case 0:
              options = _args12.length > 1 && _args12[1] !== undefined ? _args12[1] : {};
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              if (!this.destroyed) {
                _context12.next = 4;
                break;
              }
              throw new Error('Database is destroyed');
            case 4:
              if (this.initialized) {
                _context12.next = 7;
                break;
              }
              _context12.next = 7;
              return this.init();
            case 7:
              _context12.t0 = this.shouldSave;
              if (!_context12.t0) {
                _context12.next = 11;
                break;
              }
              _context12.next = 11;
              return this.save()["catch"](function (err) {
                console.error('[jexidb]', err);
              });
            case 11:
              _context12.next = 13;
              return this.indexManager.query(criteria, options.matchAny);
            case 13:
              matchingLines = _context12.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context12.next = 16;
                break;
              }
              return _context12.abrupt("return", 0);
            case 16:
              ranges = this.getRanges(_toConsumableArray(matchingLines));
              validMatchingLines = new Set(ranges.map(function (r) {
                return r.index;
              }));
              _context12.next = 20;
              return this.fileHandler.replaceLines(ranges, []);
            case 20:
              offsets = [];
              byteOffset = 0, k = 0;
              this.offsets.forEach(function (n, i) {
                if (validMatchingLines.has(i)) {
                  var r = ranges[k];
                  byteOffset -= r.end - r.start;
                  k++;
                } else {
                  offsets.push(n + byteOffset);
                }
              });
              this.offsets = offsets;
              this.indexOffset += byteOffset;
              this.indexManager.remove(_toConsumableArray(validMatchingLines));
              this.shouldSave = true;
              return _context12.abrupt("return", ranges.length);
            case 28:
            case "end":
              return _context12.stop();
          }
        }, _callee11, this);
      }));
      function _delete(_x7) {
        return _delete2.apply(this, arguments);
      }
      return _delete;
    }()
  }, {
    key: "destroy",
    value: function () {
      var _destroy = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee12() {
        return _regeneratorRuntime().wrap(function _callee12$(_context13) {
          while (1) switch (_context13.prev = _context13.next) {
            case 0:
              _context13.t0 = this.shouldSave;
              if (!_context13.t0) {
                _context13.next = 4;
                break;
              }
              _context13.next = 4;
              return this.save()["catch"](function (err) {
                console.error('[jexidb]', err);
              });
            case 4:
              this.destroyed = true;
              this.indexOffset = 0;
              this.indexManager.index = {};
              this.writeBuffer.length = 0;
              this.initialized = false;
              this.fileHandler.destroy();
            case 10:
            case "end":
              return _context13.stop();
          }
        }, _callee12, this);
      }));
      function destroy() {
        return _destroy.apply(this, arguments);
      }
      return destroy;
    }()
  }, {
    key: "length",
    get: function get() {
      var _this$offsets;
      return (this === null || this === void 0 || (_this$offsets = this.offsets) === null || _this$offsets === void 0 ? void 0 : _this$offsets.length) || 0;
    }
  }, {
    key: "index",
    get: function get() {
      return this.indexManager.index;
    }
  }]);
}(_events.EventEmitter);