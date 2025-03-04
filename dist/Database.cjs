"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Database = void 0;
var _events = require("events");
var _FileHandler = _interopRequireDefault(require("./FileHandler.mjs"));
var _IndexManager = _interopRequireDefault(require("./IndexManager.mjs"));
var _Serializer = _interopRequireDefault(require("./Serializer.mjs"));
var _asyncMutex = require("async-mutex");
var _fs = _interopRequireDefault(require("fs"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
function _defineProperty(e, r, t) { return (r = _toPropertyKey(r)) in e ? Object.defineProperty(e, r, { value: t, enumerable: !0, configurable: !0, writable: !0 }) : e[r] = t, e; }
function _slicedToArray(r, e) { return _arrayWithHoles(r) || _iterableToArrayLimit(r, e) || _unsupportedIterableToArray(r, e) || _nonIterableRest(); }
function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _iterableToArrayLimit(r, l) { var t = null == r ? null : "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (null != t) { var e, n, i, u, a = [], f = !0, o = !1; try { if (i = (t = t.call(r)).next, 0 === l) { if (Object(t) !== t) return; f = !1; } else for (; !(f = (e = i.call(t)).done) && (a.push(e.value), a.length !== l); f = !0); } catch (r) { o = !0, n = r; } finally { try { if (!f && null != t["return"] && (u = t["return"](), Object(u) !== u)) return; } finally { if (o) throw n; } } return a; } }
function _arrayWithHoles(r) { if (Array.isArray(r)) return r; }
function _createForOfIteratorHelper(r, e) { var t = "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (!t) { if (Array.isArray(r) || (t = _unsupportedIterableToArray(r)) || e && r && "number" == typeof r.length) { t && (r = t); var _n = 0, F = function F() {}; return { s: F, n: function n() { return _n >= r.length ? { done: !0 } : { done: !1, value: r[_n++] }; }, e: function e(r) { throw r; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var o, a = !0, u = !1; return { s: function s() { t = t.call(r); }, n: function n() { var r = t.next(); return a = r.done, r; }, e: function e(r) { u = !0, o = r; }, f: function f() { try { a || null == t["return"] || t["return"](); } finally { if (u) throw o; } } }; }
function _typeof(o) { "@babel/helpers - typeof"; return _typeof = "function" == typeof Symbol && "symbol" == typeof Symbol.iterator ? function (o) { return typeof o; } : function (o) { return o && "function" == typeof Symbol && o.constructor === Symbol && o !== Symbol.prototype ? "symbol" : typeof o; }, _typeof(o); }
function _regeneratorRuntime() { "use strict"; /*! regenerator-runtime -- Copyright (c) 2014-present, Facebook, Inc. -- license (MIT): https://github.com/facebook/regenerator/blob/main/LICENSE */ _regeneratorRuntime = function _regeneratorRuntime() { return e; }; var t, e = {}, r = Object.prototype, n = r.hasOwnProperty, o = Object.defineProperty || function (t, e, r) { t[e] = r.value; }, i = "function" == typeof Symbol ? Symbol : {}, a = i.iterator || "@@iterator", c = i.asyncIterator || "@@asyncIterator", u = i.toStringTag || "@@toStringTag"; function define(t, e, r) { return Object.defineProperty(t, e, { value: r, enumerable: !0, configurable: !0, writable: !0 }), t[e]; } try { define({}, ""); } catch (t) { define = function define(t, e, r) { return t[e] = r; }; } function wrap(t, e, r, n) { var i = e && e.prototype instanceof Generator ? e : Generator, a = Object.create(i.prototype), c = new Context(n || []); return o(a, "_invoke", { value: makeInvokeMethod(t, r, c) }), a; } function tryCatch(t, e, r) { try { return { type: "normal", arg: t.call(e, r) }; } catch (t) { return { type: "throw", arg: t }; } } e.wrap = wrap; var h = "suspendedStart", l = "suspendedYield", f = "executing", s = "completed", y = {}; function Generator() {} function GeneratorFunction() {} function GeneratorFunctionPrototype() {} var p = {}; define(p, a, function () { return this; }); var d = Object.getPrototypeOf, v = d && d(d(values([]))); v && v !== r && n.call(v, a) && (p = v); var g = GeneratorFunctionPrototype.prototype = Generator.prototype = Object.create(p); function defineIteratorMethods(t) { ["next", "throw", "return"].forEach(function (e) { define(t, e, function (t) { return this._invoke(e, t); }); }); } function AsyncIterator(t, e) { function invoke(r, o, i, a) { var c = tryCatch(t[r], t, o); if ("throw" !== c.type) { var u = c.arg, h = u.value; return h && "object" == _typeof(h) && n.call(h, "__await") ? e.resolve(h.__await).then(function (t) { invoke("next", t, i, a); }, function (t) { invoke("throw", t, i, a); }) : e.resolve(h).then(function (t) { u.value = t, i(u); }, function (t) { return invoke("throw", t, i, a); }); } a(c.arg); } var r; o(this, "_invoke", { value: function value(t, n) { function callInvokeWithMethodAndArg() { return new e(function (e, r) { invoke(t, n, e, r); }); } return r = r ? r.then(callInvokeWithMethodAndArg, callInvokeWithMethodAndArg) : callInvokeWithMethodAndArg(); } }); } function makeInvokeMethod(e, r, n) { var o = h; return function (i, a) { if (o === f) throw Error("Generator is already running"); if (o === s) { if ("throw" === i) throw a; return { value: t, done: !0 }; } for (n.method = i, n.arg = a;;) { var c = n.delegate; if (c) { var u = maybeInvokeDelegate(c, n); if (u) { if (u === y) continue; return u; } } if ("next" === n.method) n.sent = n._sent = n.arg;else if ("throw" === n.method) { if (o === h) throw o = s, n.arg; n.dispatchException(n.arg); } else "return" === n.method && n.abrupt("return", n.arg); o = f; var p = tryCatch(e, r, n); if ("normal" === p.type) { if (o = n.done ? s : l, p.arg === y) continue; return { value: p.arg, done: n.done }; } "throw" === p.type && (o = s, n.method = "throw", n.arg = p.arg); } }; } function maybeInvokeDelegate(e, r) { var n = r.method, o = e.iterator[n]; if (o === t) return r.delegate = null, "throw" === n && e.iterator["return"] && (r.method = "return", r.arg = t, maybeInvokeDelegate(e, r), "throw" === r.method) || "return" !== n && (r.method = "throw", r.arg = new TypeError("The iterator does not provide a '" + n + "' method")), y; var i = tryCatch(o, e.iterator, r.arg); if ("throw" === i.type) return r.method = "throw", r.arg = i.arg, r.delegate = null, y; var a = i.arg; return a ? a.done ? (r[e.resultName] = a.value, r.next = e.nextLoc, "return" !== r.method && (r.method = "next", r.arg = t), r.delegate = null, y) : a : (r.method = "throw", r.arg = new TypeError("iterator result is not an object"), r.delegate = null, y); } function pushTryEntry(t) { var e = { tryLoc: t[0] }; 1 in t && (e.catchLoc = t[1]), 2 in t && (e.finallyLoc = t[2], e.afterLoc = t[3]), this.tryEntries.push(e); } function resetTryEntry(t) { var e = t.completion || {}; e.type = "normal", delete e.arg, t.completion = e; } function Context(t) { this.tryEntries = [{ tryLoc: "root" }], t.forEach(pushTryEntry, this), this.reset(!0); } function values(e) { if (e || "" === e) { var r = e[a]; if (r) return r.call(e); if ("function" == typeof e.next) return e; if (!isNaN(e.length)) { var o = -1, i = function next() { for (; ++o < e.length;) if (n.call(e, o)) return next.value = e[o], next.done = !1, next; return next.value = t, next.done = !0, next; }; return i.next = i; } } throw new TypeError(_typeof(e) + " is not iterable"); } return GeneratorFunction.prototype = GeneratorFunctionPrototype, o(g, "constructor", { value: GeneratorFunctionPrototype, configurable: !0 }), o(GeneratorFunctionPrototype, "constructor", { value: GeneratorFunction, configurable: !0 }), GeneratorFunction.displayName = define(GeneratorFunctionPrototype, u, "GeneratorFunction"), e.isGeneratorFunction = function (t) { var e = "function" == typeof t && t.constructor; return !!e && (e === GeneratorFunction || "GeneratorFunction" === (e.displayName || e.name)); }, e.mark = function (t) { return Object.setPrototypeOf ? Object.setPrototypeOf(t, GeneratorFunctionPrototype) : (t.__proto__ = GeneratorFunctionPrototype, define(t, u, "GeneratorFunction")), t.prototype = Object.create(g), t; }, e.awrap = function (t) { return { __await: t }; }, defineIteratorMethods(AsyncIterator.prototype), define(AsyncIterator.prototype, c, function () { return this; }), e.AsyncIterator = AsyncIterator, e.async = function (t, r, n, o, i) { void 0 === i && (i = Promise); var a = new AsyncIterator(wrap(t, r, n, o), i); return e.isGeneratorFunction(r) ? a : a.next().then(function (t) { return t.done ? t.value : a.next(); }); }, defineIteratorMethods(g), define(g, u, "Generator"), define(g, a, function () { return this; }), define(g, "toString", function () { return "[object Generator]"; }), e.keys = function (t) { var e = Object(t), r = []; for (var n in e) r.push(n); return r.reverse(), function next() { for (; r.length;) { var t = r.pop(); if (t in e) return next.value = t, next.done = !1, next; } return next.done = !0, next; }; }, e.values = values, Context.prototype = { constructor: Context, reset: function reset(e) { if (this.prev = 0, this.next = 0, this.sent = this._sent = t, this.done = !1, this.delegate = null, this.method = "next", this.arg = t, this.tryEntries.forEach(resetTryEntry), !e) for (var r in this) "t" === r.charAt(0) && n.call(this, r) && !isNaN(+r.slice(1)) && (this[r] = t); }, stop: function stop() { this.done = !0; var t = this.tryEntries[0].completion; if ("throw" === t.type) throw t.arg; return this.rval; }, dispatchException: function dispatchException(e) { if (this.done) throw e; var r = this; function handle(n, o) { return a.type = "throw", a.arg = e, r.next = n, o && (r.method = "next", r.arg = t), !!o; } for (var o = this.tryEntries.length - 1; o >= 0; --o) { var i = this.tryEntries[o], a = i.completion; if ("root" === i.tryLoc) return handle("end"); if (i.tryLoc <= this.prev) { var c = n.call(i, "catchLoc"), u = n.call(i, "finallyLoc"); if (c && u) { if (this.prev < i.catchLoc) return handle(i.catchLoc, !0); if (this.prev < i.finallyLoc) return handle(i.finallyLoc); } else if (c) { if (this.prev < i.catchLoc) return handle(i.catchLoc, !0); } else { if (!u) throw Error("try statement without catch or finally"); if (this.prev < i.finallyLoc) return handle(i.finallyLoc); } } } }, abrupt: function abrupt(t, e) { for (var r = this.tryEntries.length - 1; r >= 0; --r) { var o = this.tryEntries[r]; if (o.tryLoc <= this.prev && n.call(o, "finallyLoc") && this.prev < o.finallyLoc) { var i = o; break; } } i && ("break" === t || "continue" === t) && i.tryLoc <= e && e <= i.finallyLoc && (i = null); var a = i ? i.completion : {}; return a.type = t, a.arg = e, i ? (this.method = "next", this.next = i.finallyLoc, y) : this.complete(a); }, complete: function complete(t, e) { if ("throw" === t.type) throw t.arg; return "break" === t.type || "continue" === t.type ? this.next = t.arg : "return" === t.type ? (this.rval = this.arg = t.arg, this.method = "return", this.next = "end") : "normal" === t.type && e && (this.next = e), y; }, finish: function finish(t) { for (var e = this.tryEntries.length - 1; e >= 0; --e) { var r = this.tryEntries[e]; if (r.finallyLoc === t) return this.complete(r.completion, r.afterLoc), resetTryEntry(r), y; } }, "catch": function _catch(t) { for (var e = this.tryEntries.length - 1; e >= 0; --e) { var r = this.tryEntries[e]; if (r.tryLoc === t) { var n = r.completion; if ("throw" === n.type) { var o = n.arg; resetTryEntry(r); } return o; } } throw Error("illegal catch attempt"); }, delegateYield: function delegateYield(e, r, n) { return this.delegate = { iterator: values(e), resultName: r, nextLoc: n }, "next" === this.method && (this.arg = t), y; } }, e; }
function _toConsumableArray(r) { return _arrayWithoutHoles(r) || _iterableToArray(r) || _unsupportedIterableToArray(r) || _nonIterableSpread(); }
function _nonIterableSpread() { throw new TypeError("Invalid attempt to spread non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _unsupportedIterableToArray(r, a) { if (r) { if ("string" == typeof r) return _arrayLikeToArray(r, a); var t = {}.toString.call(r).slice(8, -1); return "Object" === t && r.constructor && (t = r.constructor.name), "Map" === t || "Set" === t ? Array.from(r) : "Arguments" === t || /^(?:Ui|I)nt(?:8|16|32)(?:Clamped)?Array$/.test(t) ? _arrayLikeToArray(r, a) : void 0; } }
function _iterableToArray(r) { if ("undefined" != typeof Symbol && null != r[Symbol.iterator] || null != r["@@iterator"]) return Array.from(r); }
function _arrayWithoutHoles(r) { if (Array.isArray(r)) return _arrayLikeToArray(r); }
function _arrayLikeToArray(r, a) { (null == a || a > r.length) && (a = r.length); for (var e = 0, n = Array(a); e < a; e++) n[e] = r[e]; return n; }
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
    _this2.serializer = new _Serializer["default"](_this2.opts);
    _this2.fileHandler = new _FileHandler["default"](file);
    _this2.indexManager = new _IndexManager["default"](_this2.opts);
    _this2.indexOffset = 0;
    _this2.writeBuffer = [];
    _this2.mutex = new _asyncMutex.Mutex();
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
    key: "init",
    value: function () {
      var _init = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee() {
        var _this3 = this;
        var _this$fileHandler, lastLine, offsets, ptr, indexLine, index;
        return _regeneratorRuntime().wrap(function _callee$(_context) {
          while (1) switch (_context.prev = _context.next) {
            case 0:
              if (!this.destroyed) {
                _context.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (!this.initialized) {
                _context.next = 4;
                break;
              }
              return _context.abrupt("return");
            case 4:
              if (!this.initlializing) {
                _context.next = 8;
                break;
              }
              _context.next = 7;
              return new Promise(function (resolve) {
                return _this3.once('init', resolve);
              });
            case 7:
              return _context.abrupt("return", _context.sent);
            case 8:
              this.initializing = true;
              _context.prev = 9;
              if (!this.opts.clear) {
                _context.next = 14;
                break;
              }
              _context.next = 13;
              return this.fileHandler.truncate(0)["catch"](console.error);
            case 13:
              throw new Error('Cleared, empty file');
            case 14:
              _context.next = 16;
              return this.fileHandler.readLastLine();
            case 16:
              lastLine = _context.sent;
              if (!(!lastLine || !lastLine.length)) {
                _context.next = 19;
                break;
              }
              throw new Error('File does not exists or is a empty file');
            case 19:
              _context.next = 21;
              return this.serializer.deserialize(lastLine, {
                compress: this.opts.compressIndex
              });
            case 21:
              offsets = _context.sent;
              if (Array.isArray(offsets)) {
                _context.next = 24;
                break;
              }
              throw new Error('File to parse offsets, expected an array');
            case 24:
              this.indexOffset = offsets[offsets.length - 2];
              this.offsets = offsets;
              ptr = this.locate(offsets.length - 2);
              this.offsets = this.offsets.slice(0, -2);
              this.shouldTruncate = true;
              _context.next = 31;
              return (_this$fileHandler = this.fileHandler).readRange.apply(_this$fileHandler, _toConsumableArray(ptr));
            case 31:
              indexLine = _context.sent;
              _context.next = 34;
              return this.serializer.deserialize(indexLine, {
                compress: this.opts.compressIndex
              });
            case 34:
              index = _context.sent;
              index && this.indexManager.load(index);
              _context.next = 43;
              break;
            case 38:
              _context.prev = 38;
              _context.t0 = _context["catch"](9);
              if (Array.isArray(this.offsets)) {
                this.offsets = [];
              }
              this.indexOffset = 0;
              if (!String(_context.t0).includes('empty file')) {
                console.error('Error loading database:', _context.t0);
              }
            case 43:
              _context.prev = 43;
              this.initializing = false;
              this.initialized = true;
              this.emit('init');
              return _context.finish(43);
            case 48:
            case "end":
              return _context.stop();
          }
        }, _callee, this, [[9, 38, 43, 48]]);
      }));
      function init() {
        return _init.apply(this, arguments);
      }
      return init;
    }()
  }, {
    key: "save",
    value: function () {
      var _save = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee2() {
        var _this4 = this;
        var index, field, term, offsets, indexString, _field, _term, offsetsString;
        return _regeneratorRuntime().wrap(function _callee2$(_context2) {
          while (1) switch (_context2.prev = _context2.next) {
            case 0:
              if (!this.destroyed) {
                _context2.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (this.initialized) {
                _context2.next = 4;
                break;
              }
              throw new Error('Database not initialized');
            case 4:
              if (!this.saving) {
                _context2.next = 6;
                break;
              }
              return _context2.abrupt("return", new Promise(function (resolve) {
                return _this4.once('save', resolve);
              }));
            case 6:
              this.saving = true;
              _context2.next = 9;
              return this.flush();
            case 9:
              if (this.shouldSave) {
                _context2.next = 11;
                break;
              }
              return _context2.abrupt("return");
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
              _context2.next = 17;
              return this.serializer.serialize(index, {
                compress: this.opts.compressIndex,
                linebreak: true
              });
            case 17:
              indexString = _context2.sent;
              // force linebreak here to allow 'init' to read last line as offsets correctly
              for (_field in this.indexManager.index.data) {
                for (_term in this.indexManager.index.data[_field]) {
                  this.indexManager.index.data[_field][_term] = new Set(index.data[_field][_term]); // set back to set because of serialization
                }
              }
              offsets.push(this.indexOffset);
              offsets.push(this.indexOffset + indexString.length);
              // save offsets as JSON always to prevent linebreaks on last line, which breaks 'init()'
              _context2.next = 23;
              return this.serializer.serialize(offsets, {
                json: true,
                compress: false,
                linebreak: false
              });
            case 23:
              offsetsString = _context2.sent;
              this.writeBuffer.push(indexString);
              this.writeBuffer.push(offsetsString);
              _context2.next = 28;
              return this.flush();
            case 28:
              // write the index and offsets
              this.shouldTruncate = true;
              this.shouldSave = false;
              this.saving = false;
              this.emit('save');
            case 32:
            case "end":
              return _context2.stop();
          }
        }, _callee2, this);
      }));
      function save() {
        return _save.apply(this, arguments);
      }
      return save;
    }()
  }, {
    key: "ready",
    value: function () {
      var _ready = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee3() {
        var _this5 = this;
        return _regeneratorRuntime().wrap(function _callee3$(_context3) {
          while (1) switch (_context3.prev = _context3.next) {
            case 0:
              if (this.initialized) {
                _context3.next = 3;
                break;
              }
              _context3.next = 3;
              return new Promise(function (resolve) {
                return _this5.once('init', resolve);
              });
            case 3:
            case "end":
              return _context3.stop();
          }
        }, _callee3, this);
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
      var _readLines = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee4(map, ranges) {
        var results, i, start;
        return _regeneratorRuntime().wrap(function _callee4$(_context4) {
          while (1) switch (_context4.prev = _context4.next) {
            case 0:
              if (!ranges) ranges = this.getRanges(map);
              _context4.next = 3;
              return this.fileHandler.readRanges(ranges, this.serializer.deserialize.bind(this.serializer));
            case 3:
              results = _context4.sent;
              i = 0;
              _context4.t0 = _regeneratorRuntime().keys(results);
            case 6:
              if ((_context4.t1 = _context4.t0()).done) {
                _context4.next = 14;
                break;
              }
              start = _context4.t1.value;
              if (!(!results[start] || results[start]._ !== undefined)) {
                _context4.next = 10;
                break;
              }
              return _context4.abrupt("continue", 6);
            case 10:
              while (this.offsets[i] != start && i < map.length) i++; // weak comparison as 'start' is a string
              results[start]._ = map[i++];
              _context4.next = 6;
              break;
            case 14:
              return _context4.abrupt("return", Object.values(results).filter(function (r) {
                return r !== undefined;
              }));
            case 15:
            case "end":
              return _context4.stop();
          }
        }, _callee4, this);
      }));
      function readLines(_x, _x2) {
        return _readLines.apply(this, arguments);
      }
      return readLines;
    }()
  }, {
    key: "insert",
    value: function () {
      var _insert = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee5(data) {
        var line, position;
        return _regeneratorRuntime().wrap(function _callee5$(_context5) {
          while (1) switch (_context5.prev = _context5.next) {
            case 0:
              if (!this.destroyed) {
                _context5.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (this.initialized) {
                _context5.next = 5;
                break;
              }
              _context5.next = 5;
              return this.init();
            case 5:
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              _context5.next = 8;
              return this.serializer.serialize(data, {
                compress: this.opts.compress
              });
            case 8:
              line = _context5.sent;
              // using Buffer for offsets accuracy
              position = this.offsets.length;
              this.offsets.push(this.indexOffset);
              this.indexOffset += line.length;
              this.emit('insert', data, position);
              this.writeBuffer.push(line);
              if (!(!this.flushing && this.currentWriteBufferSize() > this.opts.maxMemoryUsage)) {
                _context5.next = 17;
                break;
              }
              _context5.next = 17;
              return this.flush();
            case 17:
              this.indexManager.add(data, position);
              this.shouldSave = true;
            case 19:
            case "end":
              return _context5.stop();
          }
        }, _callee5, this);
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
      return this.flushing = new Promise(function (resolve, reject) {
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
    }
  }, {
    key: "_flush",
    value: function () {
      var _flush2 = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee6() {
        var release, fd, data, pos, err;
        return _regeneratorRuntime().wrap(function _callee6$(_context6) {
          while (1) switch (_context6.prev = _context6.next) {
            case 0:
              _context6.next = 2;
              return this.mutex.acquire();
            case 2:
              release = _context6.sent;
              _context6.next = 5;
              return _fs["default"].promises.open(this.fileHandler.file, 'a');
            case 5:
              fd = _context6.sent;
              _context6.prev = 6;
            case 7:
              if (!this.writeBuffer.length) {
                _context6.next = 26;
                break;
              }
              data = void 0;
              pos = this.writeBuffer.findIndex(function (b) {
                return typeof b === 'number';
              });
              if (!(pos === 0)) {
                _context6.next = 21;
                break;
              }
              _context6.next = 13;
              return fd.close();
            case 13:
              _context6.next = 15;
              return this.fileHandler.truncate(this.writeBuffer.shift());
            case 15:
              _context6.next = 17;
              return _fs["default"].promises.open(this.fileHandler.file, 'a');
            case 17:
              fd = _context6.sent;
              return _context6.abrupt("continue", 7);
            case 21:
              if (pos === -1) {
                data = Buffer.concat(this.writeBuffer);
                this.writeBuffer.length = 0;
              } else {
                data = Buffer.concat(this.writeBuffer.slice(0, pos));
                this.writeBuffer.splice(0, pos);
              }
            case 22:
              _context6.next = 24;
              return fd.write(data);
            case 24:
              _context6.next = 7;
              break;
            case 26:
              this.shouldSave = true;
              _context6.next = 32;
              break;
            case 29:
              _context6.prev = 29;
              _context6.t0 = _context6["catch"](6);
              console.error('Error flushing:', _context6.t0);
            case 32:
              _context6.prev = 32;
              _context6.next = 35;
              return fd.close()["catch"](function (e) {
                return err = e;
              });
            case 35:
              release();
              err && console.error('Error closing file:', err);
              return _context6.finish(32);
            case 38:
            case "end":
              return _context6.stop();
          }
        }, _callee6, this, [[6, 29, 32, 38]]);
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
      return _wrapAsyncGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee7() {
        var ranges, readSize, groupedRanges, fd, _iterator2, _step2, groupedRange, _iteratorAbruptCompletion, _didIteratorError, _iteratorError, _iterator, _step, row, entry;
        return _regeneratorRuntime().wrap(function _callee7$(_context7) {
          while (1) switch (_context7.prev = _context7.next) {
            case 0:
              if (!_this.destroyed) {
                _context7.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              if (_this.initialized) {
                _context7.next = 5;
                break;
              }
              _context7.next = 5;
              return _awaitAsyncGenerator(_this.init());
            case 5:
              _context7.t0 = _this.shouldSave;
              if (!_context7.t0) {
                _context7.next = 9;
                break;
              }
              _context7.next = 9;
              return _awaitAsyncGenerator(_this.save()["catch"](console.error));
            case 9:
              if (!(_this.indexOffset === 0)) {
                _context7.next = 11;
                break;
              }
              return _context7.abrupt("return");
            case 11:
              if (!Array.isArray(map)) {
                if (map instanceof Set) {
                  map = _toConsumableArray(map);
                } else if (map && _typeof(map) === 'object') {
                  map = _toConsumableArray(_this.indexManager.query(map, options));
                } else {
                  map = _toConsumableArray(Array(_this.offsets.length).keys());
                }
              }
              ranges = _this.getRanges(map);
              readSize = 512 * 1024; // 512KB
              _context7.next = 16;
              return _awaitAsyncGenerator(_this.fileHandler.groupedRanges(ranges));
            case 16:
              groupedRanges = _context7.sent;
              _context7.next = 19;
              return _awaitAsyncGenerator(_fs["default"].promises.open(_this.fileHandler.file, 'r'));
            case 19:
              fd = _context7.sent;
              _context7.prev = 20;
              _iterator2 = _createForOfIteratorHelper(groupedRanges);
              _context7.prev = 22;
              _iterator2.s();
            case 24:
              if ((_step2 = _iterator2.n()).done) {
                _context7.next = 67;
                break;
              }
              groupedRange = _step2.value;
              _iteratorAbruptCompletion = false;
              _didIteratorError = false;
              _context7.prev = 28;
              _iterator = _asyncIterator(_this.fileHandler.readGroupedRange(groupedRange, fd));
            case 30:
              _context7.next = 32;
              return _awaitAsyncGenerator(_iterator.next());
            case 32:
              if (!(_iteratorAbruptCompletion = !(_step = _context7.sent).done)) {
                _context7.next = 49;
                break;
              }
              row = _step.value;
              _context7.next = 36;
              return _awaitAsyncGenerator(_this.serializer.deserialize(row.line, {
                compress: _this.opts.compress
              }));
            case 36:
              entry = _context7.sent;
              if (!(entry === null)) {
                _context7.next = 39;
                break;
              }
              return _context7.abrupt("continue", 46);
            case 39:
              if (!options.includeOffsets) {
                _context7.next = 44;
                break;
              }
              _context7.next = 42;
              return {
                entry: entry,
                start: row.start
              };
            case 42:
              _context7.next = 46;
              break;
            case 44:
              _context7.next = 46;
              return entry;
            case 46:
              _iteratorAbruptCompletion = false;
              _context7.next = 30;
              break;
            case 49:
              _context7.next = 55;
              break;
            case 51:
              _context7.prev = 51;
              _context7.t1 = _context7["catch"](28);
              _didIteratorError = true;
              _iteratorError = _context7.t1;
            case 55:
              _context7.prev = 55;
              _context7.prev = 56;
              if (!(_iteratorAbruptCompletion && _iterator["return"] != null)) {
                _context7.next = 60;
                break;
              }
              _context7.next = 60;
              return _awaitAsyncGenerator(_iterator["return"]());
            case 60:
              _context7.prev = 60;
              if (!_didIteratorError) {
                _context7.next = 63;
                break;
              }
              throw _iteratorError;
            case 63:
              return _context7.finish(60);
            case 64:
              return _context7.finish(55);
            case 65:
              _context7.next = 24;
              break;
            case 67:
              _context7.next = 72;
              break;
            case 69:
              _context7.prev = 69;
              _context7.t2 = _context7["catch"](22);
              _iterator2.e(_context7.t2);
            case 72:
              _context7.prev = 72;
              _iterator2.f();
              return _context7.finish(72);
            case 75:
              _context7.prev = 75;
              _context7.next = 78;
              return _awaitAsyncGenerator(fd.close());
            case 78:
              return _context7.finish(75);
            case 79:
            case "end":
              return _context7.stop();
          }
        }, _callee7, null, [[20,, 75, 79], [22, 69, 72, 75], [28, 51, 55, 65], [56,, 60, 64]]);
      }))();
    }
  }, {
    key: "query",
    value: function () {
      var _query = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee8(criteria) {
        var options,
          results,
          _options$orderBy$spli,
          _options$orderBy$spli2,
          field,
          _options$orderBy$spli3,
          direction,
          matchingLines,
          _args8 = arguments;
        return _regeneratorRuntime().wrap(function _callee8$(_context8) {
          while (1) switch (_context8.prev = _context8.next) {
            case 0:
              options = _args8.length > 1 && _args8[1] !== undefined ? _args8[1] : {};
              if (!this.destroyed) {
                _context8.next = 3;
                break;
              }
              throw new Error('Database is destroyed');
            case 3:
              if (this.initialized) {
                _context8.next = 6;
                break;
              }
              _context8.next = 6;
              return this.init();
            case 6:
              _context8.t0 = this.shouldSave;
              if (!_context8.t0) {
                _context8.next = 10;
                break;
              }
              _context8.next = 10;
              return this.save()["catch"](console.error);
            case 10:
              if (!Array.isArray(criteria)) {
                _context8.next = 19;
                break;
              }
              _context8.next = 13;
              return this.readLines(criteria);
            case 13:
              results = _context8.sent;
              if (options.orderBy) {
                _options$orderBy$spli = options.orderBy.split(' '), _options$orderBy$spli2 = _slicedToArray(_options$orderBy$spli, 2), field = _options$orderBy$spli2[0], _options$orderBy$spli3 = _options$orderBy$spli2[1], direction = _options$orderBy$spli3 === void 0 ? 'asc' : _options$orderBy$spli3;
                results.sort(function (a, b) {
                  if (a[field] > b[field]) return direction === 'asc' ? 1 : -1;
                  if (a[field] < b[field]) return direction === 'asc' ? -1 : 1;
                  return 0;
                });
              }
              if (options.limit) {
                results = results.slice(0, options.limit);
              }
              return _context8.abrupt("return", results);
            case 19:
              _context8.next = 21;
              return this.indexManager.query(criteria, options);
            case 21:
              matchingLines = _context8.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context8.next = 24;
                break;
              }
              return _context8.abrupt("return", []);
            case 24:
              _context8.next = 26;
              return this.query(_toConsumableArray(matchingLines), options);
            case 26:
              return _context8.abrupt("return", _context8.sent);
            case 27:
            case "end":
              return _context8.stop();
          }
        }, _callee8, this);
      }));
      function query(_x4) {
        return _query.apply(this, arguments);
      }
      return query;
    }()
  }, {
    key: "update",
    value: function () {
      var _update = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee9(criteria, data) {
        var _this8 = this;
        var options,
          matchingLines,
          ranges,
          validMatchingLines,
          entries,
          lines,
          _iterator3,
          _step3,
          entry,
          updated,
          serialized,
          byteOffset,
          newOffsets,
          rangeMap,
          i,
          originalOffset,
          _rangeMap$get,
          range,
          line,
          oldLength,
          newLength,
          _args9 = arguments;
        return _regeneratorRuntime().wrap(function _callee9$(_context9) {
          while (1) switch (_context9.prev = _context9.next) {
            case 0:
              options = _args9.length > 2 && _args9[2] !== undefined ? _args9[2] : {};
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              if (!this.destroyed) {
                _context9.next = 4;
                break;
              }
              throw new Error('Database is destroyed');
            case 4:
              if (this.initialized) {
                _context9.next = 7;
                break;
              }
              _context9.next = 7;
              return this.init();
            case 7:
              _context9.t0 = this.shouldSave;
              if (!_context9.t0) {
                _context9.next = 11;
                break;
              }
              _context9.next = 11;
              return this.save()["catch"](console.error);
            case 11:
              _context9.next = 13;
              return this.indexManager.query(criteria, options);
            case 13:
              matchingLines = _context9.sent;
              if (matchingLines !== null && matchingLines !== void 0 && matchingLines.size) {
                _context9.next = 16;
                break;
              }
              return _context9.abrupt("return", []);
            case 16:
              // Obtém ranges e valida
              ranges = this.getRanges(_toConsumableArray(matchingLines));
              validMatchingLines = new Set(ranges.map(function (r) {
                return r.index;
              }));
              if (validMatchingLines.size) {
                _context9.next = 20;
                break;
              }
              return _context9.abrupt("return", []);
            case 20:
              _context9.next = 22;
              return this.readLines(_toConsumableArray(validMatchingLines), ranges);
            case 22:
              entries = _context9.sent;
              lines = [];
              _iterator3 = _createForOfIteratorHelper(entries);
              _context9.prev = 25;
              _iterator3.s();
            case 27:
              if ((_step3 = _iterator3.n()).done) {
                _context9.next = 36;
                break;
              }
              entry = _step3.value;
              updated = _objectSpread(_objectSpread({}, entry), data);
              _context9.next = 32;
              return this.serializer.serialize(updated);
            case 32:
              serialized = _context9.sent;
              lines.push(serialized);
            case 34:
              _context9.next = 27;
              break;
            case 36:
              _context9.next = 41;
              break;
            case 38:
              _context9.prev = 38;
              _context9.t1 = _context9["catch"](25);
              _iterator3.e(_context9.t1);
            case 41:
              _context9.prev = 41;
              _iterator3.f();
              return _context9.finish(41);
            case 44:
              byteOffset = 0;
              newOffsets = [];
              rangeMap = new Map();
              ranges.forEach(function (range, i) {
                return rangeMap.set(range.index, {
                  range: range,
                  line: lines[i]
                });
              });
              for (i = 0; i < this.offsets.length; i++) {
                originalOffset = this.offsets[i];
                if (rangeMap.has(i)) {
                  _rangeMap$get = rangeMap.get(i), range = _rangeMap$get.range, line = _rangeMap$get.line;
                  oldLength = range.end - range.start;
                  newLength = line.length;
                  byteOffset += newLength - oldLength; // Atualiza deslocamento acumulado
                }
                newOffsets.push(originalOffset + byteOffset);
              }
              this.offsets = newOffsets;
              this.indexOffset += byteOffset;
              _context9.next = 53;
              return this.fileHandler.replaceLines(ranges, lines);
            case 53:
              ranges.forEach(function (range, i) {
                _this8.indexManager.dryRemove(range.index);
                _this8.indexManager.add(entries[i], range.index);
              });
              this.shouldSave = true;
              return _context9.abrupt("return", entries);
            case 56:
            case "end":
              return _context9.stop();
          }
        }, _callee9, this, [[25, 38, 41, 44]]);
      }));
      function update(_x5, _x6) {
        return _update.apply(this, arguments);
      }
      return update;
    }()
  }, {
    key: "delete",
    value: function () {
      var _delete2 = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee10(criteria) {
        var options,
          matchingLines,
          ranges,
          validMatchingLines,
          offsets,
          byteOffset,
          k,
          _args10 = arguments;
        return _regeneratorRuntime().wrap(function _callee10$(_context10) {
          while (1) switch (_context10.prev = _context10.next) {
            case 0:
              options = _args10.length > 1 && _args10[1] !== undefined ? _args10[1] : {};
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              if (!this.destroyed) {
                _context10.next = 4;
                break;
              }
              throw new Error('Database is destroyed');
            case 4:
              if (this.initialized) {
                _context10.next = 7;
                break;
              }
              _context10.next = 7;
              return this.init();
            case 7:
              _context10.t0 = this.shouldSave;
              if (!_context10.t0) {
                _context10.next = 11;
                break;
              }
              _context10.next = 11;
              return this.save()["catch"](console.error);
            case 11:
              _context10.next = 13;
              return this.indexManager.query(criteria, options);
            case 13:
              matchingLines = _context10.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context10.next = 16;
                break;
              }
              return _context10.abrupt("return", 0);
            case 16:
              ranges = this.getRanges(_toConsumableArray(matchingLines));
              validMatchingLines = new Set(ranges.map(function (r) {
                return r.index;
              }));
              _context10.next = 20;
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
              return _context10.abrupt("return", ranges.length);
            case 28:
            case "end":
              return _context10.stop();
          }
        }, _callee10, this);
      }));
      function _delete(_x7) {
        return _delete2.apply(this, arguments);
      }
      return _delete;
    }()
  }, {
    key: "destroy",
    value: function () {
      var _destroy = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee11() {
        return _regeneratorRuntime().wrap(function _callee11$(_context11) {
          while (1) switch (_context11.prev = _context11.next) {
            case 0:
              _context11.t0 = this.shouldSave;
              if (!_context11.t0) {
                _context11.next = 4;
                break;
              }
              _context11.next = 4;
              return this.save()["catch"](console.error);
            case 4:
              this.destroyed = true;
              this.indexOffset = 0;
              this.indexManager.index = {};
              this.writeBuffer.length = 0;
              this.initialized = false;
              this.fileHandler.destroy();
            case 10:
            case "end":
              return _context11.stop();
          }
        }, _callee11, this);
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