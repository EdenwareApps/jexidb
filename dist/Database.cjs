"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.Database = void 0;
var _events = require("events");
var _FileHandler = _interopRequireDefault(require("./FileHandler.mjs"));
var _IndexManager = _interopRequireDefault(require("./IndexManager.mjs"));
var _Simple = _interopRequireDefault(require("./serializers/Simple.mjs"));
var _Advanced = _interopRequireDefault(require("./serializers/Advanced.mjs"));
var _fs = _interopRequireDefault(require("fs"));
function _interopRequireDefault(e) { return e && e.__esModule ? e : { "default": e }; }
function _createForOfIteratorHelper(r, e) { var t = "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (!t) { if (Array.isArray(r) || (t = _unsupportedIterableToArray(r)) || e && r && "number" == typeof r.length) { t && (r = t); var _n = 0, F = function F() {}; return { s: F, n: function n() { return _n >= r.length ? { done: !0 } : { done: !1, value: r[_n++] }; }, e: function e(r) { throw r; }, f: F }; } throw new TypeError("Invalid attempt to iterate non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); } var o, a = !0, u = !1; return { s: function s() { t = t.call(r); }, n: function n() { var r = t.next(); return a = r.done, r; }, e: function e(r) { u = !0, o = r; }, f: function f() { try { a || null == t["return"] || t["return"](); } finally { if (u) throw o; } } }; }
function _slicedToArray(r, e) { return _arrayWithHoles(r) || _iterableToArrayLimit(r, e) || _unsupportedIterableToArray(r, e) || _nonIterableRest(); }
function _nonIterableRest() { throw new TypeError("Invalid attempt to destructure non-iterable instance.\nIn order to be iterable, non-array objects must have a [Symbol.iterator]() method."); }
function _iterableToArrayLimit(r, l) { var t = null == r ? null : "undefined" != typeof Symbol && r[Symbol.iterator] || r["@@iterator"]; if (null != t) { var e, n, i, u, a = [], f = !0, o = !1; try { if (i = (t = t.call(r)).next, 0 === l) { if (Object(t) !== t) return; f = !1; } else for (; !(f = (e = i.call(t)).done) && (a.push(e.value), a.length !== l); f = !0); } catch (r) { o = !0, n = r; } finally { try { if (!f && null != t["return"] && (u = t["return"](), Object(u) !== u)) return; } finally { if (o) throw n; } } return a; } }
function _arrayWithHoles(r) { if (Array.isArray(r)) return r; }
function _readOnlyError(r) { throw new TypeError('"' + r + '" is read-only'); }
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
var Database = exports.Database = /*#__PURE__*/function (_EventEmitter) {
  function Database(filePath) {
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
    _this2.shouldSave = false;
    if (_this2.opts.v8 || _this2.opts.compress || _this2.opts.compressIndex) {
      _this2.serializer = new _Advanced["default"](_this2.opts);
    } else {
      _this2.serializer = new _Simple["default"](_this2.opts);
    }
    _this2.fileHandler = new _FileHandler["default"](filePath);
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
              if (!this.offsets) {
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
              if (!this.saving) {
                _context2.next = 4;
                break;
              }
              return _context2.abrupt("return", new Promise(function (resolve) {
                return _this4.once('save', resolve);
              }));
            case 4:
              this.saving = true;
              _context2.next = 7;
              return this.flush();
            case 7:
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
              _context2.next = 13;
              return this.serializer.serialize(index, {
                compress: this.opts.compressIndex
              });
            case 13:
              indexString = _context2.sent;
              for (_field in this.indexManager.index.data) {
                for (_term in this.indexManager.index.data[_field]) {
                  this.indexManager.index.data[_field][_term] = new Set(index.data[_field][_term]); // set back to set because of serialization
                }
              }
              offsets.push(this.indexOffset);
              offsets.push(this.indexOffset + indexString.length);
              _context2.next = 19;
              return this.serializer.serialize(offsets, {
                compress: this.opts.compressIndex,
                linebreak: false
              });
            case 19:
              offsetsString = _context2.sent;
              if (!this.shouldTruncate) {
                _context2.next = 24;
                break;
              }
              _context2.next = 23;
              return this.fileHandler.truncate(this.indexOffset);
            case 23:
              this.shouldTruncate = false;
            case 24:
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
              _context5.next = 4;
              return this.serializer.serialize(data, {
                compress: this.opts.compress
              });
            case 4:
              line = _context5.sent;
              // using Buffer for offsets accuracy
              if (this.shouldTruncate) {
                this.writeBuffer.push(this.indexOffset);
                this.shouldTruncate = false;
              }
              position = this.offsets.length;
              this.offsets.push(this.indexOffset);
              this.indexOffset += line.length;
              this.indexManager.add(data, position);
              this.emit('insert', data, position);
              this.writeBuffer.push(line);
              if (!(!this.flushing && this.currentWriteBufferSize() > this.opts.maxMemoryUsage)) {
                _context5.next = 15;
                break;
              }
              _context5.next = 15;
              return this.flush();
            case 15:
              this.shouldSave = true;
            case 16:
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
      if (this.flushing) return this.flushing;
      return new Promise(function (resolve, reject) {
        if (_this7.destroyed) return reject(new Error('Database is destroyed'));
        if (!_this7.writeBuffer.length) return resolve();
        var err;
        _this7.flushing = _this7._flush()["catch"](function (e) {
          return err = e;
        })["finally"](function () {
          _this7.flushing = false;
          err ? reject(err) : resolve();
        });
      });
    }
  }, {
    key: "_flush",
    value: function () {
      var _flush2 = _asyncToGenerator(/*#__PURE__*/_regeneratorRuntime().mark(function _callee6() {
        var fd, data, pos;
        return _regeneratorRuntime().wrap(function _callee6$(_context6) {
          while (1) switch (_context6.prev = _context6.next) {
            case 0:
              _context6.next = 2;
              return _fs["default"].promises.open(this.fileHandler.filePath, 'a');
            case 2:
              fd = _context6.sent;
              _context6.prev = 3;
            case 4:
              if (!this.writeBuffer.length) {
                _context6.next = 23;
                break;
              }
              data = void 0;
              pos = this.writeBuffer.findIndex(function (b) {
                return typeof b === 'number';
              });
              if (!(pos === 0)) {
                _context6.next = 18;
                break;
              }
              _context6.next = 10;
              return fd.close();
            case 10:
              _context6.next = 12;
              return this.fileHandler.truncate(this.writeBuffer.shift());
            case 12:
              _context6.next = 14;
              return _fs["default"].promises.open(this.fileHandler.filePath, 'a');
            case 14:
              fd = _context6.sent;
              return _context6.abrupt("continue", 4);
            case 18:
              if (pos === -1) {
                data = Buffer.concat(this.writeBuffer);
                this.writeBuffer.length = 0;
              } else {
                data = Buffer.concat(this.writeBuffer.slice(0, pos));
                this.writeBuffer.splice(0, pos);
              }
            case 19:
              _context6.next = 21;
              return fd.write(data);
            case 21:
              _context6.next = 4;
              break;
            case 23:
              this.shouldSave = true;
              _context6.next = 29;
              break;
            case 26:
              _context6.prev = 26;
              _context6.t0 = _context6["catch"](3);
              console.error('Error flushing:', _context6.t0);
            case 29:
              _context6.prev = 29;
              _context6.next = 32;
              return fd.close();
            case 32:
              return _context6.finish(29);
            case 33:
            case "end":
              return _context6.stop();
          }
        }, _callee6, this, [[3, 26, 29, 33]]);
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
        var ranges, partitionedRanges, currentPartition, line, m, _i, _partitionedRanges, _ranges, lines, _loop, _line;
        return _regeneratorRuntime().wrap(function _callee7$(_context8) {
          while (1) switch (_context8.prev = _context8.next) {
            case 0:
              if (!_this.destroyed) {
                _context8.next = 2;
                break;
              }
              throw new Error('Database is destroyed');
            case 2:
              _context8.t0 = _this.shouldSave;
              if (!_context8.t0) {
                _context8.next = 6;
                break;
              }
              _context8.next = 6;
              return _awaitAsyncGenerator(_this.save()["catch"](console.error));
            case 6:
              if (!(_this.indexOffset === 0)) {
                _context8.next = 8;
                break;
              }
              return _context8.abrupt("return");
            case 8:
              if (!Array.isArray(map)) {
                if (map && _typeof(map) === 'object') {
                  map = _this.indexManager.query(map, options.matchAny);
                } else {
                  map = _toConsumableArray(Array(_this.offsets.length).keys());
                }
              }
              ranges = _this.getRanges(map);
              partitionedRanges = [], currentPartition = 0;
              for (line in ranges) {
                if (partitionedRanges[currentPartition] === undefined) {
                  partitionedRanges[currentPartition] = [];
                }
                partitionedRanges[currentPartition].push(ranges[line]);
                if (partitionedRanges[currentPartition].length >= _this.opts.maxMemoryUsage) {
                  +currentPartition, _readOnlyError("currentPartition");
                }
              }
              m = 0;
              _i = 0, _partitionedRanges = partitionedRanges;
            case 14:
              if (!(_i < _partitionedRanges.length)) {
                _context8.next = 31;
                break;
              }
              _ranges = _partitionedRanges[_i];
              _context8.next = 18;
              return _awaitAsyncGenerator(_this.fileHandler.readRanges(_ranges));
            case 18:
              lines = _context8.sent;
              _loop = /*#__PURE__*/_regeneratorRuntime().mark(function _loop() {
                var err, entry;
                return _regeneratorRuntime().wrap(function _loop$(_context7) {
                  while (1) switch (_context7.prev = _context7.next) {
                    case 0:
                      _context7.next = 2;
                      return _awaitAsyncGenerator(_this.serializer.deserialize(lines[_line])["catch"](function (e) {
                        return console.error(err = e);
                      }));
                    case 2:
                      entry = _context7.sent;
                      if (!err) {
                        _context7.next = 5;
                        break;
                      }
                      return _context7.abrupt("return", 1);
                    case 5:
                      if (entry._ === undefined) {
                        while (_this.offsets[m] != _line && m < map.length) m++; // weak comparison as 'start' is a string
                        entry._ = m++;
                      }
                      _context7.next = 8;
                      return entry;
                    case 8:
                    case "end":
                      return _context7.stop();
                  }
                }, _loop);
              });
              _context8.t1 = _regeneratorRuntime().keys(lines);
            case 21:
              if ((_context8.t2 = _context8.t1()).done) {
                _context8.next = 28;
                break;
              }
              _line = _context8.t2.value;
              return _context8.delegateYield(_loop(), "t3", 24);
            case 24:
              if (!_context8.t3) {
                _context8.next = 26;
                break;
              }
              return _context8.abrupt("continue", 21);
            case 26:
              _context8.next = 21;
              break;
            case 28:
              _i++;
              _context8.next = 14;
              break;
            case 31:
            case "end":
              return _context8.stop();
          }
        }, _callee7);
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
          _args9 = arguments;
        return _regeneratorRuntime().wrap(function _callee8$(_context9) {
          while (1) switch (_context9.prev = _context9.next) {
            case 0:
              options = _args9.length > 1 && _args9[1] !== undefined ? _args9[1] : {};
              if (!this.destroyed) {
                _context9.next = 3;
                break;
              }
              throw new Error('Database is destroyed');
            case 3:
              _context9.t0 = this.shouldSave;
              if (!_context9.t0) {
                _context9.next = 7;
                break;
              }
              _context9.next = 7;
              return this.save()["catch"](console.error);
            case 7:
              if (!Array.isArray(criteria)) {
                _context9.next = 16;
                break;
              }
              _context9.next = 10;
              return this.readLines(criteria);
            case 10:
              results = _context9.sent;
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
              return _context9.abrupt("return", results);
            case 16:
              _context9.next = 18;
              return this.indexManager.query(criteria, options.matchAny);
            case 18:
              matchingLines = _context9.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context9.next = 21;
                break;
              }
              return _context9.abrupt("return", []);
            case 21:
              _context9.next = 23;
              return this.query(_toConsumableArray(matchingLines), options);
            case 23:
              return _context9.abrupt("return", _context9.sent);
            case 24:
            case "end":
              return _context9.stop();
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
          _iterator,
          _step,
          _loop2,
          offsets,
          byteOffset,
          k,
          _args11 = arguments;
        return _regeneratorRuntime().wrap(function _callee9$(_context11) {
          while (1) switch (_context11.prev = _context11.next) {
            case 0:
              options = _args11.length > 2 && _args11[2] !== undefined ? _args11[2] : {};
              if (!this.destroyed) {
                _context11.next = 3;
                break;
              }
              throw new Error('Database is destroyed');
            case 3:
              _context11.t0 = this.shouldSave;
              if (!_context11.t0) {
                _context11.next = 7;
                break;
              }
              _context11.next = 7;
              return this.save()["catch"](console.error);
            case 7:
              _context11.next = 9;
              return this.indexManager.query(criteria, options.matchAny);
            case 9:
              matchingLines = _context11.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context11.next = 12;
                break;
              }
              return _context11.abrupt("return", []);
            case 12:
              ranges = this.getRanges(_toConsumableArray(matchingLines));
              validMatchingLines = new Set(ranges.map(function (r) {
                return r.index;
              }));
              if (validMatchingLines.size) {
                _context11.next = 16;
                break;
              }
              return _context11.abrupt("return", []);
            case 16:
              _context11.next = 18;
              return this.readLines(_toConsumableArray(validMatchingLines), ranges);
            case 18:
              entries = _context11.sent;
              lines = [];
              _iterator = _createForOfIteratorHelper(entries);
              _context11.prev = 21;
              _loop2 = /*#__PURE__*/_regeneratorRuntime().mark(function _loop2() {
                var entry, err, updated, ret;
                return _regeneratorRuntime().wrap(function _loop2$(_context10) {
                  while (1) switch (_context10.prev = _context10.next) {
                    case 0:
                      entry = _step.value;
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
                }, _loop2);
              });
              _iterator.s();
            case 24:
              if ((_step = _iterator.n()).done) {
                _context11.next = 28;
                break;
              }
              return _context11.delegateYield(_loop2(), "t1", 26);
            case 26:
              _context11.next = 24;
              break;
            case 28:
              _context11.next = 33;
              break;
            case 30:
              _context11.prev = 30;
              _context11.t2 = _context11["catch"](21);
              _iterator.e(_context11.t2);
            case 33:
              _context11.prev = 33;
              _iterator.f();
              return _context11.finish(33);
            case 36:
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
              _context11.next = 43;
              return this.fileHandler.replaceLines(ranges, lines);
            case 43:
              _toConsumableArray(validMatchingLines).forEach(function (lineNumber, i) {
                _this8.indexManager.dryRemove(lineNumber);
                _this8.indexManager.add(entries[i], lineNumber);
              });
              this.shouldSave = true;
              return _context11.abrupt("return", entries);
            case 46:
            case "end":
              return _context11.stop();
          }
        }, _callee9, this, [[21, 30, 33, 36]]);
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
          _args12 = arguments;
        return _regeneratorRuntime().wrap(function _callee10$(_context12) {
          while (1) switch (_context12.prev = _context12.next) {
            case 0:
              options = _args12.length > 1 && _args12[1] !== undefined ? _args12[1] : {};
              if (!this.destroyed) {
                _context12.next = 3;
                break;
              }
              throw new Error('Database is destroyed');
            case 3:
              _context12.t0 = this.shouldSave;
              if (!_context12.t0) {
                _context12.next = 7;
                break;
              }
              _context12.next = 7;
              return this.save()["catch"](console.error);
            case 7:
              _context12.next = 9;
              return this.indexManager.query(criteria, options.matchAny);
            case 9:
              matchingLines = _context12.sent;
              if (!(!matchingLines || !matchingLines.size)) {
                _context12.next = 12;
                break;
              }
              return _context12.abrupt("return", 0);
            case 12:
              ranges = this.getRanges(_toConsumableArray(matchingLines));
              validMatchingLines = new Set(ranges.map(function (r) {
                return r.index;
              }));
              _context12.next = 16;
              return this.fileHandler.replaceLines(ranges, []);
            case 16:
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
            case 24:
            case "end":
              return _context12.stop();
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
        return _regeneratorRuntime().wrap(function _callee11$(_context13) {
          while (1) switch (_context13.prev = _context13.next) {
            case 0:
              _context13.t0 = this.shouldSave;
              if (!_context13.t0) {
                _context13.next = 4;
                break;
              }
              _context13.next = 4;
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
              return _context13.stop();
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
      return this.offsets.length;
    }
  }, {
    key: "index",
    get: function get() {
      return this.indexManager.index;
    }
  }]);
}(_events.EventEmitter);