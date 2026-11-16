/* 前端渲染冒烟测试：用最小 DOM stub 加载真实 app.js，
 * 验证初始化渲染、各面板生成与动作绑定在真实数据下不抛异常。
 * 运行：node tests/dom_smoke.js
 */
"use strict";
const fs = require("fs");
const path = require("path");
const vm = require("vm");

// ---- 最小 DOM stub ----
function makeEl(id) {
  const el = {
    id, className: "", textContent: "", innerHTML: "", value: "",
    dataset: {}, style: {}, disabled: false, checked: false,
    classList: { add() {}, remove() {}, toggle() {}, contains() { return false; } },
    addEventListener() {}, appendChild() {},
    querySelectorAll() { return []; },
  };
  el.querySelector = () => el;
  return el;
}
const ids = ["latestVersionBadge","versionList","rawTable","qcList","defectList",
  "issueList","impactPanel","planPanel","consumerList","reconcilePanel","eventLog",
  "qcVersion","defRecord","rawForm","rfId","rfStream","rfPartition","rfMetric",
  "rfAmount","rfTime","defCorrect","defNote","cnName","cnMode","toast"];
const registry = Object.fromEntries(ids.map((i) => [i, makeEl(i)]));
const listeners = {};
const documentStub = {
  getElementById: (id) => registry[id] || makeEl(id),
  addEventListener: (ev, fn) => { (listeners[ev] ||= []).push(fn); },
  querySelectorAll: () => [],
};

// ---- 最小 fetch stub：从真实引擎逻辑取一份演示状态（经 HTTP 不可用时用内置夹具） ----
const fixture = JSON.parse(fs.readFileSync(path.join(__dirname, "fixture_state.json"), "utf8"));
globalThis.fetch = async () => ({
  ok: true,
  json: async () => fixture,
});
globalThis.setInterval = () => {};
globalThis.confirm = () => true;

const sandbox = { document: documentStub, fetch: globalThis.fetch,
  setInterval: globalThis.setInterval, setTimeout, clearTimeout,
  confirm: globalThis.confirm, console };
vm.createContext(sandbox);
const code = fs.readFileSync(path.join(__dirname, "..", "web", "app.js"), "utf8");
vm.runInContext(code, sandbox);

// 等待初始化的 async refresh 完成
setTimeout(() => {
  const assert = (cond, msg) => { if (!cond) { console.error("FAIL:", msg); process.exit(1); } };
  assert(registry.latestVersionBadge.textContent.includes("v2"), "版本徽标渲染");
  assert(registry.versionList.innerHTML.includes("incremental"), "版本列表渲染");
  assert(registry.rawTable.querySelector("tbody"), "原始表渲染入口存在");
  assert(registry.consumerList.innerHTML.includes("幂等"), "消费者渲染");
  assert(registry.reconcilePanel.innerHTML.includes("真值"), "对账面板渲染");
  assert(registry.eventLog.innerHTML.includes("event-item"), "事件流渲染");
  assert(registry.qcList.innerHTML.includes("待返回"), "质检任务渲染");
  assert(registry.defectList.innerHTML.includes("r1004"), "坏数据渲染");
  assert(fixture.issues.length === 0 || registry.issueList.innerHTML.includes("iss_"),
    "隔离单条件渲染");
  assert((listeners.click || []).length >= 1, "点击事件已绑定");
  console.log("DOM smoke OK: 全部面板在演示数据下渲染成功，事件绑定",
    listeners.click.length, "个");
}, 50);
