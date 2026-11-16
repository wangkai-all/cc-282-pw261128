/* 数据质量处置平台 —— 前端逻辑（原生 JS，无框架、无构建） */
"use strict";

let state = null;
const $ = (id) => document.getElementById(id);

async function api(path, body) {
  const opt = body === undefined
    ? { method: "GET" }
    : { method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body) };
  const res = await fetch(path, opt);
  const data = await res.json();
  if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
  if (data.crashed) toast("💥 模拟崩溃：" + data.error, "error");
  return data;
}

function toast(msg, kind) {
  const el = $("toast");
  el.textContent = msg;
  el.className = "toast " + (kind || "");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => el.classList.add("hidden"), 3800);
}

function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
function fmt(n) {
  if (n === null || n === undefined) return "-";
  return Number(n).toLocaleString("zh-CN", { maximumFractionDigits: 4 });
}
function shortTs(ts) { return ts ? ts.replace("T", " ").replace("Z", " UTC").slice(0, 23) : "-"; }
function typeLabel(t) {
  return { incremental: "增量", snapshot: "全量快照", compensation: "补偿",
    repair_snapshot: "修复快照" }[t] || t;
}
function typeBadge(t) {
  const cls = { incremental: "badge-primary", snapshot: "badge-teal",
    compensation: "badge-warn", repair_snapshot: "badge-purple" }[t] || "badge-neutral";
  return `<span class="badge ${cls}">${typeLabel(t)}</span>`;
}

// ---------------------------------------------------------------- 渲染
async function refresh(silent) {
  try {
    state = await api("/api/state");
    renderAll();
  } catch (e) {
    if (!silent) toast("加载失败：" + e.message, "error");
  }
}

function renderAll() {
  $("latestVersionBadge").textContent = `最新版本 v${state.latest_version}`;
  renderVersions();
  renderRaw();
  renderQc();
  renderDefects();
  renderIssues();
  renderConsumers();
  renderReconcile();
  renderEvents();
  fillSelects();
}

function renderVersions() {
  const el = $("versionList");
  if (!state.versions.length) { el.className = "version-list empty"; el.textContent = "暂无版本"; return; }
  el.className = "version-list";
  el.innerHTML = state.versions.map((v) => {
    let detail = "";
    const p = v.payload;
    if (p.type === "incremental") {
      detail = p.records.map((r) =>
        `<div class="mono">${esc(r.record_id)} · ${esc(r.partition_key)}/${esc(r.metric)} · ${fmt(r.amount)}</div>`
      ).join("");
    } else if (p.type === "compensation") {
      detail = p.corrections.map((c) =>
        `<div class="mono">${esc(c.record_id)} · 差额 <b class="${c.delta < 0 ? "delta-neg" : "delta-pos"}">${fmt(c.delta)}</b>` +
        ` · 键 ${esc(c.idempotency_key)}</div>`).join("");
    } else {
      detail = `<div class="mono">全量分区×指标 ${Object.keys(p.totals).length} 项，记录 ${p.record_ids.length} 条（replace 基线）</div>` +
        Object.entries(p.totals).map(([k, val]) => {
          const [pk, metric] = k.split("\u001f");
          return `<div class="mono">${esc(pk)}/${esc(metric)} · ${fmt(val)}</div>`;
        }).join("");
    }
    return `<div class="version-item t-${v.version_type}">
      <div class="v-head">
        <span class="v-title">v${v.version} ${typeBadge(v.version_type)}</span>
        <span class="badge badge-muted">${shortTs(v.created_at)}</span>
      </div>
      <div class="v-note">${esc(v.note || "（无备注）")}</div>
      <div class="v-detail">${detail}</div>
    </div>`;
  }).join("");
}

function renderRaw() {
  const tb = $("rawTable").querySelector("tbody");
  const confirmed = new Set(state.defects.filter((d) => d.confirmed).map((d) => d.record_id));
  const suspected = new Set(state.defects.map((d) => d.record_id));
  const published = new Set();
  state.versions.forEach((v) => {
    const p = v.payload;
    (p.records || []).forEach((r) => published.add(r.record_id));
    (p.record_ids || []).forEach((id) => published.add(id));
    (p.corrections || []).forEach((c) => published.add(c.record_id));
  });
  if (!state.raw_records.length) {
    tb.innerHTML = `<tr><td colspan="6" class="empty">暂无记录</td></tr>`;
    return;
  }
  tb.innerHTML = state.raw_records.map((r) => {
    const cls = confirmed.has(r.record_id) ? "confirmed" : suspected.has(r.record_id) ? "bad" : "";
    let badge = published.has(r.record_id)
      ? '<span class="badge badge-neutral">已发布</span>'
      : '<span class="badge badge-muted">未发布</span>';
    if (confirmed.has(r.record_id)) badge += ' <span class="badge badge-danger">已确认坏数据</span>';
    else if (suspected.has(r.record_id)) badge += ' <span class="badge badge-warn">嫌疑</span>';
    return `<tr class="${cls}">
      <td class="mono">${esc(r.record_id)}</td><td>${esc(r.partition_key)}</td>
      <td>${esc(r.metric)}</td><td>${fmt(r.amount)}</td>
      <td class="mono">${esc(r.event_time)}</td><td>${badge}</td></tr>`;
  }).join("");
}

function renderQc() {
  const el = $("qcList");
  if (!state.qc_tasks.length) { el.className = "qc-list empty"; el.textContent = "暂无质检任务"; return; }
  el.className = "qc-list";
  const pendingDefects = state.defects.filter((d) => !d.confirmed);
  el.innerHTML = state.qc_tasks.map((t) => {
    const statusBadge = {
      pending: '<span class="badge badge-neutral">待返回</span>',
      passed: '<span class="badge badge-ok">通过</span>',
      failed: '<span class="badge badge-danger">失败</span>',
      fenced: '<span class="badge badge-warn">围栏（迟到）</span>',
    }[t.status];
    let actions = "";
    if (t.status === "pending") {
      const opts = pendingDefects.map((d) =>
        `<label style="display:block;font-weight:400"><input type="checkbox" data-defect="${esc(d.defect_id)}" ` +
        `data-task="${esc(t.task_id)}"/> ${esc(d.record_id)} → ${fmt(d.correct_amount)}</label>`).join("");
      actions = `<div class="qc-actions">
          <button class="btn btn-small btn-ok" data-qc-pass="${esc(t.task_id)}">质检通过</button>
          <button class="btn btn-small btn-danger" data-qc-fail="${esc(t.task_id)}">质检失败（勾选缺陷）</button>
        </div>${opts ? `<div style="margin-top:5px">${opts}</div>` : ""}`;
    } else if (t.status === "fenced") {
      actions = `<div class="qc-actions"><span class="badge badge-warn">结果只记录不执行：新版本已发布，迟到结果不能撤销</span></div>`;
    } else {
      actions = `<div class="qc-actions"><span class="badge badge-muted">结果：${esc(JSON.stringify(t.result))}</span></div>`;
    }
    return `<div class="qc-item">
      <div class="qc-head"><b class="mono">${esc(t.task_id)}</b>
        <span>针对 v${t.version} ${statusBadge}</span>
        <span class="badge badge-muted">${shortTs(t.completed_at || t.created_at)}</span></div>
      ${actions}</div>`;
  }).join("");
}

function renderDefects() {
  const el = $("defectList");
  if (!state.defects.length) { el.className = "defect-list empty"; el.textContent = "暂无坏数据登记"; return; }
  el.className = "defect-list";
  el.innerHTML = state.defects.map((d) => `
    <div class="defect-item">
      <div class="qc-head">
        <b class="mono">${esc(d.record_id)}</b>
        ${d.confirmed ? '<span class="badge badge-danger">已确认（真值已修正）</span>'
                      : '<span class="badge badge-warn">嫌疑未确认</span>'}
      </div>
      <div style="margin-top:5px">坏值 <b>${fmt(d.bad_amount)}</b> → 正确值 <b>${fmt(d.correct_amount)}</b>
        <span class="badge badge-muted">${esc(d.note || "")}</span></div>
      ${d.confirmed ? "" : `<div class="qc-actions">
        <button class="btn btn-small btn-warn" data-confirm-defect="${esc(d.defect_id)}">人工确认（不回滚旧版本）</button>
      </div>`}
    </div>`).join("");
}

function issueStateBadge(s) {
  return {
    isolated: '<span class="badge badge-warn">已隔离</span>',
    fenced: '<span class="badge badge-warn">围栏</span>',
    repairing: '<span class="badge badge-purple">修复中</span>',
    repaired: '<span class="badge badge-ok">已修复</span>',
  }[s] || `<span class="badge badge-muted">${esc(s)}</span>`;
}

function renderIssues() {
  const el = $("issueList");
  if (!state.issues.length) { el.className = "issue-list empty"; el.textContent = "暂无隔离单"; return; }
  el.className = "issue-list";
  el.innerHTML = state.issues.map((i) => {
    const delta = i.expected_total - i.bad_total;
    const disabled = i.state === "repaired" ? "disabled" : "";
    return `<div class="issue-item">
      <div class="issue-head">
        <label><input type="checkbox" data-issue="${esc(i.issue_id)}" ${disabled}
          ${i.state !== "repaired" ? "checked" : ""}/>
          <b class="mono">${esc(i.issue_id)}</b></label>
        ${issueStateBadge(i.state)}
      </div>
      <div style="margin-top:5px">
        分区 <b>${esc(i.partition_key)}</b> · 指标 <b>${esc(i.metric)}</b> ·
        坏版本 <b>v${i.bad_version}</b><br/>
        记录 <span class="mono">${esc(i.record_ids.join(", "))}</span><br/>
        坏合计 ${fmt(i.bad_total)} → 应计 ${fmt(i.expected_total)}
        （差额 <b class="${delta < 0 ? "delta-neg" : "delta-pos"}">${fmt(delta)}</b>）
      </div>
      <div class="issue-actions">
        <button class="btn btn-small" data-impact="${esc(i.issue_id)}">查看影响面</button>
      </div>
    </div>`;
  }).join("");
}

function renderConsumers() {
  const el = $("consumerList");
  if (!state.consumers.length) { el.className = "consumer-list empty"; el.textContent = "暂无消费者"; return; }
  el.className = "consumer-list";
  const latest = state.latest_version;
  el.innerHTML = state.consumers.map((c) => {
    const modeBadge = c.mode === "idempotent"
      ? '<span class="badge badge-teal">幂等</span>'
      : '<span class="badge badge-danger">非幂等</span>';
    const stateRows = c.state.length
      ? `<table class="state-table"><thead><tr><th>分区</th><th>指标</th><th>当前累计</th><th>基线</th></tr></thead><tbody>` +
        c.state.map((s) => `<tr><td>${esc(s.partition_key)}</td><td>${esc(s.metric)}</td>
          <td><b>${fmt(s.total)}</b></td><td>v${s.base_version}</td></tr>`).join("") + "</tbody></table>"
      : '<div class="empty">无物化状态</div>';
    const tracks = c.tracks.map((tr) =>
      `<span class="track-row">v${tr.version}:<span class="status-${tr.status}">${esc(tr.status)}</span>` +
      `×${tr.attempts}</span>`).join("") || '<span class="empty">无投递</span>';
    const snapshots = state.versions.filter((v) =>
      v.payload.type === "snapshot" || v.payload.type === "repair_snapshot")
      .map((v) => v.version);
    return `<div class="consumer-card">
      <div class="consumer-head">
        <b>${esc(c.name)}</b> ${modeBadge}
        <span class="badge badge-neutral">offset v${c.last_offset} / v${latest}</span>
      </div>
      <div style="margin-top:6px">${stateRows}</div>
      <div style="margin-top:6px">${tracks}</div>
      <div class="consumer-actions">
        <button class="btn btn-small btn-primary" data-deliver="${esc(c.consumer_id)}">投递到最新版本</button>
        <button class="btn btn-small btn-danger" data-crash-before="${esc(c.consumer_id)}">崩溃演练：提交前失败</button>
        <button class="btn btn-small btn-danger" data-crash-mid="${esc(c.consumer_id)}">崩溃演练：处理中途失败，再重试</button>
        ${snapshots.length ? `<button class="btn btn-small btn-warn" data-misuse="${esc(c.consumer_id)}"
          data-snap="${snapshots[snapshots.length - 1]}">错误接线：把快照当增量累加</button>` : ""}
        <button class="btn btn-small btn-ghost" data-reset-consumer="${esc(c.consumer_id)}">重置该消费者</button>
      </div>
    </div>`;
  }).join("");
}

function renderReconcile() {
  const el = $("reconcilePanel");
  const r = state.reconcile;
  if (!r.consumers.length) { el.className = "reconcile-panel empty"; el.textContent = "注册消费者后即可对账"; return; }
  const expected = `<div style="margin-bottom:8px"><b>系统真值（原始台账＋已确认修正）：</b>` +
    r.expected_totals.map((x) =>
      `<span class="track-row badge badge-muted">${esc(x.partition_key)}/${esc(x.metric)}=${fmt(x.total)}</span>`
    ).join("") + "</div>";
  el.className = "reconcile-panel";
  el.innerHTML = expected + r.consumers.map((c) => {
    const head = c.matched
      ? `<b>${esc(c.name)}</b> <span class="badge badge-ok">✓ 全部一致</span>`
      : `<b>${esc(c.name)}</b> <span class="badge badge-danger">✗ ${c.diffs.length} 项差异</span>`;
    const diffs = c.diffs.length
      ? `<table class="state-table" style="margin-top:6px"><thead><tr><th>分区</th><th>指标</th>
        <th>真值</th><th>消费者值</th><th>差异</th></tr></thead><tbody>` +
        c.diffs.map((d) => `<tr><td>${esc(d.partition_key)}</td><td>${esc(d.metric)}</td>
          <td>${fmt(d.expected)}</td><td>${fmt(d.actual)}</td>
          <td class="${d.difference < 0 ? "delta-neg" : "delta-pos"}">${fmt(d.difference)}</td></tr>`).join("") +
        "</tbody></table>"
      : "";
    return `<div class="recon-consumer ${c.matched ? "recon-ok" : "recon-bad"}">${head}${diffs}</div>`;
  }).join("") +
  (r.all_matched
    ? '<div class="badge badge-ok" style="margin-top:4px">结论：所有下游与真值一致</div>'
    : '<div class="badge badge-danger" style="margin-top:4px">结论：存在偏离，需按差异核对处置/接线</div>');
}

function renderEvents() {
  const el = $("eventLog");
  if (!state.events.length) { el.className = "event-log empty"; el.textContent = "暂无事件"; return; }
  el.className = "event-log";
  el.innerHTML = state.events.map((e) => `
    <div class="event-item">
      <span class="event-ts">${shortTs(e.ts)}</span>
      <span class="event-kind k-${esc(e.kind)}">${esc(e.kind)}</span>
      <div class="event-detail">${esc(JSON.stringify(e.detail))}</div>
    </div>`).join("");
}

function fillSelects() {
  const qc = $("qcVersion");
  qc.innerHTML = state.versions.map((v) => `<option value="${v.version}">v${v.version} ${typeLabel(v.version_type)}</option>`).join("")
    || '<option value="">（无版本）</option>';
  const dr = $("defRecord");
  dr.innerHTML = state.raw_records.map((r) =>
    `<option value="${esc(r.record_id)}">${esc(r.record_id)}（${esc(r.partition_key)}/${esc(r.metric)}，当前 ${fmt(r.amount)}）</option>`
  ).join("") || '<option value="">（无记录）</option>';
}

// ---------------------------------------------------------------- 动作绑定
document.addEventListener("click", async (ev) => {
  const t = ev.target;
  try {
    if (t.id === "btnSeed") { await api("/api/demo/seed"); await refresh(); toast("演示场景已载入"); return; }
    if (t.id === "btnReset") {
      if (!confirm("确认清空全部数据？")) return;
      await api("/api/reset"); await refresh(); toast("已清空"); return;
    }
    if (t.id === "btnRefresh") { await refresh(); toast("已刷新", "ok"); return; }

    if (t.id === "btnAddRecord") { $("rawForm").classList.toggle("hidden"); return; }
    if (t.id === "rfCancel") { $("rawForm").classList.add("hidden"); return; }
    if (t.id === "rfSubmit") {
      const rec = {
        record_id: $("rfId").value.trim() || undefined,
        stream: $("rfStream").value.trim(),
        partition_key: $("rfPartition").value.trim(),
        metric: $("rfMetric").value.trim(),
        amount: parseFloat($("rfAmount").value),
        event_time: $("rfTime").value.trim() || new Date().toISOString(),
      };
      if (!rec.stream || !rec.partition_key || !rec.metric || Number.isNaN(rec.amount))
        throw new Error("请填充分区、指标、金额");
      await api("/api/ingest", { records: [rec] });
      $("rawForm").classList.add("hidden");
      await refresh(); toast("记录已接入（不可变台账）", "ok"); return;
    }
    if (t.id === "btnPublishInc") {
      const published = new Set();
      state.versions.forEach((v) => {
        (v.payload.records || []).forEach((r) => published.add(r.record_id));
      });
      const ids = state.raw_records.filter((r) => !published.has(r.record_id))
        .map((r) => r.record_id);
      if (!ids.length) throw new Error("没有未发布的记录");
      const r = await api("/api/publish-incremental", { record_ids: ids, note: "页面发布增量" });
      await refresh(); toast(`已发布增量 v${r.version}`, "ok"); return;
    }
    if (t.id === "btnSnapshot") {
      const r = await api("/api/snapshot", { note: "页面发布全量快照" });
      await refresh(); toast(`已发布快照 v${r.version}`, "ok"); return;
    }

    if (t.id === "btnQcCreate") {
      const v = parseInt($("qcVersion").value, 10);
      if (!v) throw new Error("请先发布版本");
      const r = await api("/api/qc/create", { version: v });
      await refresh(); toast(`质检任务 ${r.task_id} 已创建`, "ok"); return;
    }
    if (t.dataset.qcPass) {
      await api("/api/qc/complete", { task_id: t.dataset.qcPass, passed: true });
      await refresh(); toast("质检通过", "ok"); return;
    }
    if (t.dataset.qcFail) {
      const tid = t.dataset.qcFail;
      const defectIds = [...document.querySelectorAll(`input[data-task="${tid}"]:checked`)]
        .map((cb) => cb.dataset.defect);
      const r = await api("/api/qc/complete",
        { task_id: tid, passed: false, defect_ids: defectIds });
      await refresh();
      toast(r.status === "fenced" ? "质检迟到，结果进入围栏，未撤销任何版本" : "质检失败，已生成/更新隔离单",
        r.status === "fenced" ? "error" : "ok");
      return;
    }
    if (t.dataset.confirmDefect) {
      await api("/api/defect/confirm", { defect_id: t.dataset.confirmDefect });
      await refresh(); toast("已人工确认：真值修正，旧版本保留", "ok"); return;
    }

    if (t.id === "btnInject") {
      const rid = $("defRecord").value;
      const correct = parseFloat($("defCorrect").value);
      if (!rid || Number.isNaN(correct)) throw new Error("请选择记录并填写正确金额");
      const r = await api("/api/defect/inject",
        { record_id: rid, correct_amount: correct, note: $("defNote").value.trim() });
      await refresh(); toast(`坏数据嫌疑已登记（坏值 ${fmt(r.bad_amount)} → ${fmt(r.correct_amount)}）`); return;
    }
    if (t.dataset.impact) {
      const r = await api("/api/issue/impact", { issue_id: t.dataset.impact });
      renderImpact(r); return;
    }
    if (t.id === "btnPlan") {
      const ids = [...document.querySelectorAll('input[data-issue]:checked')]
        .map((cb) => cb.dataset.issue);
      if (!ids.length) throw new Error("请勾选至少一个未修复隔离单");
      const r = await api("/api/disposition/plan", { issue_ids: ids });
      renderPlan(r); return;
    }

    if (t.id === "btnRegister") {
      const name = $("cnName").value.trim();
      if (!name) throw new Error("请填写消费者名称");
      await api("/api/consumer/register", { name, mode: $("cnMode").value });
      $("cnName").value = "";
      await refresh(); toast("消费者已注册", "ok"); return;
    }
    if (t.dataset.deliver) {
      const r = await api("/api/consumer/deliver", { consumer_id: t.dataset.deliver });
      await refresh();
      toast(r.applied_versions.length ? `已投递版本：${r.applied_versions.map((v) => "v" + v).join(", ")}`
        : "已是最新，无新版本可投递", "ok"); return;
    }
    if (t.dataset.crashBefore) {
      await api("/api/consumer/deliver",
        { consumer_id: t.dataset.crashBefore, crash_phase: "before_commit" });
      await refresh(); return;
    }
    if (t.dataset.crashMid) {
      const cid = t.dataset.crashMid;
      await api("/api/consumer/deliver", { consumer_id: cid, crash_phase: "mid" });
      await refresh();
      const retry = await api("/api/consumer/deliver", { consumer_id: cid });
      await refresh();
      toast(`已重试：跳过 ${retry.skipped_units} 个已应用单元，未重复冲减`, "ok"); return;
    }
    if (t.dataset.misuse) {
      if (!confirm("这是错误接线演示：把快照当增量累加会导致数值翻倍，继续？")) return;
      await api("/api/consumer/misuse-snapshot",
        { consumer_id: t.dataset.misuse, version: parseInt(t.dataset.snap, 10) });
      await refresh(); toast("已制造错误接线，请查看对账差异", "error"); return;
    }
    if (t.dataset.resetConsumer) {
      await api("/api/consumer/reset", { consumer_id: t.dataset.resetConsumer });
      await refresh(); toast("消费者状态已重置", "ok"); return;
    }
    if (t.id === "btnReconcile") { await refresh(); toast("对账已更新", "ok"); return; }
  } catch (e) {
    toast(e.message, "error");
  }
});

// 处置执行按钮（事件委托内单独处理，因为面板是动态生成的）
document.addEventListener("click", async (ev) => {
  const t = ev.target;
  try {
    if (t.dataset.execComp || t.dataset.execSnap) {
      const ids = [...document.querySelectorAll('input[data-issue]:checked')]
        .map((cb) => cb.dataset.issue);
      if (!ids.length) throw new Error("请勾选至少一个未修复隔离单");
      const strategy = t.dataset.execComp ? "compensation" : "snapshot";
      const r = await api("/api/disposition/execute",
        { strategy, issue_ids: ids, note: "页面执行处置" });
      await refresh();
      $("planPanel").classList.add("hidden");
      toast(strategy === "compensation"
        ? `补偿版本 v${r.repair_version} 已发布（含幂等键）`
        : `修复快照 v${r.repair_version} 已发布，等待消费者切换基线`, "ok");
    }
  } catch (e) {
    toast(e.message, "error");
  }
});

// ---------------------------------------------------------------- 影响面 & 处置面板
function renderImpact(r) {
  const el = $("impactPanel");
  el.classList.remove("hidden");
  const i = r.issue;
  el.innerHTML = `
    <h3>影响面 · <span class="mono">${esc(i.issue_id)}</span></h3>
    <ul>
      <li>受影响分区 / 指标：<b>${esc(i.partition_key)} / ${esc(i.metric)}</b></li>
      <li>传播到的版本：${r.affected_versions.map((v) => `<b>v${v}</b>`).join("、")}
        （坏版本保留，不物理删除）</li>
      <li>涉及记录：<span class="mono">${esc(r.affected_records.map((x) => x.record_id).join(", "))}</span></li>
      <li>总差额：<b class="${r.delta < 0 ? "delta-neg" : "delta-pos"}">${fmt(r.delta)}</b>
        （应计 ${fmt(i.expected_total)}，坏合计 ${fmt(i.bad_total)}）</li>
    </ul>
    <h3>已读旧版的消费者（必须收到补偿/快照才能修正）</h3>
    ${r.read_consumers.length ? "<ul>" + r.read_consumers.map((c) =>
      `<li>${esc(c.name)} <span class="badge badge-danger">已读 v${i.bad_version}</span>
       ${esc(c.mode)} · offset v${c.last_offset}</li>`).join("") + "</ul>"
      : '<div class="empty">无</div>'}
    <h3>尚未读到坏版本的消费者（追上时直接得到正确结果，不抖动）</h3>
    ${r.unread_consumers.length ? "<ul>" + r.unread_consumers.map((c) =>
      `<li>${esc(c.name)} <span class="badge badge-teal">未读 v${i.bad_version}</span>
       ${esc(c.mode)} · offset v${c.last_offset}</li>`).join("") + "</ul>"
      : '<div class="empty">无</div>'}`;
}

function renderPlan(r) {
  const el = $("planPanel");
  el.classList.remove("hidden");
  const c = r.options.compensation;
  const s = r.options.snapshot;
  const scopeC = c.scope_partition_metrics.map((x) => `${x[0]}/${x[1]}`).join("、");
  const consumerRows = r.consumers.map((x) => `
    <tr><td>${esc(x.name)}</td>
      <td>${x.read_bad_version ? '<span class="badge badge-danger">已读坏版</span>'
                                : '<span class="badge badge-teal">未读</span>'}</td>
      <td>${esc(x.compensation_risk)}</td>
      <td>${esc(x.snapshot_requirement)}</td></tr>`).join("");
  el.innerHTML = `
    <div class="plan-grid">
      <div class="plan-card comp">
        <h3>方案 A：${esc(c.label)}</h3>
        <dl>
          <dt>修复范围</dt><dd>${c.scope_size} 个分区×指标：${esc(scopeC)}；重写 ${c.rebuild_record_count} 条记录</dd>
          <dt>等待成本</dt><dd>${esc(c.wait)}</dd>
          <dt>消费者动作</dt><dd>${esc(c.consumer_action)}</dd>
          <dt>重复处理风险</dt><dd>${esc(c.duplicate_risk)}</dd>
          <dt>落后消费者</dt><dd>${esc(c.laggard_behavior)}</dd>
          <dt>历史保留</dt><dd>${esc(c.history)}</dd>
        </dl>
        <div class="qc-actions" style="margin-top:10px">
          <button class="btn btn-small btn-warn" data-exec-comp="1">执行：发布补偿记录</button>
        </div>
      </div>
      <div class="plan-card snap">
        <h3>方案 B：${esc(s.label)}</h3>
        <dl>
          <dt>修复范围</dt><dd>${s.scope_size} 个分区×指标（全表）；重算 ${s.rebuild_record_count} 条记录</dd>
          <dt>等待成本</dt><dd>${esc(s.wait)}</dd>
          <dt>消费者动作</dt><dd>${esc(s.consumer_action)}</dd>
          <dt>重复处理风险</dt><dd>${esc(s.duplicate_risk)}</dd>
          <dt>落后消费者</dt><dd>${esc(s.laggard_behavior)}</dd>
          <dt>历史保留</dt><dd>${esc(s.history)}</dd>
        </dl>
        <div class="qc-actions" style="margin-top:10px">
          <button class="btn btn-small btn-purple" data-exec-snap="1">执行：切完整新快照</button>
        </div>
      </div>
    </div>
    <h3 style="margin:12px 0 6px;font-size:13px">逐消费者风险对照</h3>
    <div class="table-wrap"><table>
      <thead><tr><th>消费者</th><th>是否已读坏版</th><th>补偿方案风险</th><th>快照方案要求</th></tr></thead>
      <tbody>${consumerRows}</tbody></table></div>`;
}

refresh();
setInterval(() => refresh(true), 5000);
