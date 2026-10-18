// LogGate dashboard. Vanilla JS, no external resources. Every value from
// the server is inserted with textContent (log content is untrusted).
(function () {
  "use strict";

  var $ = function (id) { return document.getElementById(id); };
  var nf = new Intl.NumberFormat("en-US");
  var fmt = function (n) { return n == null ? "–" : nf.format(n); };
  var demo = false;
  var history = []; // [{t, l1: {dest: n}}]
  var HISTORY = 120;
  var seriesColor = ["var(--s1)", "var(--s2)", "var(--s3)", "var(--s4)"];

  // ---- theme ------------------------------------------------------------
  function storedTheme() { try { return localStorage.getItem("loggate-theme"); } catch (e) { return null; } }
  function applyTheme(t) {
    if (t === "light" || t === "dark") document.documentElement.setAttribute("data-theme", t);
    else document.documentElement.removeAttribute("data-theme");
  }
  applyTheme(storedTheme());
  $("theme-btn").addEventListener("click", function () {
    var cur = document.documentElement.getAttribute("data-theme");
    if (!cur) cur = window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
    var next = cur === "dark" ? "light" : "dark";
    applyTheme(next);
    try { localStorage.setItem("loggate-theme", next); } catch (e) { /* private mode */ }
  });

  // ---- helpers ----------------------------------------------------------
  function el(tag, cls, text) {
    var e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function tag(text, kind) { return el("span", "tag" + (kind ? " " + kind : ""), text); }
  function td(content, cls) {
    var c = el("td", cls);
    if (content instanceof Node) c.appendChild(content); else c.textContent = content;
    return c;
  }
  function toast(msg, bad) {
    var t = $("toast");
    t.textContent = msg;
    t.className = "toast" + (bad ? " bad" : "");
    clearTimeout(toast.timer);
    toast.timer = setTimeout(function () { t.className = "toast hidden"; }, 3500);
  }
  function hhmmss(ms) {
    var d = new Date(ms);
    return d.toLocaleTimeString("en-GB", { hour12: false });
  }

  var LIFECYCLE = {
    active: ["运行", "active", "ok"], registered: ["已注册", "registered", "info"],
    draining: ["排空中", "draining", "warn"], unregistered: ["已注销", "unregistered", "bad"],
    absent: ["未注册", "absent", "bad"], restarting: ["重启中", "restarting", "warn"], stopped: ["已停止", "stopped", "bad"]
  };
  var BREAKER = { closed: ["闭合", "closed", "ok"], half_open: ["半开", "half-open", "warn"], open: ["断开", "open", "bad"] };
  var ERRKIND = { retryable: "可重试 retryable", non_retryable: "不可重试 non-retryable", throttled: "限流 throttled" };
  var LEVEL = { general: ["一般", "general", "info"], important: ["重要", "important", "warn"], core: ["核心", "core", "bad"], unset: ["未分级", "none", ""] };
  var PII = { id_card: "身份证 ID card", bank_card: "银行卡 Bank card", phone: "手机号 Phone", email: "邮箱 Email" };
  var CLASS = { 3002: "3002 认证 Authentication", 0: "0 基础事件 Base Event" };

  function state(map, key) {
    var v = map[key] || [key || "–", "", ""];
    return tag(v[0] + " · " + v[1], v[2]);
  }

  // ---- actions ----------------------------------------------------------
  function act(path, body, label) {
    return fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-LogGate-Demo": "1" },
      body: JSON.stringify(body || {})
    }).then(function (r) { return r.json().catch(function () { return { ok: r.ok }; }); })
      .then(function (j) {
        if (j.ok) toast("✓ " + label);
        else toast("✗ " + label + ": " + (j.error || "failed"), true);
      })
      .catch(function (e) { toast("✗ " + label + ": " + e, true); });
  }
  $("crash-btn").addEventListener("click", function (ev) {
    var b = ev.currentTarget;
    b.disabled = true;
    act("/demo/kill-restart", {}, "模拟崩溃并重启 · simulated crash + restart").finally(function () { b.disabled = false; });
  });

  function actionButtons(d) {
    var box = el("div", "actions");
    function btn(zh, en, path, body, label, enabled) {
      var b = el("button", "btn");
      b.type = "button";
      b.appendChild(document.createTextNode(zh + " "));
      b.appendChild(el("small", null, en));
      b.disabled = !enabled;
      b.addEventListener("click", function () { b.disabled = true; act(path, body, label); });
      box.appendChild(b);
    }
    var hasRecv = !!d.fault;
    var mode = d.fault ? d.fault.mode : "";
    var registered = d.lifecycle === "active" || d.lifecycle === "registered";
    if (hasRecv) {
      btn("停止接收", "Kill sink", "/demo/fault", { dest: d.name, mode: "down" }, d.name + " down", mode !== "down");
      btn("变慢", "Slow 500ms", "/demo/fault", { dest: d.name, mode: "slow", delay_ms: 500 }, d.name + " slow 500ms", mode !== "slow");
      btn("恢复", "Restore", "/demo/fault", { dest: d.name, mode: "ok" }, d.name + " restored", mode !== "ok");
    }
    btn("注销", "Unregister", "/demo/unregister", { name: d.name }, "unregister " + d.name, registered);
    btn("注册", "Register", "/demo/register", { name: d.name }, "register " + d.name, !registered && d.lifecycle !== "restarting" && d.lifecycle !== "draining");
    return box;
  }

  // ---- render -----------------------------------------------------------
  function renderStatus(s) {
    var pill = $("status-pill"), txt = $("status-text");
    var st = s.gateway.status;
    pill.className = "pill " + (st === "running" ? "ok" : st === "restarting" ? "warn" : "bad");
    var label = { running: "运行中 Running", restarting: "重启中 Restarting", stopped: "已停止 Stopped" }[st] || st;
    if (s.gateway.boots > 1) label += " · boot #" + s.gateway.boots;
    txt.textContent = label;
  }

  function renderKPIs(s) {
    var g = s.gateway, d = s.demo;
    var delivered = 0, parts = [];
    s.dests.forEach(function (r) { delivered += r.delivered || 0; parts.push(r.name + " " + fmt(r.delivered)); });
    if (d) {
      $("k-ingested").textContent = fmt(d.sent);
      $("k-ingested-foot").textContent = "负载端发送 · sent by loadgen · " + (d.eps || 0).toFixed(0) + " eps";
      $("k-acked").textContent = fmt(d.acked);
      $("k-acked-foot").textContent = "未确认 not acked: " + fmt(d.refused + d.failed);
    } else {
      $("k-ingested").textContent = fmt(g.ingested);
      $("k-ingested-foot").textContent = "拒绝 refused " + fmt(g.refused);
      $("k-acked").textContent = fmt(g.acked);
      $("k-acked-foot").textContent = "next seq " + fmt(g.next_seq);
    }
    $("k-delivered").textContent = fmt(delivered);
    $("k-delivered-foot").textContent = parts.join(" · ");
    var l1 = $("k-l1");
    l1.textContent = fmt(g.l1_pending);
    l1.className = "k-val" + (g.l1_pending > 0 ? " warn" : "");
    $("k-l1-foot").textContent = "记录 records " + fmt(g.l1_records);
    var dlq = $("k-dlq");
    dlq.textContent = fmt(g.dlq);
    var bp = $("k-bp");
    bp.textContent = g.backpressure ? "开启 ON" : "关闭 off";
    bp.className = "k-val" + (g.backpressure ? " bad" : " ok");
    $("k-bp-foot").textContent = g.backpressure ? "ingest 返回 429 · answering 429" : "正常接收 · accepting";
  }

  function renderDests(s) {
    var tb = document.querySelector("#dest-table tbody");
    tb.textContent = "";
    s.dests.forEach(function (d) {
      var tr = el("tr");
      var name = el("div");
      name.appendChild(el("span", "dest-name", d.name));
      var sub = d.type || "";
      if (d.fault) sub += " · 接收端 receiver: " + ({ ok: "正常 ok", down: "停止 down", slow: "慢 slow " + d.fault.delay_ms + "ms" }[d.fault.mode] || d.fault.mode);
      if (d.level_blocked) sub += " · max_level 拦截 blocked " + fmt(d.level_blocked);
      name.appendChild(el("span", "dest-type", sub));
      tr.appendChild(td(name));
      tr.appendChild(td(state(LIFECYCLE, d.lifecycle)));
      tr.appendChild(td(d.breaker ? state(BREAKER, d.breaker) : "–"));
      tr.appendChild(td(fmt(d.l2), "num"));
      var l1 = td(fmt(d.l1_pending), "num");
      if (d.l1_pending > 0) { l1.style.color = "var(--warn)"; l1.style.fontWeight = "600"; }
      tr.appendChild(l1);
      tr.appendChild(td(d.p99_ms == null ? "–" : (d.p99_ms < 1000 ? d.p99_ms.toFixed(1) + " ms" : (d.p99_ms / 1000).toFixed(2) + " s"), "num"));
      var err = "–";
      if (d.last_err_kind) err = (ERRKIND[d.last_err_kind] || d.last_err_kind) + " · " + Math.round(d.last_err_ago_s) + "s 前 ago";
      tr.appendChild(td(err));
      tr.appendChild(td(fmt(d.delivered), "num"));
      if (demo) tr.appendChild(td(actionButtons(d)));
      tb.appendChild(tr);
    });
  }

  function renderChart(s) {
    var point = { t: s.at, l1: {} };
    s.dests.forEach(function (d) { point.l1[d.name] = d.l1_pending || 0; });
    history.push(point);
    if (history.length > HISTORY) history.shift();
    var names = s.dests.map(function (d) { return d.name; });
    var svg = $("l1-chart"), W = 800, H = 140, P = 18;
    var maxV = 1;
    history.forEach(function (p) { names.forEach(function (n) { maxV = Math.max(maxV, p.l1[n] || 0); }); });
    var ns = "http://www.w3.org/2000/svg";
    svg.textContent = "";
    var axis = document.createElementNS(ns, "line");
    axis.setAttribute("x1", 0); axis.setAttribute("x2", W); axis.setAttribute("y1", H - P); axis.setAttribute("y2", H - P);
    axis.setAttribute("class", "axis");
    svg.appendChild(axis);
    var label = document.createElementNS(ns, "text");
    label.setAttribute("x", 4); label.setAttribute("y", 12);
    label.textContent = "max " + fmt(maxV);
    svg.appendChild(label);
    var legend = $("chart-legend");
    legend.textContent = "";
    names.forEach(function (n, i) {
      var pts = history.map(function (p, j) {
        var x = (HISTORY - history.length + j) / (HISTORY - 1) * W;
        var y = (H - P) - ((p.l1[n] || 0) / maxV) * (H - P - 16);
        return x.toFixed(1) + "," + y.toFixed(1);
      });
      var pl = document.createElementNS(ns, "polyline");
      pl.setAttribute("points", pts.join(" "));
      pl.setAttribute("fill", "none");
      pl.setAttribute("stroke", seriesColor[i % seriesColor.length]);
      pl.setAttribute("stroke-width", "2");
      pl.setAttribute("vector-effect", "non-scaling-stroke");
      svg.appendChild(pl);
      var it = el("span");
      var sw = el("i");
      sw.style.background = seriesColor[i % seriesColor.length];
      it.appendChild(sw);
      it.appendChild(document.createTextNode(n + " " + fmt(point.l1[n])));
      legend.appendChild(it);
    });
  }

  function renderRecon(d) {
    var tb = document.querySelector("#recon-table tbody");
    tb.textContent = "";
    if (!d.recon || !d.recon.length) {
      var tr0 = el("tr");
      var c = td("等待第一次对账… waiting for the first reconciliation", "empty");
      c.colSpan = 8;
      tr0.appendChild(c);
      tb.appendChild(tr0);
      return;
    }
    d.recon.forEach(function (r) {
      var tr = el("tr", r.settled && r.lost === 0 ? "settled" : "");
      tr.appendChild(td(el("span", "dest-name", r.dest)));
      tr.appendChild(td(fmt(r.expected), "num"));
      tr.appendChild(td(fmt(r.received), "num"));
      var out = td(fmt(r.outstanding), "num");
      if (r.outstanding > 0) out.style.color = "var(--warn)";
      tr.appendChild(out);
      tr.appendChild(td(fmt(r.dup) + " / " + fmt(r.stored_dup), "num"));
      tr.appendChild(td(fmt(r.dlq), "num"));
      tr.appendChild(td(fmt(r.misrouted), "num"));
      var lost = el("td", "num lost-cell");
      if (r.lost == null) lost.appendChild(tag("收敛中 settling", "warn"));
      else lost.appendChild(el("span", r.lost === 0 ? "lost-ok" : "lost-bad", fmt(r.lost)));
      tr.appendChild(lost);
      tb.appendChild(tr);
    });
  }

  function renderLog(d) {
    var ul = $("action-log");
    ul.textContent = "";
    (d.log || []).forEach(function (l) {
      var li = el("li");
      li.appendChild(el("time", null, hhmmss(l.at)));
      li.appendChild(el("span", l.ok ? "" : "fail", l.text));
      ul.appendChild(li);
    });
    var line = "负载 load: " + (d.eps || 0).toFixed(0) + " eps · sent " + fmt(d.sent) + " · acked " + fmt(d.acked) +
      " · 429/503 " + fmt(d.refused) + " · 连接失败 failed " + fmt(d.failed);
    if (d.crashes) line += " · 模拟崩溃 simulated crashes " + d.crashes;
    if (d.last_err) line += " · last error: " + d.last_err;
    $("load-line").textContent = line;
  }

  var openSeqs = {};
  function renderMasked(s) {
    var hits = $("mask-hits");
    hits.textContent = "";
    ["id_card", "bank_card", "phone", "email"].forEach(function (k) {
      var c = el("span", "chip", PII[k]);
      c.appendChild(el("b", null, fmt((s.mask_hits || {})[k] || 0)));
      hits.appendChild(c);
    });
    var ol = $("masked-list");
    ol.textContent = "";
    var list = (s.demo && s.demo.masked) || [];
    if (!list.length) { ol.appendChild(el("li", "empty", "暂无事件 · no events yet")); return; }
    list.forEach(function (m) {
      var li = el("li");
      var meta = el("div", "m-meta");
      meta.appendChild(el("span", "m-seq", "#" + m.seq + " · " + hhmmss(m.at)));
      meta.appendChild(tag(CLASS[m.class_uid] || ("class " + m.class_uid)));
      var lv = LEVEL[m.level] || LEVEL.unset;
      meta.appendChild(tag("分级 " + lv[0] + " · " + lv[1], lv[2]));
      (m.data_class || []).forEach(function (c) { meta.appendChild(tag(c, "info")); });
      meta.appendChild(tag("命中 hits " + m.hits, m.hits ? "ok" : ""));
      li.appendChild(meta);
      var body = el("div", "m-body" + (openSeqs[m.seq] ? " open" : ""), m.masked);
      body.title = "点击展开 · click to expand";
      body.addEventListener("click", function () { openSeqs[m.seq] = !openSeqs[m.seq]; body.classList.toggle("open"); });
      li.appendChild(body);
      ol.appendChild(li);
    });
  }

  function setMode(isDemo) {
    if (isDemo === demo) return;
    demo = isDemo;
    document.querySelectorAll(".demo-only").forEach(function (n) { n.classList.toggle("hidden", !demo); });
    $("demo-badge").classList.toggle("hidden", !demo);
    $("title-zh").textContent = demo ? "安全日志网关 · 演示控制台" : "安全日志网关 · 运行面板";
    $("title-en").textContent = demo ? "LogGate · Demo console" : "LogGate · Operations dashboard";
    document.title = demo ? "LogGate 演示" : "LogGate 控制台";
  }

  function render(s) {
    setMode(s.mode === "demo");
    renderStatus(s);
    renderKPIs(s);
    renderDests(s);
    renderChart(s);
    if (demo && s.demo) {
      renderRecon(s.demo);
      renderLog(s.demo);
      renderMasked(s);
      $("foot-left").textContent = "LogGate demo · data dir: " + s.demo.data_dir;
    }
    $("foot-right").textContent = "更新于 updated " + hhmmss(s.at) + " · WAL segments " + fmt(s.gateway.wal_segments) +
      " · replayed " + fmt(s.gateway.replayed);
  }

  // ---- stream -----------------------------------------------------------
  function connect() {
    var es = new EventSource("api/stream");
    es.addEventListener("snapshot", function (ev) {
      try { render(JSON.parse(ev.data)); } catch (e) { console.error(e); }
    });
    es.onerror = function () {
      $("status-pill").className = "pill bad";
      $("status-text").textContent = "连接中断 Disconnected — retrying";
    };
  }
  connect();
})();
