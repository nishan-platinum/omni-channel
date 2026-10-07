// Agent desktop over the hub WebSocket (/v1/hub/ws/agent, cookie session, same origin).
// Reconnects automatically and resumes each conversation from the last sequence number it saw,
// so nothing is missed across a node restart or a network blip. No build step, no framework.
(function () {
  "use strict";
  var desk = document.getElementById("desk");
  if (!desk) return;
  var $ = function (id) { return document.getElementById(id); };
  var state = { convs: {}, order: [], active: null, ws: null, retry: 0, pending: {} };

  function wsUrl() {
    return (location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/v1/hub/ws/agent";
  }

  function setConn(text, cls) {
    var c = $("conn");
    c.textContent = text;
    c.className = "badge " + cls;
  }

  function uid() {
    var a = new Uint8Array(12);
    (window.crypto || window.msCrypto).getRandomValues(a);
    return Array.prototype.map.call(a, function (b) { return ("0" + b.toString(16)).slice(-2); }).join("");
  }

  function send(obj) {
    if (state.ws && state.ws.readyState === 1) { state.ws.send(JSON.stringify(obj)); return true; }
    return false;
  }

  function upsertConv(c) {
    var cur = state.convs[c.id];
    if (!cur) {
      cur = state.convs[c.id] = { conv: c, messages: [], lastSeq: 0 };
      state.order.push(c.id);
    } else {
      cur.conv = c;
    }
    return cur;
  }

  function addMessage(m) {
    var c = state.convs[m.conversation_id];
    if (!c) return;
    for (var i = 0; i < c.messages.length; i++) {
      if (c.messages[i].id === m.id) { c.messages[i] = m; return; }
    }
    c.messages.push(m);
    c.messages.sort(function (a, b) { return a.seq - b.seq; });
    if (m.seq > c.lastSeq) c.lastSeq = m.seq;
    if (m.conversation_id !== state.active) c.unread = (c.unread || 0) + 1;
  }

  function label(c) {
    return (c.customer_name ? c.customer_name + " · " : "") + c.customer_address;
  }

  function renderList() {
    var ul = $("conversations");
    ul.innerHTML = "";
    var open = state.order.filter(function (id) { return state.convs[id].conv.status === "assigned"; });
    $("no-convs").hidden = open.length > 0;
    open.forEach(function (id) {
      var c = state.convs[id];
      var li = document.createElement("li");
      var b = document.createElement("button");
      b.type = "button";
      b.className = "hub-conv" + (id === state.active ? " active" : "");
      b.setAttribute("aria-pressed", id === state.active ? "true" : "false");
      var ch = document.createElement("span");
      ch.className = "tag";
      ch.textContent = c.conv.channel;
      b.appendChild(ch);
      b.appendChild(document.createTextNode(" " + label(c.conv)));
      if (c.unread) {
        var u = document.createElement("span");
        u.className = "badge badge-info";
        u.textContent = c.unread + " new";
        b.appendChild(document.createTextNode(" "));
        b.appendChild(u);
      }
      b.addEventListener("click", function () { select(id); });
      li.appendChild(b);
      ul.appendChild(li);
    });
  }

  function renderThread() {
    var ol = $("messages");
    ol.innerHTML = "";
    var c = state.active && state.convs[state.active];
    if (!c || c.conv.status !== "assigned") {
      $("thread-title").textContent = "Select a conversation";
      $("reply").hidden = true;
      $("close-conv").hidden = true;
      return;
    }
    $("thread-title").textContent = c.conv.channel + " · " + label(c.conv) + " · skill " + c.conv.required_skill;
    $("reply").hidden = c.conv.channel === "voice";
    $("close-conv").hidden = false;
    c.messages.forEach(function (m) {
      var li = document.createElement("li");
      li.className = "hub-msg hub-" + m.direction;
      var meta = document.createElement("div");
      meta.className = "hub-meta";
      var who = m.direction === "inbound" ? "Customer" : m.direction === "outbound" ? "You" : "Event";
      meta.textContent = "#" + m.seq + " · " + who + " · " + new Date(m.created_at).toLocaleTimeString();
      if (m.delivery_status) {
        var s = document.createElement("span");
        s.className = "badge " + ({ read: "badge-ok", delivered: "badge-ok", failed: "badge-bad" }[m.delivery_status] || "badge-info");
        s.textContent = m.delivery_status;
        meta.appendChild(document.createTextNode(" "));
        meta.appendChild(s);
      }
      var body = document.createElement("div");
      body.className = "hub-body";
      body.textContent = m.body;
      li.appendChild(meta);
      li.appendChild(body);
      ol.appendChild(li);
    });
    ol.scrollTop = ol.scrollHeight;
  }

  function select(id) {
    state.active = id;
    if (state.convs[id]) state.convs[id].unread = 0;
    renderList();
    renderThread();
  }

  function render() { renderList(); renderThread(); }

  function onFrame(f) {
    switch (f.type) {
      case "welcome":
        $("presence").value = f.agent.presence;
        $("presence").disabled = false;
        f.conversations.forEach(function (x) {
          upsertConv(x.conversation);
          x.messages.forEach(addMessage);
        });
        // Conversations no longer assigned to us (re-queued while away) are dropped.
        var live = f.conversations.map(function (x) { return x.conversation.id; });
        state.order.forEach(function (id) { if (live.indexOf(id) < 0) state.convs[id].conv.status = "closed"; });
        if (!state.active && live.length) state.active = live[0];
        setConn("connected · node " + f.node, "badge-ok");
        render();
        break;
      case "conversation.assigned":
        upsertConv(f.conversation);
        f.messages.forEach(addMessage);
        if (!state.active) state.active = f.conversation.id;
        render();
        break;
      case "conversation.updated":
        upsertConv(f.conversation);
        if (f.conversation.status !== "assigned" && state.active === f.conversation.id) state.active = null;
        render();
        break;
      case "message.new":
        addMessage(f.message);
        render();
        break;
      case "message.status":
        var c = state.convs[f.conversation_id];
        if (c) c.messages.forEach(function (m) { if (m.id === f.message_id) m.delivery_status = f.status; });
        renderThread();
        break;
      case "ack":
        delete state.pending[f.client_msg_id];
        addMessage(f.message);
        renderThread();
        break;
      case "presence":
        $("presence").value = f.status;
        break;
      case "error":
        $("reply-error").textContent = f.message + " (" + f.code + ")";
        break;
      case "reconnect":
        setConn("server restarting — reconnecting…", "badge-warn");
        break;
    }
  }

  function connect() {
    setConn("connecting…", "badge-muted");
    var ws = new WebSocket(wsUrl());
    state.ws = ws;
    ws.onopen = function () {
      state.retry = 0;
      var resume = {};
      state.order.forEach(function (id) { resume[id] = state.convs[id].lastSeq; });
      ws.send(JSON.stringify({ type: "hello", resume: resume }));
      // Re-send replies that were never acknowledged (same client_msg_id → no duplicates).
      Object.keys(state.pending).forEach(function (k) { ws.send(JSON.stringify(state.pending[k])); });
    };
    ws.onmessage = function (e) {
      try { onFrame(JSON.parse(e.data)); } catch (err) { /* ignore malformed */ }
    };
    ws.onclose = function (e) {
      $("presence").disabled = true;
      if (e.code === 4401 || e.code === 1008) { setConn("signed out — reload to sign in", "badge-bad"); return; }
      var wait = Math.min(10000, 300 * Math.pow(2, state.retry++)) + Math.floor(Math.random() * 300);
      setConn("disconnected — retrying in " + Math.round(wait / 1000) + "s", "badge-warn");
      setTimeout(connect, wait);
    };
  }

  $("presence").addEventListener("change", function () {
    send({ type: "presence.set", status: $("presence").value });
  });
  $("reply").addEventListener("submit", function (e) {
    e.preventDefault();
    $("reply-error").textContent = "";
    var text = $("reply-text").value.trim();
    if (!text || !state.active) return;
    var frame = { type: "message.send", conversation_id: state.active, client_msg_id: uid(), body: text };
    state.pending[frame.client_msg_id] = frame;
    if (!send(frame)) $("reply-error").textContent = "Not connected — the reply will be sent when the connection is back.";
    $("reply-text").value = "";
  });
  $("close-conv").addEventListener("click", function () {
    if (state.active && window.confirm("Close this conversation?")) send({ type: "conversation.close", conversation_id: state.active });
  });
  setInterval(function () { send({ type: "ping" }); }, 25000);
  connect();
})();
