// Customer web chat over the hub WebSocket (/v1/hub/ws/customer). The session token is kept in
// this browser's localStorage (24 h) and sent in the first frame, never in the URL. Reconnects
// automatically and resumes from the last sequence number.
(function () {
  "use strict";
  var root = document.getElementById("chat");
  if (!root) return;
  var $ = function (id) { return document.getElementById(id); };
  var widget = root.getAttribute("data-widget");
  var KEY = "occ-chat-" + widget;
  var st = { token: null, ws: null, lastSeq: 0, seen: {}, retry: 0, pending: {} };

  function store(v) { try { if (v) localStorage.setItem(KEY, v); else localStorage.removeItem(KEY); } catch (e) { /* private mode */ } }
  function load() { try { return localStorage.getItem(KEY); } catch (e) { return null; } }
  function setConn(t, cls) { var c = $("conn"); c.textContent = t; c.className = "badge " + cls; }
  function uid() {
    var a = new Uint8Array(12);
    window.crypto.getRandomValues(a);
    return Array.prototype.map.call(a, function (b) { return ("0" + b.toString(16)).slice(-2); }).join("");
  }

  function addMessage(m) {
    if (st.seen[m.id]) return;
    st.seen[m.id] = true;
    if (m.seq > st.lastSeq) st.lastSeq = m.seq;
    var li = document.createElement("li");
    li.className = "hub-msg " + (m.from === "me" ? "hub-outbound" : "hub-inbound");
    var meta = document.createElement("div");
    meta.className = "hub-meta";
    meta.textContent = (m.from === "me" ? "You" : "Agent") + " · " + new Date(m.created_at).toLocaleTimeString();
    var body = document.createElement("div");
    body.className = "hub-body";
    body.textContent = m.body;
    li.appendChild(meta);
    li.appendChild(body);
    var ol = $("messages");
    ol.appendChild(li);
    ol.scrollTop = ol.scrollHeight;
  }

  function markSeen() {
    if (st.ws && st.ws.readyState === 1 && st.lastSeq) st.ws.send(JSON.stringify({ type: "seen", seq: st.lastSeq }));
  }

  function showChat() {
    $("start").hidden = true;
    $("messages").hidden = false;
    $("send").hidden = false;
    $("reset").hidden = false;
  }

  function connect() {
    setConn("connecting…", "badge-muted");
    var ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/v1/hub/ws/customer");
    st.ws = ws;
    ws.onopen = function () {
      st.retry = 0;
      ws.send(JSON.stringify({ type: "auth", token: st.token, last_seq: st.lastSeq }));
    };
    ws.onmessage = function (e) {
      var f;
      try { f = JSON.parse(e.data); } catch (err) { return; }
      if (f.type === "welcome") {
        setConn("connected", "badge-ok");
        showChat();
        f.messages.forEach(addMessage);
        Object.keys(st.pending).forEach(function (k) { ws.send(JSON.stringify(st.pending[k])); });
        markSeen();
      } else if (f.type === "message.new") {
        addMessage(f.message);
        if (document.visibilityState === "visible") markSeen();
      } else if (f.type === "ack") {
        delete st.pending[f.client_msg_id];
        addMessage(f.message);
      } else if (f.type === "error") {
        $("error").textContent = f.message;
        if (f.code === "UNAUTHENTICATED") { store(null); st.token = null; }
      } else if (f.type === "reconnect") {
        setConn("server restarting — reconnecting…", "badge-warn");
      }
    };
    ws.onclose = function (e) {
      if (!st.token) { setConn("chat ended — start a new chat", "badge-muted"); $("start").hidden = false; return; }
      var wait = Math.min(10000, 300 * Math.pow(2, st.retry++)) + Math.floor(Math.random() * 300);
      setConn("disconnected — retrying", "badge-warn");
      setTimeout(connect, wait);
    };
  }

  $("start").addEventListener("submit", function (e) {
    e.preventDefault();
    $("error").textContent = "";
    fetch("/v1/hub/customer/sessions", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ widget_key: widget, name: $("name").value })
    }).then(function (r) { return r.json().then(function (j) { return { ok: r.ok, j: j }; }); })
      .then(function (res) {
        if (!res.ok) { $("error").textContent = (res.j.error && res.j.error.message) || "Could not start the chat"; return; }
        st.token = res.j.data.token;
        store(st.token);
        connect();
      })
      .catch(function () { $("error").textContent = "Network error — try again."; });
  });

  $("send").addEventListener("submit", function (e) {
    e.preventDefault();
    var text = $("text").value.trim();
    if (!text) return;
    var frame = { type: "message.send", client_msg_id: uid(), body: text };
    st.pending[frame.client_msg_id] = frame;
    if (st.ws && st.ws.readyState === 1) st.ws.send(JSON.stringify(frame));
    $("text").value = "";
  });

  $("reset").addEventListener("click", function () {
    store(null);
    st.token = null;
    if (st.ws) st.ws.close();
    location.reload();
  });

  document.addEventListener("visibilitychange", function () { if (document.visibilityState === "visible") markSeen(); });
  setInterval(function () { if (st.ws && st.ws.readyState === 1) st.ws.send(JSON.stringify({ type: "ping" })); }, 25000);

  st.token = load();
  if (st.token) connect();
})();
