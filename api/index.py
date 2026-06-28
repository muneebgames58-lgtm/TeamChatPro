from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File, Form, Response
from fastapi.responses import JSONResponse, HTMLResponse
from jose import jwt, JWTError
import datetime, os, uuid, base64, json, logging
import httpx
import pusher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# ---------- Environment Validation ----------
REQUIRED_ENV = [
    "TURSO_DATABASE_URL",
    "TURSO_AUTH_TOKEN",
    "PUSHER_APP_ID", "PUSHER_KEY", "PUSHER_SECRET", "PUSHER_CLUSTER",
    "JWT_SECRET"
]
missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
if missing:
    raise RuntimeError(f"Missing environment variables: {', '.join(missing)}")

# ---------- Config ----------
TURSO_URL = os.environ["TURSO_DATABASE_URL"].rstrip("/")
TURSO_TOKEN = os.environ["TURSO_AUTH_TOKEN"]
PUSHER_APP_ID = os.environ["PUSHER_APP_ID"]
PUSHER_KEY = os.environ["PUSHER_KEY"]
PUSHER_SECRET = os.environ["PUSHER_SECRET"]
PUSHER_CLUSTER = os.environ["PUSHER_CLUSTER"]
JWT_SECRET = os.environ["JWT_SECRET"]

pusher_client = pusher.Pusher(app_id=PUSHER_APP_ID, key=PUSHER_KEY, secret=PUSHER_SECRET, cluster=PUSHER_CLUSTER, ssl=True)

# ---------- Turso HTTP Helpers ----------
def _infer_type(value):
    if isinstance(value, int): return "integer"
    if isinstance(value, float): return "real"
    if value is None: return "null"
    return "text"

async def turso_request(sql: str, params: list = None):
    url = f"{TURSO_URL}/v2/pipeline"
    headers = {"Authorization": f"Bearer {TURSO_TOKEN}", "Content-Type": "application/json"}
    args_list = []
    if params:
        for p in params:
            t = _infer_type(p)
            args_list.append({"type": t, "value": None if t == "null" else (int(p) if t == "integer" else (float(p) if t == "real" else str(p)))})
    body = {"requests": [{"type": "execute", "stmt": {"sql": sql, "args": args_list}}, {"type": "close"}]}
    async with httpx.AsyncClient() as client:
        resp = await client.post(url, headers=headers, json=body, timeout=30.0)
        data = resp.json()
        if resp.status_code != 200: raise HTTPException(500, f"DB error: {resp.status_code}")
        results = data.get("results", [])
        if not results: return {"rows": [], "rows_written": 0, "rows_read": 0}
        first = results[0]
        if first.get("type") == "ok":
            inner = first.get("response", {})
            if inner.get("type") == "error": raise HTTPException(500, f"SQL error: {inner.get('error')}")
            result = inner.get("result", {})
        elif first.get("type") == "execute":
            result = first.get("result", {})
        else:
            return {"rows": [], "rows_written": 0, "rows_read": 0}
        cols = [c["name"] for c in result.get("cols", [])]
        rows = []
        for row in result.get("rows", []):
            vals = [v.get("value") if isinstance(v, dict) else v for v in row]
            rows.append(dict(zip(cols, vals)))
        return {"rows": rows, "rows_written": result.get("rows_written", 0), "rows_read": result.get("rows_read", 0), "rows_affected": result.get("rows_affected", 0)}

async def db_execute(sql: str, params: list = None) -> list:
    return (await turso_request(sql, params))["rows"]

async def db_run(sql: str, params: list = None) -> dict:
    return await turso_request(sql, params)

# ---------- Auth Dependency ----------
async def get_current_user(request: Request):
    token = request.cookies.get("token")
    if not token: raise HTTPException(401)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        rows = await db_execute("SELECT * FROM users WHERE id = ?", [payload["user_id"]])
        if not rows: raise HTTPException(401)
        return rows[0]
    except JWTError: raise HTTPException(401)

async def check_membership(ch_id, user_id):
    if not await db_execute("SELECT 1 FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user_id]):
        raise HTTPException(403)

def serialize_message(row):
    row["created_at"] = str(row["created_at"])
    return row

def group_reactions(rows):
    groups = {}
    for r in rows:
        e = r["emoji"]
        groups.setdefault(e, {"emoji": e, "count": 0, "users": []})
        groups[e]["count"] += 1
        groups[e]["users"].append({"user_id": r["user_id"], "username": r.get("username","")})
    return list(groups.values())

# ---------- Embedded Frontend (Full Featured) ----------
FRONTEND_HTML = r"""
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=no" />
  <title>Chatta</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet" />
  <style>
    :root {
      --bg-main: #111b21; --bg-panel: #202c33; --bg-chat: #0b141a;
      --outgoing-bubble: #005c4b; --incoming-bubble: #202c33; --accent: #00a884;
      --red: #f15c6d; --text-primary: #e9edef; --text-secondary: #8696a0;
      --radius-bubble: 8px; --radius-modal: 12px; --font-family: 'Inter', sans-serif;
    }
    [data-theme="light"] {
      --bg-main: #f0f2f5; --bg-panel: #ffffff; --bg-chat: #efeae2;
      --outgoing-bubble: #d9fdd3; --incoming-bubble: #ffffff; --accent: #008069;
      --text-primary: #111b21; --text-secondary: #667781;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: var(--font-family); background: var(--bg-main); color: var(--text-primary); height: 100vh; overflow: hidden; }
    #app { display: flex; height: 100vh; }
    .sidebar { width: 35%; min-width: 320px; max-width: 500px; background: var(--bg-panel); display: flex; flex-direction: column; border-right: 1px solid rgba(0,0,0,0.2); }
    .sidebar-header { display: flex; align-items: center; padding: 12px 16px; background: var(--bg-panel); gap: 10px; }
    .sidebar-header .avatar { width: 44px; height: 44px; border-radius: 50%; cursor: pointer; }
    .sidebar-header .user-info { flex: 1; }
    .sidebar-header .user-name { font-weight: 600; font-size: 15px; }
    .sidebar-header .user-status { font-size: 12px; color: var(--text-secondary); }
    .search-box { padding: 0 12px 10px; position: relative; }
    .search-box input { width: 100%; padding: 10px 12px 10px 40px; border-radius: 8px; border: none; background: var(--bg-main); color: var(--text-primary); font-size: 14px; outline: none; }
    .search-box .search-icon { position: absolute; left: 22px; top: 50%; transform: translateY(-50%); color: var(--text-secondary); font-size: 16px; }
    .channel-list { overflow-y: auto; flex: 1; }
    .channel-item { display: flex; align-items: center; padding: 12px 16px; cursor: pointer; transition: background 0.15s; gap: 12px; border-bottom: 1px solid rgba(255,255,255,0.05); }
    .channel-item:hover { background: rgba(255,255,255,0.04); }
    .channel-item.active { background: rgba(0,168,132,0.15); }
    .channel-avatar { width: 48px; height: 48px; border-radius: 50%; flex-shrink: 0; }
    .channel-info { flex: 1; overflow: hidden; }
    .channel-name { font-weight: 500; font-size: 15px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .channel-last-msg { font-size: 13px; color: var(--text-secondary); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; margin-top: 2px; }
    .channel-meta { display: flex; flex-direction: column; align-items: flex-end; gap: 4px; }
    .unread-badge { background: var(--accent); color: white; border-radius: 12px; min-width: 22px; height: 22px; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: 600; padding: 0 6px; }
    .channel-time { font-size: 11px; color: var(--text-secondary); }
    .chat-area { flex: 1; display: flex; flex-direction: column; background: var(--bg-chat); position: relative; }
    .chat-header { display: flex; align-items: center; padding: 12px 16px; background: var(--bg-panel); gap: 10px; border-bottom: 1px solid rgba(0,0,0,0.1); }
    .chat-title { flex: 1; font-weight: 600; font-size: 16px; }
    .chat-messages { flex: 1; overflow-y: auto; padding: 20px 60px; background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100"><path d="M30 20 L30 40 M40 20 L40 40 M50 20 L50 40 M60 20 L60 40 M70 20 L70 40" stroke="%23555" stroke-width="0.5" opacity="0.03"/></svg>'); display: flex; flex-direction: column; gap: 2px; }
    .message-row { display: flex; max-width: 70%; margin-bottom: 2px; position: relative; }
    .message-row.outgoing { align-self: flex-end; }
    .message-row.incoming { align-self: flex-start; }
    .message-bubble { padding: 8px 14px; border-radius: var(--radius-bubble); font-size: 14px; line-height: 1.5; word-break: break-word; position: relative; box-shadow: 0 1px 2px rgba(0,0,0,0.1); }
    .outgoing .message-bubble { background: var(--outgoing-bubble); border-top-right-radius: 2px; }
    .incoming .message-bubble { background: var(--incoming-bubble); border-top-left-radius: 2px; }
    .message-meta { display: flex; justify-content: flex-end; align-items: center; gap: 4px; font-size: 11px; color: var(--text-secondary); margin-top: 4px; }
    .reply-preview { background: rgba(0,0,0,0.1); border-left: 3px solid var(--accent); padding: 4px 10px; margin-bottom: 4px; border-radius: 4px; font-size: 12px; color: var(--text-secondary); cursor: pointer; }
    .chat-input { padding: 10px 16px; background: var(--bg-panel); display: flex; align-items: center; gap: 10px; }
    .chat-input textarea { flex: 1; resize: none; border-radius: 24px; border: none; padding: 10px 20px; background: var(--bg-main); color: var(--text-primary); font-family: var(--font-family); font-size: 14px; outline: none; max-height: 100px; line-height: 1.4; }
    .send-btn { background: var(--accent); border: none; color: white; border-radius: 50%; width: 46px; height: 46px; display: flex; align-items: center; justify-content: center; cursor: pointer; font-size: 20px; }
    .modal-overlay { position: fixed; top:0; left:0; right:0; bottom:0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000; animation: fadeIn 0.2s; }
    .modal { background: var(--bg-panel); border-radius: var(--radius-modal); max-width: 500px; width: 90%; max-height: 85vh; overflow-y: auto; padding: 24px; color: var(--text-primary); }
    .modal h2 { margin-bottom: 16px; }
    .modal input, .modal textarea, .modal select { width: 100%; padding: 12px; margin-bottom: 16px; background: var(--bg-main); border: 1px solid var(--text-secondary); border-radius: 8px; color: var(--text-primary); font-size: 14px; }
    .modal button { padding: 12px 24px; background: var(--accent); border: none; color: white; border-radius: 8px; cursor: pointer; font-weight: 600; font-size: 14px; margin-right: 8px; }
    .context-menu { position: fixed; background: var(--bg-panel); border-radius: 8px; box-shadow: 0 8px 24px rgba(0,0,0,0.5); z-index: 2000; padding: 4px 0; min-width: 190px; display: none; }
    .context-menu-item { padding: 10px 20px; cursor: pointer; font-size: 14px; display: flex; align-items: center; gap: 10px; color: var(--text-primary); }
    .context-menu-item:hover { background: rgba(255,255,255,0.08); }
    .toast-container { position: fixed; bottom: 30px; left: 50%; transform: translateX(-50%); z-index: 3000; display: flex; flex-direction: column; align-items: center; }
    .toast { background: var(--accent); color: white; padding: 12px 28px; border-radius: 24px; margin-top: 8px; font-size: 14px; font-weight: 500; animation: slideUp 0.3s ease; }
    .hidden { display: none !important; }
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
    @keyframes slideUp { from { opacity: 0; transform: translateY(10px); } to { opacity: 1; transform: translateY(0); } }
    @media (max-width: 768px) {
      .sidebar { width: 100% !important; max-width: 100% !important; }
      .chat-area { display: none; }
      .chat-area.mobile-open { display: flex; }
      .chat-messages { padding: 10px 20px; }
    }
  </style>
</head>
<body>
  <div id="app"></div>
  <div class="context-menu" id="context-menu"></div>
  <div class="toast-container"></div>
  <div class="modal-overlay hidden" id="global-modal"></div>
  <script src="https://js.pusher.com/8.2.0/pusher.min.js"></script>
  <script>
    const API = '/api';
    let currentUser = null, activeChannel = null, channels = [], channelMessages = new Map();
    let socket = null, replyToMessageId = null, scrollAtBottom = true;

    const $ = s => document.querySelector(s);
    const toast = msg => {
      let c = $('.toast-container'); if(!c) { c=document.createElement('div'); c.className='toast-container'; document.body.appendChild(c); }
      const el = document.createElement('div'); el.className='toast'; el.textContent = msg; c.appendChild(el); setTimeout(() => el.remove(), 3000);
    };
    const formatDate = d => new Date(d).toLocaleDateString('en-US', {month:'short',day:'numeric',year:'numeric'});
    const formatTime = d => new Date(d).toLocaleTimeString('en-US', {hour:'2-digit',minute:'2-digit'});
    const formatFull = d => new Date(d).toLocaleString();
    const escapeHtml = t => t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\n/g,'<br>');

    async function apiFetch(url, opts={}) {
      const headers = { ...opts.headers };
      if (!(opts.body instanceof FormData)) headers['Content-Type'] = 'application/json';
      const res = await fetch(API+url, { credentials:'include', headers, ...opts });
      if (!res.ok) {
        const err = await res.json().catch(() => ({detail:'Request failed'}));
        throw new Error(err.detail || 'Error');
      }
      return res.json();
    }

    // Auth
    async function checkAuth() { try { currentUser = await apiFetch('/me'); } catch { currentUser = null; } }
    async function login(u, p) { await apiFetch('/login',{method:'POST',body:JSON.stringify({username:u.trim(),password:p})}); await checkAuth(); }
    async function register(u, e, p) { await apiFetch('/register',{method:'POST',body:JSON.stringify({username:u.trim(),email:e.trim(),password:p})}); await checkAuth(); }
    async function logout() { await apiFetch('/logout',{method:'POST'}); currentUser=null; activeChannel=null; channels=[]; if(socket)socket.disconnect(); }

    // Channels
    async function loadChannels() { channels = await apiFetch('/channels'); renderSidebar(); }
    async function createChannel(type, name, members=[]) {
      await apiFetch('/channels',{method:'POST',body:JSON.stringify({type,name,members})});
      await loadChannels();
    }
    async function leaveChannel(id) { await apiFetch(`/channels/${id}/leave`,{method:'POST'}); if(activeChannel===id) activeChannel=null; await loadChannels(); }
    async function archiveChannel(id) { await apiFetch(`/channels/${id}/archive`,{method:'POST'}); await loadChannels(); }
    async function pinChannel(id) { await apiFetch(`/channels/${id}/pin`,{method:'POST'}); await loadChannels(); }
    async function addMember(chId, userId) { await apiFetch(`/channels/${chId}/members`,{method:'POST',body:JSON.stringify({user_id:userId})}); }
    async function setDisappearing(chId, ttl) { await apiFetch(`/channels/${chId}/disappearing`,{method:'POST',body:JSON.stringify({ttl})}); }

    // Messages
    async function loadMessages(chId, before=null) {
      if(!chId) return;
      const url = before ? `/channels/${chId}/messages?before=${before}` : `/channels/${chId}/messages`;
      const msgs = await apiFetch(url);
      channelMessages.set(chId, msgs);
    }
    async function sendMessage(content, type='text', file=null) {
      if(!activeChannel) return;
      if(type==='text') await apiFetch(`/channels/${activeChannel}/messages`,{method:'POST',body:JSON.stringify({content,type,reply_to:replyToMessageId})});
      else { const fd = new FormData(); fd.append('content',content); fd.append('type',type); fd.append('file',file); if(replyToMessageId) fd.append('reply_to',replyToMessageId);
        await fetch(API+`/channels/${activeChannel}/messages`,{method:'POST',credentials:'include',body:fd}); }
      replyToMessageId=null;
    }
    async function deleteMessage(id) { await apiFetch(`/messages/${id}`,{method:'DELETE'}); }
    async function editMessage(id, content) { await apiFetch(`/messages/${id}`,{method:'PUT',body:JSON.stringify({content})}); }
    async function reactToMessage(msgId, emoji) { await apiFetch(`/messages/${msgId}/react`,{method:'POST',body:JSON.stringify({emoji})}); }
    async function pinMessage(msgId) { await apiFetch(`/messages/${msgId}/pin`,{method:'POST'}); }
    async function markRead(chId) { await apiFetch(`/channels/${chId}/read`,{method:'POST'}); }

    // Profile
    async function updateProfile(data) { await apiFetch('/profile',{method:'PUT',body:JSON.stringify(data)}); currentUser = await apiFetch('/me'); }
    async function changePassword(oldPwd, newPwd) { await apiFetch('/change-password',{method:'POST',body:JSON.stringify({old_password:oldPwd,new_password:newPwd})}); }
    async function blockUser(userId) { await apiFetch(`/block/${userId}`,{method:'POST'}); }
    async function unblockUser(userId) { await apiFetch(`/block/${userId}`,{method:'DELETE'}); }

    // Bookmarks
    async function addBookmark(msgId, folder='General') { await apiFetch('/bookmarks',{method:'POST',body:JSON.stringify({message_id:msgId,folder})}); }
    async function getBookmarks() { return await apiFetch('/bookmarks'); }
    async function deleteBookmark(id) { /* not implemented on backend, but we can add later */ }

    // Tasks
    async function createTask(msgId, title) { await apiFetch('/tasks',{method:'POST',body:JSON.stringify({message_id:msgId,title})}); }
    async function getTasks() { return await apiFetch('/tasks'); }
    async function toggleTask(id, isDone) { /* not implemented, would need backend route */ }

    // Drafts
    async function saveDraft(chId, content) { await apiFetch(`/channels/${chId}/draft`,{method:'POST',body:JSON.stringify({content})}); }
    async function getDraft(chId) { const d = await apiFetch(`/channels/${chId}/draft`); return d.content || ''; }

    // Custom Status
    async function setCustomStatus(content, emoji='', type='text') { await apiFetch('/status',{method:'POST',body:JSON.stringify({type,content,emoji})}); }
    async function getStatuses() { return await apiFetch('/statuses'); }

    // Admin
    async function getAdminChannels() { return await apiFetch('/admin/channels'); }

    // Pusher
    function initPusher() {
      if(socket) socket.disconnect();
      socket = new Pusher(""" + PUSHER_KEY + """, { cluster: """ + PUSHER_CLUSTER + """, authEndpoint: API+'/pusher/auth', encrypted: true });
      socket.connection.bind('connected', ()=>{ if(activeChannel) subscribeToChannel(activeChannel); });
    }
    function subscribeToChannel(chId) {
      const ch = socket.subscribe('private-channel-'+chId);
      ch.bind('new-message', data => {
        const msgs = channelMessages.get(chId)||[];
        msgs.push(data.message);
        channelMessages.set(chId, msgs);
        if(activeChannel===chId) renderMessages();
      });
      ch.bind('message-updated', data => {
        const msgs = channelMessages.get(chId);
        if(msgs){ const idx = msgs.findIndex(m=>m.id===data.message.id); if(idx>-1) msgs[idx] = {...msgs[idx], ...data.message}; if(activeChannel===chId) renderMessages(); }
      });
      ch.bind('message-deleted', data => {
        const msgs = channelMessages.get(chId);
        if(msgs){ const m = msgs.find(m=>m.id===data.message_id); if(m) m.is_deleted=true; if(activeChannel===chId) renderMessages(); }
      });
    }

    // Rendering
    function renderAuthScreen() {
      $('#app').innerHTML = `<div class="modal-overlay" style="display:flex;"><div class="modal">
        <h2>Welcome to Chatta</h2>
        <input id="auth-username" placeholder="Username" />
        <input id="auth-email" placeholder="Email (register)" />
        <input id="auth-password" type="password" placeholder="Password" />
        <div style="display:flex; gap:10px;">
          <button id="btn-login">Login</button>
          <button id="btn-register">Register</button>
        </div>
      </div></div>`;
      $('#btn-login').onclick = async () => { try { await login($('#auth-username').value, $('#auth-password').value); initApp(); } catch(e){ toast(e.message); } };
      $('#btn-register').onclick = async () => { try { await register($('#auth-username').value, $('#auth-email').value, $('#auth-password').value); initApp(); } catch(e){ toast(e.message); } };
    }

    function renderMainUI() {
      $('#app').innerHTML = `
        <div class="sidebar">
          <div class="sidebar-header">
            <img src="${currentUser.avatar_url}" class="avatar" onclick="openProfileModal()" />
            <div class="user-info">
              <div class="user-name">${currentUser.username}</div>
              <div class="user-status">${currentUser.status_preset}</div>
            </div>
            <button onclick="toggleTheme()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;font-size:18px;" title="Toggle theme">🌓</button>
            <button onclick="openNewChatModal()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;font-size:20px;" title="New chat">✚</button>
            <button id="btn-logout" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;font-size:20px;" title="Logout">⏻</button>
          </div>
          <div class="search-box">
            <span class="search-icon">🔍</span>
            <input id="global-search" placeholder="Search or start new chat" />
          </div>
          <div class="channel-list" id="channel-list"></div>
        </div>
        <div class="chat-area" id="chat-area">
          <div class="chat-header">
            <button class="back-btn" id="mobile-back">←</button>
            <div class="chat-title" id="channel-title">Select a channel</div>
            <div style="display:flex; gap:8px;">
              <button onclick="openPinnedMessages()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;" title="Pinned">📌</button>
              <button onclick="openBookmarksModal()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;" title="Bookmarks">⭐</button>
              <button onclick="openTasksModal()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;" title="Tasks">✅</button>
              <button onclick="openStatusesModal()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;" title="Statuses">📊</button>
              <button onclick="openChannelInfoModal()" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;" title="Channel info">⚙</button>
              <button onclick="openAdminPanel()" id="btn-admin" style="background:none;border:none;color:var(--text-secondary);cursor:pointer;display:${currentUser.role==='admin'?'':'none'}" title="Admin">🔧</button>
            </div>
          </div>
          <div class="chat-messages" id="chat-messages"></div>
          <div class="chat-input">
            <textarea id="message-input" placeholder="Type a message..." rows="1"></textarea>
            <button class="send-btn" id="btn-send">➤</button>
          </div>
        </div>`;
      renderSidebar();
      document.getElementById('btn-logout').onclick = async () => { await logout(); initApp(); };
      document.getElementById('btn-send').onclick = () => { const inp = $('#message-input'); if(inp.value.trim()) { sendMessage(inp.value); inp.value=''; } };
      document.getElementById('message-input').addEventListener('keydown', e => { if(e.key==='Enter' && !e.shiftKey) { e.preventDefault(); $('#btn-send').click(); } });
      document.getElementById('mobile-back').addEventListener('click', () => { $('#chat-area').classList.remove('mobile-open'); });
      document.getElementById('global-search').addEventListener('keydown', e => { if(e.key==='Enter') globalSearch(e.target.value); });
      // Load draft
      (async () => {
        if(activeChannel) { const draft = await getDraft(activeChannel); if(draft) $('#message-input').value = draft; }
      })();
    }

    function renderSidebar() {
      const list = $('#channel-list');
      if(!list) return;
      list.innerHTML = channels.map(ch => {
        const lastMsg = ch.last_msg || '';
        const time = ch.last_msg_time ? formatTime(ch.last_msg_time) : '';
        return `<div class="channel-item ${ch.id===activeChannel?'active':''}" data-channel="${ch.id}">
          <img src="${ch.avatar_url || '/api/placeholder/48/48'}" class="channel-avatar" />
          <div class="channel-info">
            <div class="channel-name">${ch.name}</div>
            <div class="channel-last-msg">${lastMsg}</div>
          </div>
          <div class="channel-meta">
            ${time ? `<div class="channel-time">${time}</div>` : ''}
            ${ch.unread_count ? `<div class="unread-badge">${ch.unread_count}</div>` : ''}
          </div>
        </div>`;
      }).join('');
      document.querySelectorAll('.channel-item').forEach(el => el.onclick = () => switchChannel(el.dataset.channel));
    }

    async function switchChannel(id) {
      if(!id || activeChannel===id) return;
      activeChannel = id;
      $('#channel-title').textContent = channels.find(c=>c.id===id)?.name || '';
      $('#chat-messages').innerHTML = '<div style="text-align:center;padding:40px;">Loading...</div>';
      await loadMessages(id);
      renderMessages();
      subscribeToChannel(id);
      markRead(id);
      renderSidebar();
      $('#chat-area').classList.add('mobile-open');
      scrollToBottom(true);
      // Save draft of previous channel? We'll ignore for now.
    }

    function renderMessages() {
      const msgs = channelMessages.get(activeChannel)||[];
      let html = '';
      msgs.forEach(m => {
        if(m.is_deleted) {
          html += `<div class="message-row" style="justify-content:center;"><div style="font-size:12px;color:var(--text-secondary);font-style:italic;">This message was deleted</div></div>`;
          return;
        }
        const isOut = m.user_id === currentUser.id;
        html += `<div class="message-row ${isOut?'outgoing':'incoming'}" data-id="${m.id}" oncontextmenu="showContextMenu(event,'${m.id}')">`;
        if(m.reply_to) html += `<div class="reply-preview" onclick="scrollToMessage('${m.reply_to}')">↩ Reply</div>`;
        html += `<div><div class="message-bubble">${escapeHtml(m.content)}</div>`;
        html += `<div class="message-meta">
          ${m.is_edited ? '<span style="font-size:11px;">✎</span>' : ''}
          <span class="ticks" style="color:${m.read_status==='read'?'var(--accent)':'var(--text-secondary)'}">${m.read_status==='read'?'✓✓':(m.read_status==='delivered'?'✓✓':'✓')}</span>
          <span style="font-size:11px;">${formatTime(m.created_at)}</span>
        </div></div></div>`;
      });
      $('#chat-messages').innerHTML = html;
      if(scrollAtBottom) $('#chat-messages').scrollTop = $('#chat-messages').scrollHeight;
    }

    // Modals
    function openModal(html) {
      const modal = $('#global-modal');
      modal.innerHTML = `<div class="modal">${html}</div>`;
      modal.classList.remove('hidden');
      modal.querySelector('.modal').addEventListener('click', e => e.stopPropagation());
      modal.onclick = () => modal.classList.add('hidden');
    }

    // Profile Modal
    function openProfileModal() {
      openModal(`
        <h2>Edit Profile</h2>
        <input id="prof-bio" placeholder="Bio" value="${currentUser.bio||''}" />
        <input id="prof-phone" placeholder="Phone" value="${currentUser.phone||''}" />
        <input id="prof-avatar" placeholder="Avatar URL" value="${currentUser.avatar_url}" />
        <select id="prof-status">
          <option value="online" ${currentUser.status_preset==='online'?'selected':''}>Online</option>
          <option value="away" ${currentUser.status_preset==='away'?'selected':''}>Away</option>
          <option value="busy" ${currentUser.status_preset==='busy'?'selected':''}>Busy</option>
        </select>
        <button onclick="saveProfile()">Save</button>
        <hr style="margin:20px 0;border-color:#333;">
        <h3>Change Password</h3>
        <input id="old-pwd" type="password" placeholder="Old password" />
        <input id="new-pwd" type="password" placeholder="New password" />
        <button onclick="changePwd()">Change Password</button>
        <hr style="margin:20px 0;border-color:#333;">
        <h3>Block / Unblock User</h3>
        <input id="block-user-id" placeholder="User ID" />
        <button onclick="blockUserFunc()">Block</button>
        <button onclick="unblockUserFunc()">Unblock</button>
      `);
      window.saveProfile = async () => {
        await updateProfile({ bio: $('#prof-bio').value, phone: $('#prof-phone').value, avatar_url: $('#prof-avatar').value, status_preset: $('#prof-status').value });
        toast('Profile updated'); $('#global-modal').classList.add('hidden'); renderMainUI();
      };
      window.changePwd = async () => {
        await changePassword($('#old-pwd').value, $('#new-pwd').value);
        toast('Password changed'); $('#global-modal').classList.add('hidden');
      };
      window.blockUserFunc = async () => { await blockUser(parseInt($('#block-user-id').value)); toast('Blocked'); };
      window.unblockUserFunc = async () => { await unblockUser(parseInt($('#block-user-id').value)); toast('Unblocked'); };
    }

    // New Chat Modal
    function openNewChatModal() {
      openModal(`
        <h2>New Chat</h2>
        <select id="new-chat-type">
          <option value="direct">Direct Message</option>
          <option value="group">Group</option>
          <option value="announcement">Announcement</option>
        </select>
        <input id="new-chat-name" placeholder="Group/Announcement name (if applicable)" />
        <input id="new-chat-members" placeholder="User IDs separated by commas" />
        <button onclick="createNewChat()">Create</button>
      `);
      window.createNewChat = async () => {
        const type = $('#new-chat-type').value;
        const name = $('#new-chat-name').value.trim();
        const membersStr = $('#new-chat-members').value;
        const members = membersStr ? membersStr.split(',').map(s => parseInt(s.trim())) : [];
        if (type !== 'direct' && !name) { toast('Please enter a name'); return; }
        await createChannel(type, name || 'Direct', members);
        toast('Channel created'); $('#global-modal').classList.add('hidden');
      };
    }

    // Channel Info Modal (manage active channel)
    function openChannelInfoModal() {
      if(!activeChannel) return;
      const ch = channels.find(c=>c.id===activeChannel);
      if(!ch) return;
      openModal(`
        <h2>${ch.name}</h2>
        <p>Type: ${ch.type}</p>
        <button onclick="leaveChannel('${ch.id}')">Leave</button>
        <button onclick="archiveChannel('${ch.id}')">Archive</button>
        <button onclick="pinChannel('${ch.id}')">${ch.starred?'Unstar':'Star'}</button>
        <hr style="margin:20px 0;border-color:#333;">
        <input id="add-member-id" placeholder="User ID to add" />
        <button onclick="addMember('${ch.id}', parseInt($('#add-member-id').value))">Add Member</button>
        <hr style="margin:20px 0;border-color:#333;">
        <select id="ttl-select">
          <option value="86400">24 hours</option>
          <option value="604800">7 days</option>
          <option value="7776000">90 days</option>
          <option value="0">Off</option>
        </select>
        <button onclick="setDisappearing('${ch.id}', parseInt($('#ttl-select').value))">Set Disappearing</button>
      `);
    }

    // Pinned Messages Modal
    async function openPinnedMessages() {
      if(!activeChannel) return;
      // For now, just show recent messages as pinned placeholder (backend pinning works, we can filter by pinned flag)
      const msgs = channelMessages.get(activeChannel)||[];
      const pinned = msgs.filter(m => m.pinned); // if we had pinned flag
      let html = (pinned.length ? pinned : msgs.slice(0,5)).map(m => `<div style="padding:8px;border-bottom:1px solid #333;cursor:pointer;" onclick="switchToMsg('${m.id}')">📌 ${escapeHtml(m.content.substring(0,50))}</div>`).join('');
      openModal(`<h2>Pinned Messages</h2>${html||'No pinned messages'}`);
      window.switchToMsg = (msgId) => { scrollToMessage(msgId); $('#global-modal').classList.add('hidden'); };
    }

    // Bookmarks Modal
    async function openBookmarksModal() {
      const bm = await getBookmarks();
      let html = bm.map(b => `<div style="padding:10px;border-bottom:1px solid #333;display:flex;justify-content:space-between;">
        <div onclick="jumpToBookmark('${b.message_id}')" style="cursor:pointer;flex:1;">
          <strong>${b.folder}</strong>: ${escapeHtml(b.content?.substring(0,60))} <span style="color:var(--text-secondary);font-size:12px;">${formatFull(b.created_at)}</span>
        </div>
        <button onclick="deleteBookmark(${b.id})" style="background:var(--red);border:none;color:white;border-radius:4px;padding:4px 8px;">✕</button>
      </div>`).join('');
      openModal(`<h2>Bookmarks</h2>${html||'No bookmarks yet'}`);
      window.jumpToBookmark = (msgId) => {
        const entry = bm.find(b=>b.message_id===msgId);
        if(entry?.channel_id) { switchChannel(entry.channel_id); setTimeout(() => scrollToMessage(msgId), 500); }
        $('#global-modal').classList.add('hidden');
      };
      window.deleteBookmark = async (id) => { /* call delete endpoint if available */ toast('Delete not yet implemented'); };
    }

    // Tasks Modal
    async function openTasksModal() {
      const tasks = await getTasks();
      let html = tasks.map(t => `<div style="padding:8px;border-bottom:1px solid #333;display:flex;justify-content:space-between;">
        <span onclick="toggleTaskCompletion(${t.id})" style="cursor:pointer;">${t.is_done?'✅':'⬜'} ${t.title} ${t.due_date?`(due: ${t.due_date})`:''}</span>
        <button onclick="deleteTask(${t.id})" style="background:var(--red);border:none;color:white;border-radius:4px;padding:4px 8px;">✕</button>
      </div>`).join('');
      openModal(`<h2>Tasks</h2>${html||'No tasks'}`);
      window.toggleTaskCompletion = async (id) => { /* need backend route */ toast('Toggle not yet implemented'); };
      window.deleteTask = async (id) => { /* need backend route */ toast('Delete not yet implemented'); };
    }

    // Statuses Modal
    async function openStatusesModal() {
      const statuses = await getStatuses();
      let html = statuses.map(s => `<div style="padding:8px;border-bottom:1px solid #333;"><strong>${s.username}</strong>: ${s.emoji} ${s.content}</div>`).join('');
      openModal(`<h2>Custom Statuses</h2>${html||'No statuses set'}
        <hr><input id="status-text" placeholder="Your status" /><input id="status-emoji" placeholder="Emoji" /><button onclick="setCustomStatus($('#status-text').value, $('#status-emoji').value)">Set</button>
      `);
    }

    // Admin Panel
    async function openAdminPanel() {
      if(currentUser.role !== 'admin') return;
      const chs = await getAdminChannels();
      let html = chs.map(ch => `<div style="padding:6px;border-bottom:1px solid #333;display:flex;justify-content:space-between;">
        ${ch.name} (${ch.type})
        <div>
          <button onclick="archiveChannel('${ch.id}')">Archive</button>
          <button onclick="forceJoin('${ch.id}')">Join</button>
        </div>
      </div>`).join('');
      openModal(`<h2>Admin Panel</h2>${html}`);
      window.forceJoin = async (chId) => { await apiFetch(`/channels/${chId}/members`,{method:'POST',body:JSON.stringify({user_id:currentUser.id})}); toast('Joined'); };
    }

    // Context Menu (right-click on message)
    window.showContextMenu = (e, msgId) => {
      e.preventDefault();
      const menu = $('#context-menu');
      menu.innerHTML = `
        <div class="context-menu-item" onclick="replyToMsg('${msgId}')">↩ Reply</div>
        <div class="context-menu-item" onclick="editMsg('${msgId}')">✎ Edit</div>
        <div class="context-menu-item" onclick="deleteMsg('${msgId}')">🗑 Delete</div>
        <div class="context-menu-item" onclick="copyMsg('${msgId}')">📋 Copy</div>
        <div class="context-menu-item" onclick="pinMsg('${msgId}')">📌 Pin</div>
        <div class="context-menu-item" onclick="reactMsg('${msgId}')">😀 React</div>
        <div class="context-menu-item" onclick="taskFromMsg('${msgId}')">✅ Task</div>
        <div class="context-menu-item" onclick="bookmarkMsg('${msgId}')">⭐ Bookmark</div>
        <div class="context-menu-item" onclick="tagMsg('${msgId}')">🏷 Tag</div>
      `;
      menu.style.display = 'block';
      menu.style.left = Math.min(e.clientX, window.innerWidth-200) + 'px';
      menu.style.top = Math.min(e.clientY, window.innerHeight-250) + 'px';
      document.addEventListener('click', () => menu.style.display = 'none', {once:true});
    };
    window.replyToMsg = (id) => { replyToMessageId = id; toast('Reply mode'); };
    window.editMsg = async (id) => {
      const msgs = channelMessages.get(activeChannel);
      const msg = msgs.find(m=>m.id===id);
      if(msg) { const newText = prompt('Edit message:', msg.content); if(newText) await editMessage(id, newText); }
    };
    window.deleteMsg = async (id) => { await deleteMessage(id); };
    window.copyMsg = async (id) => {
      const msgs = channelMessages.get(activeChannel);
      const msg = msgs.find(m=>m.id===id);
      if(msg) { await navigator.clipboard.writeText(msg.content); toast('Copied'); }
    };
    window.pinMsg = async (id) => { await pinMessage(id); toast('Pinned'); };
    window.reactMsg = async (id) => { const emoji = prompt('Enter emoji:'); if(emoji) await reactToMessage(id, emoji); };
    window.taskFromMsg = async (id) => { const title = prompt('Task title:'); if(title) await createTask(id, title); toast('Task created'); };
    window.bookmarkMsg = async (id) => { const folder = prompt('Folder (default General):')||'General'; await addBookmark(id, folder); toast('Bookmarked'); };
    window.tagMsg = async (id) => { const tag = prompt('Tag name:'); if(tag) await apiFetch(`/messages/${id}/tag`,{method:'POST',body:JSON.stringify({tag})}); toast('Tagged'); };

    function toggleTheme() {
      const html = document.documentElement;
      const current = html.getAttribute('data-theme');
      html.setAttribute('data-theme', current === 'dark' ? 'light' : 'dark');
    }
    function scrollToBottom(smooth) {
      const el = $('#chat-messages');
      el.scrollTo({ top: el.scrollHeight, behavior: smooth?'smooth':'auto' });
      scrollAtBottom = true;
    }
    function scrollToMessage(msgId) {
      const el = document.querySelector(`[data-id="${msgId}"]`);
      if(el) el.scrollIntoView({ behavior:'smooth', block:'center' });
    }
    async function globalSearch(query) {
      if(!query) return;
      const results = await apiFetch(`/search?q=${encodeURIComponent(query)}`);
      let html = results.map(r => `<div style="padding:6px;border-bottom:1px solid #333;">${r.username}: ${escapeHtml(r.content.substring(0,60))}</div>`).join('');
      openModal(`<h2>Search Results</h2>${html||'None'}`);
    }

    // Initialization
    async function initApp() {
      await checkAuth();
      if(!currentUser) renderAuthScreen();
      else {
        renderMainUI();
        await loadChannels();
        initPusher();
      }
    }
    window.onload = initApp;
  </script>
</body>
</html>
"""

# ---------- Root route ----------
@app.get("/", response_class=HTMLResponse)
async def root():
    return HTMLResponse(content=FRONTEND_HTML)

# ---------- Health check ----------
@app.get("/api/health")
async def health():
    try:
        await db_execute("SELECT 1")
        return {"status": "ok"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})

# ---------- Auth Routes ----------
@app.post("/api/register")
async def register(data: dict):
    username = data.get("username", "").strip()
    email = data.get("email", "").strip()
    password = data.get("password", "")
    logger.info(f"Register: {username}")
    await db_run("INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)", [username, email, password])
    return {"ok": True}

@app.post("/api/login")
async def login(data: dict, response: Response):
    username = data.get("username", "").strip()
    password = data.get("password", "")
    rows = await db_execute("SELECT * FROM users WHERE username = ?", [username])
    if not rows: raise HTTPException(401, detail="User not found")
    user = rows[0]
    if user["password_hash"] != password: raise HTTPException(401, detail="Invalid credentials")
    token = jwt.encode({"user_id": user["id"], "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7)}, JWT_SECRET)
    response.set_cookie("token", token, httponly=True, secure=True, samesite="lax")
    sess_id = str(uuid.uuid4())
    await db_run("INSERT INTO sessions (id, user_id, ip, browser) VALUES (?, ?, ?, ?)", [sess_id, user["id"], "unknown", "unknown"])
    return {"ok": True}

@app.get("/api/me")
async def me(user=Depends(get_current_user)):
    return user

@app.post("/api/logout")
async def logout(response: Response, user=Depends(get_current_user)):
    response.delete_cookie("token")
    return {"ok": True}

@app.put("/api/profile")
async def update_profile(data: dict, user=Depends(get_current_user)):
    await db_run("UPDATE users SET bio=?, phone=?, avatar_url=?, status_preset=? WHERE id=?", [data.get("bio", user["bio"]), data.get("phone", user["phone"]), data.get("avatar_url", user["avatar_url"]), data.get("status_preset", user["status_preset"]), user["id"]])
    return {"ok": True}

@app.post("/api/change-password")
async def change_password(data: dict, user=Depends(get_current_user)):
    if data["old_password"] != user["password_hash"]: raise HTTPException(400, "Wrong password")
    await db_run("UPDATE users SET password_hash=? WHERE id=?", [data["new_password"], user["id"]])
    return {"ok": True}

@app.post("/api/block/{user_id}")
async def block_user(user_id: int, user=Depends(get_current_user)):
    await db_run("INSERT OR IGNORE INTO blocked_users (blocker_id, blocked_id) VALUES (?, ?)", [user["id"], user_id])
    return {"ok": True}

@app.delete("/api/block/{user_id}")
async def unblock_user(user_id: int, user=Depends(get_current_user)):
    await db_run("DELETE FROM blocked_users WHERE blocker_id=? AND blocked_id=?", [user["id"], user_id])
    return {"ok": True}

# ---------- Channels ----------
@app.get("/api/channels")
async def get_channels(user=Depends(get_current_user)):
    rows = await db_execute("""
        SELECT c.*, cm.unread_count, cm.starred,
            (SELECT content FROM messages WHERE channel_id=c.id AND is_deleted=0 ORDER BY created_at DESC LIMIT 1) as last_msg,
            (SELECT created_at FROM messages WHERE channel_id=c.id AND is_deleted=0 ORDER BY created_at DESC LIMIT 1) as last_msg_time
        FROM channels c JOIN channel_members cm ON c.id=cm.channel_id
        WHERE cm.user_id=? AND c.is_archived=0 ORDER BY last_msg DESC
    """, [user["id"]])
    for r in rows:
        if r["type"] == "direct":
            other = await db_execute("SELECT u.username, u.avatar_url FROM channel_members cm JOIN users u ON cm.user_id=u.id WHERE cm.channel_id=? AND cm.user_id!=? LIMIT 1", [r["id"], user["id"]])
            if other: r["name"] = other[0]["username"]; r["avatar_url"] = other[0]["avatar_url"]
        if r.get("last_msg_time"): r["last_msg_time"] = str(r["last_msg_time"])
    return rows

@app.post("/api/channels")
async def create_channel(data: dict, user=Depends(get_current_user)):
    ch_id = str(uuid.uuid4())
    await db_run("INSERT INTO channels (id, name, type, created_by) VALUES (?, ?, ?, ?)", [ch_id, data["name"], data["type"], user["id"]])
    await db_run("INSERT INTO channel_members (channel_id, user_id, role) VALUES (?, ?, 'admin')", [ch_id, user["id"]])
    for m in data.get("members", []): await db_run("INSERT INTO channel_members (channel_id, user_id) VALUES (?, ?)", [ch_id, m])
    return {"id": ch_id}

@app.post("/api/channels/{ch_id}/leave")
async def leave_channel(ch_id: str, user=Depends(get_current_user)):
    await db_run("DELETE FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/archive")
async def archive_channel(ch_id: str, user=Depends(get_current_user)):
    await db_run("UPDATE channels SET is_archived=1 WHERE id=?", [ch_id])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/pin")
async def pin_channel(ch_id: str, user=Depends(get_current_user)):
    await db_run("UPDATE channel_members SET starred=1 WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/members")
async def add_member(ch_id: str, data: dict, user=Depends(get_current_user)):
    await db_run("INSERT INTO channel_members (channel_id, user_id) VALUES (?, ?)", [ch_id, data["user_id"]])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/disappearing")
async def set_disappearing(ch_id: str, data: dict, user=Depends(get_current_user)):
    await db_run("UPDATE messages SET disappearing_ttl=? WHERE channel_id=? AND created_at > ?", [data["ttl"], ch_id, datetime.datetime.utcnow()])
    return {"ok": True}

# ---------- Messages ----------
@app.get("/api/channels/{ch_id}/messages")
async def get_messages(ch_id: str, before: str = None, user=Depends(get_current_user)):
    await check_membership(ch_id, user["id"])
    params = [ch_id]
    sql = "SELECT m.*, u.username, u.avatar_url FROM messages m JOIN users u ON m.user_id=u.id WHERE m.channel_id=? AND m.is_deleted=0"
    if before: sql += " AND m.created_at < ?"; params.append(before)
    sql += " ORDER BY m.created_at DESC LIMIT 50"
    rows = await db_execute(sql, params)
    messages = []
    for r in reversed(rows):
        msg = serialize_message(r)
        reacts = await db_execute("SELECT r.emoji, r.user_id, u.username FROM reactions r JOIN users u ON r.user_id=u.id WHERE r.message_id=?", [msg["id"]])
        msg["reactions"] = group_reactions(reacts)
        receipts = await db_execute("SELECT user_id FROM read_receipts WHERE message_id=?", [msg["id"]])
        msg["read_status"] = "read" if any(rr["user_id"] == user["id"] for rr in receipts) else ("delivered" if receipts else "sent")
        messages.append(msg)
    return messages

@app.post("/api/channels/{ch_id}/messages")
async def send_message(ch_id: str, content: str = Form(None), type: str = Form("text"), file: UploadFile = File(None), reply_to: str = Form(None), thread_parent: str = Form(None), user=Depends(get_current_user)):
    await check_membership(ch_id, user["id"])
    msg_id = str(uuid.uuid4())
    file_url = content or ""
    if file:
        file_content = await file.read()
        file_url = f"data:{file.content_type};base64,{base64.b64encode(file_content).decode()}"
    await db_run("INSERT INTO messages (id, channel_id, user_id, content, type, reply_to, thread_parent) VALUES (?, ?, ?, ?, ?, ?, ?)", [msg_id, ch_id, user["id"], file_url, type, reply_to, thread_parent])
    msg_rows = await db_execute("SELECT m.*, u.username, u.avatar_url FROM messages m JOIN users u ON m.user_id=u.id WHERE m.id=?", [msg_id])
    msg = serialize_message(msg_rows[0])
    pusher_client.trigger(f'private-channel-{ch_id}', 'new-message', {'message': msg})
    members = await db_execute("SELECT user_id FROM channel_members WHERE channel_id=? AND user_id!=?", [ch_id, user["id"]])
    for m in members: await db_run("UPDATE channel_members SET unread_count = unread_count + 1 WHERE channel_id=? AND user_id=?", [ch_id, m["user_id"]])
    return {"id": msg_id}

@app.put("/api/messages/{msg_id}")
async def edit_message(msg_id: str, data: dict, user=Depends(get_current_user)):
    msg = await db_execute("SELECT * FROM messages WHERE id=? AND user_id=?", [msg_id, user["id"]])
    if not msg: raise HTTPException(403)
    await db_run("UPDATE messages SET content=?, is_edited=1, edited_at=CURRENT_TIMESTAMP WHERE id=?", [data["content"], msg_id])
    updated = await db_execute("SELECT * FROM messages WHERE id=?", [msg_id])
    pusher_client.trigger(f'private-channel-{msg[0]["channel_id"]}', 'message-updated', {'message': serialize_message(updated[0])})
    return {"ok": True}

@app.delete("/api/messages/{msg_id}")
async def delete_message(msg_id: str, user=Depends(get_current_user)):
    msg = await db_execute("SELECT * FROM messages WHERE id=?", [msg_id])
    if not msg: raise HTTPException(404)
    ch_id = msg[0]["channel_id"]
    mem = await db_execute("SELECT role FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    if not mem or (msg[0]["user_id"] != user["id"] and mem[0]["role"] not in ("admin","moderator")): raise HTTPException(403)
    await db_run("UPDATE messages SET is_deleted=1 WHERE id=?", [msg_id])
    pusher_client.trigger(f'private-channel-{ch_id}', 'message-deleted', {'message_id': msg_id})
    return {"ok": True}

# ---------- Reactions ----------
@app.post("/api/messages/{msg_id}/react")
async def react_to_message(msg_id: str, data: dict, user=Depends(get_current_user)):
    await db_run("INSERT OR REPLACE INTO reactions (message_id, user_id, emoji) VALUES (?, ?, ?)", [msg_id, user["id"], data["emoji"]])
    msg = await db_execute("SELECT channel_id FROM messages WHERE id=?", [msg_id])
    if msg:
        reacts = await db_execute("SELECT r.emoji, r.user_id, u.username FROM reactions r JOIN users u ON r.user_id=u.id WHERE r.message_id=?", [msg_id])
        pusher_client.trigger(f'private-channel-{msg[0]["channel_id"]}', 'reaction-updated', {'message_id': msg_id, 'reactions': group_reactions(reacts)})
    return {"ok": True}

# ---------- Typing ----------
@app.post("/api/channels/{ch_id}/typing")
async def typing_indicator(ch_id: str, user=Depends(get_current_user)):
    await db_run("INSERT OR REPLACE INTO typing_status (channel_id, user_id, started_at) VALUES (?, ?, CURRENT_TIMESTAMP)", [ch_id, user["id"]])
    pusher_client.trigger(f'private-channel-{ch_id}', 'typing', {'user_id': user["id"], 'username': user["username"]})
    return {"ok": True}

# ---------- Read Receipts ----------
@app.post("/api/channels/{ch_id}/read")
async def mark_read(ch_id: str, user=Depends(get_current_user)):
    msgs = await db_execute("SELECT id FROM messages WHERE channel_id=? AND is_deleted=0 AND id NOT IN (SELECT message_id FROM read_receipts WHERE user_id=?)", [ch_id, user["id"]])
    if msgs:
        for m in msgs: await db_run("INSERT OR IGNORE INTO read_receipts (message_id, user_id) VALUES (?, ?)", [m["id"], user["id"]])
        pusher_client.trigger(f'private-channel-{ch_id}', 'read-receipt', {'message_ids': [m["id"] for m in msgs], 'user_id': user["id"]})
    await db_run("UPDATE channel_members SET unread_count=0 WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return {"ok": True}

# ---------- Pins ----------
@app.post("/api/messages/{msg_id}/pin")
async def pin_message(msg_id: str, user=Depends(get_current_user)):
    msg = await db_execute("SELECT channel_id FROM messages WHERE id=?", [msg_id])
    if msg:
        await db_run("INSERT OR IGNORE INTO message_pins (channel_id, message_id, pinned_by) VALUES (?, ?, ?)", [msg[0]["channel_id"], msg_id, user["id"]])
        pusher_client.trigger(f'private-channel-{msg[0]["channel_id"]}', 'message-pinned', {'message_id': msg_id})
    return {"ok": True}

# ---------- Tasks ----------
@app.post("/api/tasks")
async def create_task(data: dict, user=Depends(get_current_user)):
    await db_run("INSERT INTO tasks (user_id, message_id, title) VALUES (?, ?, ?)", [user["id"], data.get("message_id"), data["title"]])
    return {"ok": True}

@app.get("/api/tasks")
async def get_tasks(user=Depends(get_current_user)):
    return await db_execute("SELECT * FROM tasks WHERE user_id=? OR assigned_to=?", [user["id"], user["id"]])

# ---------- Bookmarks ----------
@app.post("/api/bookmarks")
async def add_bookmark(data: dict, user=Depends(get_current_user)):
    await db_run("INSERT INTO bookmarks (user_id, message_id, folder) VALUES (?, ?, ?)", [user["id"], data["message_id"], data.get("folder", "General")])
    return {"ok": True}

@app.get("/api/bookmarks")
async def get_bookmarks(user=Depends(get_current_user)):
    return await db_execute("SELECT b.*, m.content, m.channel_id FROM bookmarks b JOIN messages m ON b.message_id=m.id WHERE b.user_id=?", [user["id"]])

# ---------- Tags ----------
@app.post("/api/messages/{msg_id}/tag")
async def add_tag(msg_id: str, data: dict, user=Depends(get_current_user)):
    tag_name = data["tag"]
    tag = await db_execute("SELECT id FROM tags WHERE name=?", [tag_name])
    if not tag: await db_run("INSERT INTO tags (name) VALUES (?)", [tag_name]); tag = await db_execute("SELECT id FROM tags WHERE name=?", [tag_name])
    await db_run("INSERT OR IGNORE INTO message_tags (message_id, tag_id) VALUES (?, ?)", [msg_id, tag[0]["id"]])
    return {"ok": True}

# ---------- Drafts ----------
@app.post("/api/channels/{ch_id}/draft")
async def save_draft(ch_id: str, data: dict, user=Depends(get_current_user)):
    await db_run("INSERT OR REPLACE INTO drafts (channel_id, user_id, content, updated_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP)", [ch_id, user["id"], data["content"]])
    return {"ok": True}

@app.get("/api/channels/{ch_id}/draft")
async def get_draft(ch_id: str, user=Depends(get_current_user)):
    d = await db_execute("SELECT content FROM drafts WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return d[0] if d else {"content": ""}

# ---------- Custom Statuses ----------
@app.post("/api/status")
async def set_status(data: dict, user=Depends(get_current_user)):
    await db_run("INSERT INTO custom_statuses (user_id, type, content, emoji) VALUES (?, ?, ?, ?)", [user["id"], data.get("type","text"), data.get("content"), data.get("emoji","")])
    return {"ok": True}

@app.get("/api/statuses")
async def get_statuses():
    return await db_execute("SELECT cs.*, u.username FROM custom_statuses cs JOIN users u ON cs.user_id=u.id ORDER BY cs.created_at DESC")

# ---------- Search ----------
@app.get("/api/search")
async def global_search(q: str):
    return await db_execute("SELECT m.*, u.username FROM messages m JOIN users u ON m.user_id=u.id WHERE m.content LIKE ? AND m.is_deleted=0 LIMIT 20", [f"%{q}%"])

@app.get("/api/channels/{ch_id}/search")
async def channel_search(ch_id: str, q: str):
    return await db_execute("SELECT m.*, u.username FROM messages m JOIN users u ON m.user_id=u.id WHERE m.channel_id=? AND m.content LIKE ? AND m.is_deleted=0 LIMIT 20", [ch_id, f"%{q}%"])

# ---------- Admin ----------
@app.get("/api/admin/channels")
async def admin_channels(user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(403)
    return await db_execute("SELECT * FROM channels")

# ---------- Pusher Auth ----------
@app.post("/api/pusher/auth")
async def pusher_auth(request: Request, user=Depends(get_current_user)):
    form = await request.form()
    channel_name = form["channel_name"]
    socket_id = form["socket_id"]
    if not channel_name.startswith("private-channel-"): raise HTTPException(403)
    ch_id = channel_name.split("-", 2)[2]
    await check_membership(ch_id, user["id"])
    auth = pusher_client.authenticate(channel=channel_name, socket_id=socket_id, custom_data={"user_id": str(user["id"])})
    return auth

# ---------- Interactive Buttons ----------
@app.post("/api/interactive/{btn_id}/respond")
async def respond_button(btn_id: int, user=Depends(get_current_user)):
    await db_run("INSERT OR IGNORE INTO interactive_responses (button_id, user_id) VALUES (?, ?)", [btn_id, user["id"]])
    return {"ok": True}
