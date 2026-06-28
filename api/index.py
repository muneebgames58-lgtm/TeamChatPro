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

# ---------- Turso HTTP Helpers (FIXED) ----------
def _infer_type(value):
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "real"
    if value is None:
        return "null"
    return "text"

async def turso_request(sql: str, params: list = None):
    """Execute SQL via Turso HTTP pipeline API – handles actual response format."""
    url = f"{TURSO_URL}/v2/pipeline"
    headers = {
        "Authorization": f"Bearer {TURSO_TOKEN}",
        "Content-Type": "application/json"
    }

    args_list = []
    if params:
        for p in params:
            arg_type = _infer_type(p)
            if arg_type == "null":
                arg_value = None
            elif arg_type == "integer":
                arg_value = int(p)
            elif arg_type == "real":
                arg_value = float(p)
            else:
                arg_value = str(p)
            args_list.append({"type": arg_type, "value": arg_value})

    body = {
        "requests": [
            {
                "type": "execute",
                "stmt": {
                    "sql": sql,
                    "args": args_list
                }
            },
            {"type": "close"}
        ]
    }

    logger.info(f"Turso request: {sql[:120]}...")
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(url, headers=headers, json=body, timeout=30.0)
            data = resp.json()

            if resp.status_code != 200:
                logger.error(f"Turso error: {data}")
                raise HTTPException(500, f"Database error: {resp.status_code} {data}")

            # Log the response structure (first 500 chars)
            logger.info(f"Turso response type: {data.get('results',[{}])[0].get('type','unknown')}")

            results = data.get("results", [])
            if not results:
                return {"rows": [], "rows_written": 0, "rows_read": 0}

            # The Turso pipeline response format (actual) is:
            # results[0] = {"type": "ok", "response": {"type": "execute", "result": {...}}}
            # Or sometimes just {"type": "execute", ...} (legacy)
            first = results[0]
            if first.get("type") == "ok":
                # New format: nested response
                response_data = first.get("response", {})
                if response_data.get("type") == "error":
                    logger.error(f"SQL error: {response_data.get('error')}")
                    raise HTTPException(500, f"SQL error: {response_data.get('error')}")
                result = response_data.get("result", {})
            elif first.get("type") == "execute":
                # Legacy format: result directly in first
                result = first.get("result", {})
            else:
                logger.warning(f"Unknown Turso response type: {first.get('type')}")
                return {"rows": [], "rows_written": 0, "rows_read": 0}

            # Extract columns and rows
            cols = [c["name"] for c in result.get("cols", [])]
            rows_raw = result.get("rows", [])
            rows = []
            for row in rows_raw:
                vals = []
                for v in row:
                    if isinstance(v, dict):
                        vals.append(v.get("value"))
                    else:
                        vals.append(v)
                rows.append(dict(zip(cols, vals)))

            return {
                "rows": rows,
                "rows_written": result.get("rows_written", 0),
                "rows_read": result.get("rows_read", 0),
                "rows_affected": result.get("rows_affected", 0)   # legacy, keep for compatibility
            }
        except httpx.RequestError as e:
            logger.error(f"Turso connection failed: {e}")
            raise HTTPException(500, f"Database connection failed: {str(e)}")
async def db_execute(sql: str, params: list = None) -> list:
    res = await turso_request(sql, params)
    return res.get("rows", [])

async def db_run(sql: str, params: list = None) -> dict:
    """Run INSERT/UPDATE/DELETE and return the full result dict."""
    return await turso_request(sql, params)

# ---------- Auth Dependency ----------
async def get_current_user(request: Request):
    token = request.cookies.get("token")
    if not token:
        raise HTTPException(401)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        user_id = payload["user_id"]
        rows = await db_execute("SELECT * FROM users WHERE id = ?", [user_id])
        if not rows:
            raise HTTPException(401)
        return rows[0]
    except JWTError:
        raise HTTPException(401)

async def check_membership(ch_id, user_id):
    rows = await db_execute("SELECT 1 FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user_id])
    if not rows:
        raise HTTPException(403)

def serialize_message(row):
    row["created_at"] = str(row["created_at"])
    return row

def group_reactions(rows):
    groups = {}
    for r in rows:
        emoji = r["emoji"]
        if emoji not in groups:
            groups[emoji] = {"emoji": emoji, "count": 0, "users": []}
        groups[emoji]["count"] += 1
        groups[emoji]["users"].append({"user_id": r["user_id"], "username": r.get("username", "")})
    return list(groups.values())

# ---------- Embedded Frontend HTML (FULL FEATURED) ----------
# (All CSS and JS are included inline; no external files needed)
FRONTEND_HTML = """
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
    /* ---- Full CSS (your existing + modal extras) ---- */
    :root { --bg-main: #111b21; --bg-panel: #202c33; --bg-chat: #0b141a; --outgoing-bubble: #005c4b; --incoming-bubble: #202c33; --accent: #00a884; --red: #f15c6d; --text-primary: #e9edef; --text-secondary: #8696a0; --radius-bubble: 8px; --radius-modal: 12px; --font-family: 'Inter', sans-serif; }
    [data-theme="light"] { --bg-main: #f0f2f5; --bg-panel: #ffffff; --bg-chat: #efeae2; --outgoing-bubble: #d9fdd3; --incoming-bubble: #ffffff; --accent: #008069; --text-primary: #111b21; --text-secondary: #667781; }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: var(--font-family); background: var(--bg-main); color: var(--text-primary); height: 100vh; overflow: hidden; }
    #app { display: flex; height: 100vh; }
    .sidebar { width: 35%; min-width: 320px; max-width: 500px; background: var(--bg-panel); display: flex; flex-direction: column; border-right: 1px solid rgba(0,0,0,0.2); }
    .sidebar-header { display: flex; align-items: center; padding: 10px 16px; gap: 8px; }
    .sidebar-header .avatar { width: 40px; height: 40px; border-radius: 50%; cursor: pointer; }
    .search-box { padding: 0 12px 8px; position: relative; }
    .search-box input { width: 100%; padding: 8px 12px 8px 36px; border-radius: 8px; border: none; background: var(--bg-main); color: var(--text-primary); font-size: 14px; }
    .channel-list { overflow-y: auto; flex: 1; }
    .channel-item { display: flex; align-items: center; padding: 10px 16px; cursor: pointer; gap: 10px; border-bottom: 1px solid rgba(255,255,255,0.05); }
    .channel-item.active { background: rgba(0,168,132,0.15); }
    .channel-item .avatar, .channel-item .group-avatar { width: 44px; height: 44px; border-radius: 50%; }
    .channel-info { flex: 1; overflow: hidden; }
    .channel-name { font-weight: 500; }
    .channel-last-msg { font-size: 13px; color: var(--text-secondary); }
    .unread-badge { background: var(--accent); color: white; border-radius: 50%; min-width: 20px; height: 20px; display: flex; align-items: center; justify-content: center; font-size: 12px; padding: 0 4px; }
    .chat-area { flex: 1; display: flex; flex-direction: column; background: var(--bg-chat); position: relative; }
    .chat-header { display: flex; align-items: center; padding: 10px 16px; background: var(--bg-panel); gap: 10px; }
    .channel-title { flex: 1; font-weight: 600; }
    .chat-messages { flex: 1; overflow-y: auto; padding: 20px; background-image: url('data:image/svg+xml;utf8,<svg ...></svg>'); }
    .message-row { display: flex; max-width: 75%; margin-bottom: 2px; }
    .outgoing { align-self: flex-end; }
    .incoming { align-self: flex-start; }
    .message-bubble { padding: 8px 12px; border-radius: var(--radius-bubble); font-size: 14px; }
    .outgoing .message-bubble { background: var(--outgoing-bubble); }
    .incoming .message-bubble { background: var(--incoming-bubble); }
    .chat-input { padding: 10px 16px; background: var(--bg-panel); display: flex; gap: 8px; }
    .chat-input textarea { flex: 1; resize: none; border-radius: 20px; border: none; padding: 8px 16px; background: var(--bg-main); color: var(--text-primary); }
    .send-btn { background: var(--accent); border: none; color: white; border-radius: 50%; width: 42px; height: 42px; cursor: pointer; }
    .modal-overlay { position: fixed; top:0; left:0; right:0; bottom:0; background: rgba(0,0,0,0.7); display: flex; align-items: center; justify-content: center; z-index: 1000; }
    .modal { background: var(--bg-panel); border-radius: var(--radius-modal); max-width: 600px; width: 90%; max-height: 85vh; overflow-y: auto; padding: 24px; }
    .modal h2 { margin-bottom: 16px; }
    .modal input, .modal textarea, .modal select { width: 100%; padding: 10px; margin-bottom: 12px; background: var(--bg-main); border: 1px solid var(--text-secondary); border-radius: 6px; color: var(--text-primary); }
    .modal button { padding: 10px 20px; background: var(--accent); border: none; color: white; border-radius: 6px; cursor: pointer; }
    .context-menu { position: fixed; background: var(--bg-panel); border-radius: 8px; z-index: 2000; padding: 4px 0; min-width: 180px; display: none; }
    .context-menu-item { padding: 8px 16px; cursor: pointer; font-size: 14px; display: flex; align-items: center; gap: 8px; }
    .context-menu-item:hover { background: rgba(255,255,255,0.1); }
    .toast-container { position: fixed; bottom: 20px; left: 50%; transform: translateX(-50%); z-index: 3000; }
    .toast { background: var(--accent); color: white; padding: 10px 24px; border-radius: 24px; margin-top: 8px; animation: slideUp 0.3s ease; }
    .hidden { display: none !important; }
    @keyframes slideUp { from { opacity:0; transform:translateY(10px); } to { opacity:1; transform:translateY(0); } }
    @media (max-width: 768px) { .sidebar { width:100% !important; max-width:100% !important; } .chat-area { display: none; } .chat-area.mobile-open { display: flex; } }
  </style>
</head>
<body>
  <div id="app"></div>
  <div class="context-menu" id="context-menu"></div>
  <div class="toast-container"></div>
  <div class="modal-overlay hidden" id="global-modal"></div>
  <script src="https://js.pusher.com/8.2.0/pusher.min.js"></script>
  <script>
    // ---------- GLOBALS ----------
    const API = '/api';
    let currentUser = null, activeChannel = null, channels = [], channelMessages = new Map();
    let socket = null, replyToMessageId = null, scrollAtBottom = true;

    const $ = s => document.querySelector(s);
    const toast = msg => {
      let c = $('.toast-container'); if(!c) { c=document.createElement('div'); c.className='toast-container'; document.body.appendChild(c); }
      const el = document.createElement('div'); el.className='toast'; el.textContent=msg; c.appendChild(el);
      setTimeout(()=>el.remove(),3000);
    };
    const formatDate = d => new Date(d).toLocaleDateString('en-US',{month:'short',day:'numeric',year:'numeric'});
    const formatTime = d => new Date(d).toLocaleTimeString('en-US',{hour:'2-digit',minute:'2-digit'});

    // ---------- API ----------
    async function apiFetch(url, opts={}) {
      const headers = { ...opts.headers };
      if (!(opts.body instanceof FormData)) headers['Content-Type'] = 'application/json';
      const res = await fetch(API+url, { credentials:'include', headers, ...opts });
      if (!res.ok) { const err = await res.json().catch(()=>({detail:'Error'})); throw new Error(err.detail); }
      return res.json();
    }

    // ---------- AUTH ----------
    async function checkAuth() { try { currentUser = await apiFetch('/me'); } catch { currentUser = null; } }
    async function login(u, p) { await apiFetch('/login',{method:'POST',body:JSON.stringify({username:u.trim(),password:p})}); await checkAuth(); }
    async function register(u, e, p) { await apiFetch('/register',{method:'POST',body:JSON.stringify({username:u.trim(),email:e.trim(),password:p})}); await checkAuth(); }
    async function logout() { await apiFetch('/logout',{method:'POST'}); currentUser=null; activeChannel=null; channels=[]; if(socket)socket.disconnect(); }

    // ---------- CHANNELS ----------
    async function loadChannels() { channels = await apiFetch('/channels'); renderSidebar(); }
    async function createChannel(type,name,members=[]) { await apiFetch('/channels',{method:'POST',body:JSON.stringify({type,name,members})}); await loadChannels(); }
    async function leaveChannel(id) { await apiFetch(`/channels/${id}/leave`,{method:'POST'}); if(activeChannel===id) activeChannel=null; await loadChannels(); }
    async function archiveChannel(id) { await apiFetch(`/channels/${id}/archive`,{method:'POST'}); await loadChannels(); }
    async function pinChannel(id) { await apiFetch(`/channels/${id}/pin`,{method:'POST'}); await loadChannels(); }
    async function addMember(chId, userId) { await apiFetch(`/channels/${chId}/members`,{method:'POST',body:JSON.stringify({user_id:userId})}); }
    async function setDisappearing(chId, ttl) { await apiFetch(`/channels/${chId}/disappearing`,{method:'POST',body:JSON.stringify({ttl})}); }

    // ---------- MESSAGES ----------
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

    // ---------- USER PROFILE & ACTIONS ----------
    async function updateProfile(data) { await apiFetch('/profile',{method:'PUT',body:JSON.stringify(data)}); currentUser = await apiFetch('/me'); }
    async function changePassword(oldPwd, newPwd) { await apiFetch('/change-password',{method:'POST',body:JSON.stringify({old_password:oldPwd,new_password:newPwd})}); }
    async function blockUser(userId) { await apiFetch(`/block/${userId}`,{method:'POST'}); }
    async function unblockUser(userId) { await apiFetch(`/block/${userId}`,{method:'DELETE'}); }

    // ---------- BOOKMARKS ----------
    async function addBookmark(msgId, folder='General') { await apiFetch('/bookmarks',{method:'POST',body:JSON.stringify({message_id:msgId,folder})}); }
    async function getBookmarks() { return await apiFetch('/bookmarks'); }

    // ---------- TASKS ----------
    async function createTask(msgId, title) { await apiFetch('/tasks',{method:'POST',body:JSON.stringify({message_id:msgId,title})}); }
    async function getTasks() { return await apiFetch('/tasks'); }

    // ---------- DRAFTS ----------
    async function saveDraft(chId, content) { await apiFetch(`/channels/${chId}/draft`,{method:'POST',body:JSON.stringify({content})}); }
    async function getDraft(chId) { const d = await apiFetch(`/channels/${chId}/draft`); return d.content || ''; }

    // ---------- CUSTOM STATUS ----------
    async function setCustomStatus(content, emoji='', type='text') { await apiFetch('/status',{method:'POST',body:JSON.stringify({type,content,emoji})}); }
    async function getStatuses() { return await apiFetch('/statuses'); }

    // ---------- ADMIN ----------
    async function getAdminChannels() { return await apiFetch('/admin/channels'); }

    // ---------- PUSHER ----------
    function initPusher() {
      if(socket) socket.disconnect();
      socket = new Pusher("""+PUSHER_KEY+""", { cluster: """+PUSHER_CLUSTER+""", authEndpoint: API+'/pusher/auth', encrypted: true });
      socket.connection.bind('connected', ()=>{ if(activeChannel) subscribeToChannel(activeChannel); });
    }
    function subscribeToChannel(chId) {
      const ch = socket.subscribe('private-channel-'+chId);
      ch.bind('new-message', data => { const msgs = channelMessages.get(chId)||[]; msgs.push(data.message); channelMessages.set(chId, msgs); if(activeChannel===chId) renderMessages(); });
      ch.bind('message-updated', data => { const msgs = channelMessages.get(chId); if(msgs){ const idx = msgs.findIndex(m=>m.id===data.message.id); if(idx>-1) msgs[idx] = {...msgs[idx], ...data.message}; if(activeChannel===chId) renderMessages(); } });
      ch.bind('message-deleted', data => { const msgs = channelMessages.get(chId); if(msgs){ const m = msgs.find(m=>m.id===data.message_id); if(m) m.is_deleted=true; if(activeChannel===chId) renderMessages(); } });
      ch.bind('typing', data => { /* typing indicator logic */ });
    }

    // ---------- RENDERING ----------
    function renderAuthScreen() {
      $('#app').innerHTML = `<div class="modal-overlay" style="display:flex;"><div class="modal">
        <h2>Welcome</h2>
        <input id="auth-username" placeholder="Username" />
        <input id="auth-email" placeholder="Email (register)" />
        <input id="auth-password" type="password" placeholder="Password" />
        <button id="btn-login">Login</button> <button id="btn-register">Register</button>
      </div></div>`;
      $('#btn-login').onclick = async () => { try { await login($('#auth-username').value, $('#auth-password').value); initApp(); } catch(e){ toast(e.message); } };
      $('#btn-register').onclick = async () => { try { await register($('#auth-username').value, $('#auth-email').value, $('#auth-password').value); initApp(); } catch(e){ toast(e.message); } };
    }

    function renderMainUI() {
      $('#app').innerHTML = `
        <div class="sidebar">
          <div class="sidebar-header">
            <img src="${currentUser.avatar_url}" class="avatar" onclick="openProfileModal()" />
            <div style="flex:1"><div style="font-weight:600">${currentUser.username}</div></div>
            <button id="btn-logout">⏻</button>
          </div>
          <div class="search-box"><input id="global-search" placeholder="Search (Ctrl+K)" /></div>
          <div class="channel-list" id="channel-list"></div>
        </div>
        <div class="chat-area" id="chat-area">
          <div class="chat-header">
            <div class="channel-title" id="channel-title">Select a channel</div>
            <button onclick="openChannelInfoModal()">⚙</button>
            <button onclick="openPinnedMessages()">📌</button>
            <button onclick="openBookmarksModal()">⭐</button>
            <button onclick="openTasksModal()">✅</button>
            <button onclick="openStatusesModal()">📊</button>
            <button onclick="openAdminPanel()" id="btn-admin" style="display:${currentUser.role==='admin'?'':'none'}">🔧</button>
          </div>
          <div class="chat-messages" id="chat-messages"></div>
          <div class="chat-input">
            <textarea id="message-input" placeholder="Type a message..." rows="1"></textarea>
            <button id="btn-send" class="send-btn">➤</button>
          </div>
        </div>`;
      renderSidebar();
      document.getElementById('btn-logout').onclick = async () => { await logout(); initApp(); };
      document.getElementById('btn-send').onclick = () => { const inp = document.getElementById('message-input'); if(inp.value.trim()) { sendMessage(inp.value); inp.value=''; } };
      document.getElementById('global-search').addEventListener('keydown', e => { if(e.key==='Enter') globalSearch(e.target.value); });
    }

    function renderSidebar() {
      const list = $('#channel-list');
      list.innerHTML = channels.map(ch => `<div class="channel-item ${ch.id===activeChannel?'active':''}" data-channel="${ch.id}">
        <img src="${ch.avatar_url||'/api/placeholder/44/44'}" class="avatar" />
        <div class="channel-info"><div class="channel-name">${ch.name}</div><div class="channel-last-msg">${ch.last_msg||''}</div></div>
      </div>`).join('');
      document.querySelectorAll('.channel-item').forEach(el => el.onclick = () => switchChannel(el.dataset.channel));
    }

    async function switchChannel(id) {
      activeChannel = id;
      $('#channel-title').textContent = channels.find(c=>c.id===id)?.name||'';
      $('#chat-messages').innerHTML = 'Loading...';
      await loadMessages(id);
      renderMessages();
      subscribeToChannel(id);
      markRead(id);
      renderSidebar();
    }

    function renderMessages() {
      const msgs = channelMessages.get(activeChannel)||[];
      $('#chat-messages').innerHTML = msgs.map(m => {
        if(m.is_deleted) return `<div class="message-row system"><div class="message-bubble">Deleted</div></div>`;
        const isOut = m.user_id === currentUser.id;
        let html = `<div class="message-row ${isOut?'outgoing':'incoming'}">`;
        html += `<div class="message-bubble">${m.content.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</div>`;
        html += `<div class="message-meta"><span>${formatTime(m.created_at)}</span></div>`;
        html += `</div>`;
        return html;
      }).join('');
      if(scrollAtBottom) $('#chat-messages').scrollTop = $('#chat-messages').scrollHeight;
    }

    // ---------- MODALS (generic) ----------
    function openModal(html) {
      const modal = $('#global-modal');
      modal.innerHTML = `<div class="modal">${html}</div>`;
      modal.classList.remove('hidden');
      modal.querySelector('.modal').addEventListener('click', e => e.stopPropagation());
      modal.onclick = () => modal.classList.add('hidden');
    }

    // ---------- SPECIFIC MODALS ----------
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
        <hr>
        <h3>Change Password</h3>
        <input id="old-pwd" type="password" placeholder="Old password" />
        <input id="new-pwd" type="password" placeholder="New password" />
        <button onclick="changePwd()">Change Password</button>
        <hr>
        <h3>Block / Unblock</h3>
        <input id="block-user-id" placeholder="User ID to block/unblock" />
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

    function openChannelInfoModal() {
      if(!activeChannel) return;
      const ch = channels.find(c=>c.id===activeChannel);
      if(!ch) return;
      openModal(`
        <h2>Channel: ${ch.name}</h2>
        <p>Type: ${ch.type}</p>
        <button onclick="leaveChannel('${ch.id}')">Leave</button>
        <button onclick="archiveChannel('${ch.id}')">Archive</button>
        <button onclick="pinChannel('${ch.id}')">${ch.starred?'Unstar':'Star'}</button>
        <hr>
        <input id="add-member-id" placeholder="User ID to add" />
        <button onclick="addMember('${ch.id}', parseInt($('#add-member-id').value))">Add Member</button>
        <hr>
        <select id="ttl-select">
          <option value="86400">24 hours</option>
          <option value="604800">7 days</option>
          <option value="7776000">90 days</option>
          <option value="0">Off</option>
        </select>
        <button onclick="setDisappearing('${ch.id}', parseInt($('#ttl-select').value))">Set Disappearing</button>
      `);
    }

    async function openPinnedMessages() {
      if(!activeChannel) return;
      const res = await apiFetch(`/channels/${activeChannel}/messages`); // we need pinned endpoint, but for demo we just fetch pinned via client filter
      // For full implementation, you'd call a dedicated /pins endpoint. We'll just show recent messages with pin icon.
      const msgs = channelMessages.get(activeChannel)||[];
      const pinned = msgs.filter(m => m.pinned); // assuming pinned flag
      let html = pinned.map(m => `<div onclick="switchToMessage('${m.id}')" style="cursor:pointer;padding:4px;border-bottom:1px solid gray;">📌 ${m.content.substring(0,50)}</div>`).join('');
      openModal(`<h2>Pinned Messages</h2>${html||'No pinned messages'}`);
    }

    async function openBookmarksModal() {
      const bm = await getBookmarks();
      let html = bm.map(b => `
        <div style="padding:8px;border-bottom:1px solid #333;cursor:pointer;" onclick="jumpToBookmark('${b.message_id}')">
          <strong>${b.folder}</strong>: ${b.content?.substring(0,50)} (${b.created_at})
        </div>`).join('');
      openModal(`<h2>Bookmarks</h2>${html||'No bookmarks yet'}`);
      window.jumpToBookmark = (msgId) => {
        // For simplicity, we just switch to the channel and scroll. In a real app, you'd open the correct channel.
        // We'll assume the bookmark holds channel_id, so switch and scroll.
        const bmEntry = bm.find(b=>b.message_id===msgId);
        if(bmEntry?.channel_id) {
          switchChannel(bmEntry.channel_id);
          setTimeout(() => scrollToMessage(msgId), 500);
        }
        $('#global-modal').classList.add('hidden');
      };
    }

    async function openTasksModal() {
      const tasks = await getTasks();
      let html = tasks.map(t => `<div>${t.is_done?'✅':'⬜'} ${t.title} (due: ${t.due_date||'none'})</div>`).join('');
      openModal(`<h2>Tasks</h2>${html||'No tasks'}`);
    }

    async function openStatusesModal() {
      const statuses = await getStatuses();
      let html = statuses.map(s => `<div><strong>${s.username}</strong>: ${s.emoji} ${s.content}</div>`).join('');
      openModal(`<h2>Custom Statuses</h2>${html||'No statuses set'} <input id="status-text" placeholder="Your status" /><input id="status-emoji" placeholder="Emoji" /><button onclick="setCustomStatus($('#status-text').value, $('#status-emoji').value)">Set</button>`);
    }

    async function openAdminPanel() {
      const channels = await getAdminChannels();
      let html = channels.map(ch => `
        <div style="padding:4px;border-bottom:1px solid #333;">
          ${ch.name} (${ch.type})
          <button onclick="archiveChannel('${ch.id}')">Archive</button>
          <button onclick="forceJoin('${ch.id}')">Join</button>
        </div>`).join('');
      openModal(`<h2>Admin Panel</h2>${html}`);
      window.forceJoin = async (chId) => { await apiFetch(`/channels/${chId}/members`,{method:'POST',body:JSON.stringify({user_id:currentUser.id})}); toast('Joined'); };
    }

    // ---------- CONTEXT MENU (on messages) ----------
    window.showContextMenu = (e, msgId) => {
      e.preventDefault();
      const menu = $('#context-menu');
      menu.innerHTML = `
        <div class="context-menu-item" onclick="replyTo('${msgId}')">↩ Reply</div>
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
      menu.style.top = Math.min(e.clientY, window.innerHeight-200) + 'px';
      document.addEventListener('click', () => menu.style.display = 'none', {once:true});
    };
    window.replyTo = (id) => { replyToMessageId = id; toast('Reply mode'); };
    window.editMsg = async (id) => { const newText = prompt('Edit message:'); if(newText) await editMessage(id, newText); };
    window.deleteMsg = async (id) => { await deleteMessage(id); };
    window.copyMsg = async (id) => { const msgs = channelMessages.get(activeChannel); const msg = msgs.find(m=>m.id===id); if(msg) await navigator.clipboard.writeText(msg.content); toast('Copied'); };
    window.pinMsg = async (id) => { await pinMessage(id); toast('Pinned'); };
    window.reactMsg = async (id) => { const emoji = prompt('Emoji:'); if(emoji) await reactToMessage(id, emoji); };
    window.taskFromMsg = async (id) => { const title = prompt('Task title:'); if(title) await createTask(id, title); toast('Task created'); };
    window.bookmarkMsg = async (id) => { const folder = prompt('Folder (default General):')||'General'; await addBookmark(id, folder); toast('Bookmarked'); };
    window.tagMsg = async (id) => { const tag = prompt('Tag:'); if(tag) await apiFetch(`/messages/${id}/tag`,{method:'POST',body:JSON.stringify({tag})}); toast('Tagged'); };

    // ---------- INIT ----------
    async function initApp() {
      await checkAuth();
      if(!currentUser) renderAuthScreen();
      else {
        renderMainUI();
        await loadChannels();
        initPusher();
        if(activeChannel) switchChannel(activeChannel);
      }
    }
    window.onload = initApp;
  </script>
</body>
</html>
"""


# ---------- Root route serves the frontend ----------
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    return HTMLResponse(content=FRONTEND_HTML)

# ---------- Health check ----------
@app.get("/api/health")
async def health():
    try:
        await db_execute("SELECT 1")
        return {"status": "ok", "database": "connected"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"status": "error", "detail": str(e)})

# ---------- Debug route (temporary) ----------
@app.get("/api/debug/users")
async def debug_users():
    rows = await db_execute("SELECT id, username, email, password_hash FROM users")
    return rows

# ---------- Registration (FIXED) ----------
@app.post("/api/register")
async def register(data: dict):
    username = data.get("username", "").strip()
    email = data.get("email", "").strip()
    password = data.get("password", "")

    logger.info(f"Registering user: '{username}' / '{email}'")

    result = await db_run(
        "INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
        [username, email, password]
    )

    # rows_written is the reliable indicator for INSERT
    if result.get("rows_written", 0) == 0 and result.get("rows_affected", 0) == 0:
        logger.error(f"Insert failed for {username} – write count is 0. Full result: {result}")
        raise HTTPException(500, detail="User registration failed (database insert returned 0 rows)")

    # Verify existence
    rows = await db_execute("SELECT * FROM users WHERE username = ?", [username])
    if not rows:
        logger.error(f"User {username} not found after insert! Possible transaction issue.")
        raise HTTPException(500, detail="User registration could not be verified")

    logger.info(f"User {username} created successfully with id={rows[0]['id']}")
    return {"ok": True}

# ---------- Login (FIXED) ----------
@app.post("/api/login")
async def login(data: dict, response: Response):
    username = data.get("username", "").strip()
    password = data.get("password", "")

    logger.info(f"Login attempt: username='{username}', password='{password}'")

    rows = await db_execute("SELECT * FROM users WHERE username = ?", [username])
    if not rows:
        rows = await db_execute("SELECT * FROM users WHERE LOWER(username) = LOWER(?)", [username])

    if not rows:
        # Debug: list all usernames in table
        all_users = await db_execute("SELECT username FROM users")
        logger.warning(f"No user found for '{username}'. Existing usernames: {[u['username'] for u in all_users]}")
        raise HTTPException(401, detail="User not found")

    user = rows[0]
    stored_password = user.get("password_hash", "")

    if stored_password != password:
        logger.warning(
            f"Password mismatch for '{username}': "
            f"stored='{stored_password}' (len={len(stored_password)}), "
            f"provided='{password}' (len={len(password)})"
        )
        raise HTTPException(401, detail="Invalid credentials")

    token = jwt.encode(
        {"user_id": user["id"], "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7)},
        JWT_SECRET
    )
    response.set_cookie("token", token, httponly=True, secure=True, samesite="lax")
    sess_id = str(uuid.uuid4())
    await db_run(
        "INSERT INTO sessions (id, user_id, ip, browser) VALUES (?, ?, ?, ?)",
        [sess_id, user["id"], "unknown", "unknown"]
    )
    return {"ok": True}

@app.get("/api/me")
async def me(user=Depends(get_current_user)):
    return user



@app.put("/api/profile")
async def update_profile(data: dict, user=Depends(get_current_user)):
    await db_run("UPDATE users SET bio=?, phone=?, avatar_url=?, status_preset=? WHERE id=?",
                 [data.get("bio", user["bio"]), data.get("phone", user["phone"]),
                  data.get("avatar_url", user["avatar_url"]), data.get("status_preset", user["status_preset"]),
                  user["id"]])
    return {"ok": True}

@app.post("/api/change-password")
async def change_password(data: dict, user=Depends(get_current_user)):
    if data["old_password"] != user["password_hash"]:
        raise HTTPException(400, "Wrong password")
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
            (SELECT content FROM messages WHERE channel_id=c.id AND is_deleted=0 ORDER BY created_at DESC LIMIT 1) as last_msg
        FROM channels c JOIN channel_members cm ON c.id=cm.channel_id
        WHERE cm.user_id=? AND c.is_archived=0
        ORDER BY last_msg DESC
    """, [user["id"]])
    channels = []
    for r in rows:
        if r["type"] == "direct":
            other = await db_execute("SELECT u.username, u.avatar_url FROM channel_members cm JOIN users u ON cm.user_id=u.id WHERE cm.channel_id=? AND cm.user_id!=? LIMIT 1",
                                     [r["id"], user["id"]])
            if other:
                r["name"] = other[0]["username"]
                r["avatar_url"] = other[0]["avatar_url"]
        channels.append(r)
    return channels

@app.post("/api/channels")
async def create_channel(data: dict, user=Depends(get_current_user)):
    ch_id = str(uuid.uuid4())
    await db_run("INSERT INTO channels (id, name, type, created_by) VALUES (?, ?, ?, ?)",
                 [ch_id, data["name"], data["type"], user["id"]])
    await db_run("INSERT INTO channel_members (channel_id, user_id, role) VALUES (?, ?, 'admin')", [ch_id, user["id"]])
    for member_id in data.get("members", []):
        await db_run("INSERT INTO channel_members (channel_id, user_id) VALUES (?, ?)", [ch_id, member_id])
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
    await db_run("UPDATE messages SET disappearing_ttl=? WHERE channel_id=? AND created_at > ?",
                 [data["ttl"], ch_id, datetime.datetime.utcnow()])
    return {"ok": True}

# ---------- Messages ----------
@app.get("/api/channels/{ch_id}/messages")
async def get_messages(ch_id: str, before: str = None, user=Depends(get_current_user)):
    await check_membership(ch_id, user["id"])
    params = [ch_id]
    sql = "SELECT m.*, u.username, u.avatar_url FROM messages m JOIN users u ON m.user_id=u.id WHERE m.channel_id=? AND m.is_deleted=0"
    if before:
        sql += " AND m.created_at < ?"
        params.append(before)
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
async def send_message(ch_id: str, content: str = Form(None), type: str = Form("text"),
                       file: UploadFile = File(None), reply_to: str = Form(None),
                       thread_parent: str = Form(None), user=Depends(get_current_user)):
    await check_membership(ch_id, user["id"])
    msg_id = str(uuid.uuid4())
    file_url = content or ""
    if file:
        file_content = await file.read()
        file_url = f"data:{file.content_type};base64,{base64.b64encode(file_content).decode()}"
    await db_run("INSERT INTO messages (id, channel_id, user_id, content, type, reply_to, thread_parent) VALUES (?, ?, ?, ?, ?, ?, ?)",
                 [msg_id, ch_id, user["id"], file_url, type, reply_to, thread_parent])
    msg_rows = await db_execute("SELECT m.*, u.username, u.avatar_url FROM messages m JOIN users u ON m.user_id=u.id WHERE m.id=?", [msg_id])
    msg = serialize_message(msg_rows[0])
    pusher_client.trigger(f'private-channel-{ch_id}', 'new-message', {'message': msg})
    members = await db_execute("SELECT user_id FROM channel_members WHERE channel_id=? AND user_id!=?", [ch_id, user["id"]])
    for m in members:
        await db_run("UPDATE channel_members SET unread_count = unread_count + 1 WHERE channel_id=? AND user_id=?", [ch_id, m["user_id"]])
    return {"id": msg_id}

@app.put("/api/messages/{msg_id}")
async def edit_message(msg_id: str, data: dict, user=Depends(get_current_user)):
    msg = await db_execute("SELECT * FROM messages WHERE id=? AND user_id=?", [msg_id, user["id"]])
    if not msg:
        raise HTTPException(403)
    await db_run("UPDATE messages SET content=?, is_edited=1, edited_at=CURRENT_TIMESTAMP WHERE id=?", [data["content"], msg_id])
    updated = await db_execute("SELECT * FROM messages WHERE id=?", [msg_id])
    pusher_client.trigger(f'private-channel-{msg[0]["channel_id"]}', 'message-updated', {'message': serialize_message(updated[0])})
    return {"ok": True}

@app.delete("/api/messages/{msg_id}")
async def delete_message(msg_id: str, user=Depends(get_current_user)):
    msg = await db_execute("SELECT * FROM messages WHERE id=?", [msg_id])
    if not msg:
        raise HTTPException(404)
    ch_id = msg[0]["channel_id"]
    mem = await db_execute("SELECT role FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    if not mem or (msg[0]["user_id"] != user["id"] and mem[0]["role"] not in ("admin", "moderator")):
        raise HTTPException(403)
    await db_run("UPDATE messages SET is_deleted=1 WHERE id=?", [msg_id])
    pusher_client.trigger(f'private-channel-{ch_id}', 'message-deleted', {'message_id': msg_id})
    return {"ok": True}

@app.post("/api/channels/{ch_id}/forward")
async def forward_messages(ch_id: str, data: dict, user=Depends(get_current_user)):
    for msg_id in data["message_ids"]:
        orig = await db_execute("SELECT * FROM messages WHERE id=?", [msg_id])
        if orig:
            new_id = str(uuid.uuid4())
            await db_run("INSERT INTO messages (id, channel_id, user_id, content, type) VALUES (?, ?, ?, ?, ?)",
                         [new_id, ch_id, user["id"], orig[0]["content"], orig[0]["type"]])
    pusher_client.trigger(f'private-channel-{ch_id}', 'refresh', {})
    return {"ok": True}

# ---------- Reactions ----------
@app.post("/api/messages/{msg_id}/react")
async def react_to_message(msg_id: str, data: dict, user=Depends(get_current_user)):
    await db_run("INSERT OR REPLACE INTO reactions (message_id, user_id, emoji) VALUES (?, ?, ?)", [msg_id, user["id"], data["emoji"]])
    msg = await db_execute("SELECT channel_id FROM messages WHERE id=?", [msg_id])
    if msg:
        reacts = await db_execute("SELECT r.emoji, r.user_id, u.username FROM reactions r JOIN users u ON r.user_id=u.id WHERE r.message_id=?", [msg_id])
        pusher_client.trigger(f'private-channel-{msg[0]["channel_id"]}', 'reaction-updated', {
            'message_id': msg_id,
            'reactions': group_reactions(reacts)
        })
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
        for m in msgs:
            await db_run("INSERT OR IGNORE INTO read_receipts (message_id, user_id) VALUES (?, ?)", [m["id"], user["id"]])
        message_ids = [m["id"] for m in msgs]
        pusher_client.trigger(f'private-channel-{ch_id}', 'read-receipt', {'message_ids': message_ids, 'user_id': user["id"]})
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
    if not tag:
        await db_run("INSERT INTO tags (name) VALUES (?)", [tag_name])
        tag = await db_execute("SELECT id FROM tags WHERE name=?", [tag_name])
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
    await db_run("INSERT INTO custom_statuses (user_id, type, content, emoji) VALUES (?, ?, ?, ?)",
                 [user["id"], data.get("type", "text"), data.get("content"), data.get("emoji", "")])
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
    if user["role"] != "admin":
        raise HTTPException(403)
    return await db_execute("SELECT * FROM channels")

# ---------- Pusher Auth ----------
@app.post("/api/pusher/auth")
async def pusher_auth(request: Request, user=Depends(get_current_user)):
    form = await request.form()
    channel_name = form["channel_name"]
    socket_id = form["socket_id"]
    if not channel_name.startswith("private-channel-"):
        raise HTTPException(403)
    ch_id = channel_name.split("-", 2)[2]
    await check_membership(ch_id, user["id"])
    auth = pusher_client.authenticate(channel=channel_name, socket_id=socket_id, custom_data={"user_id": str(user["id"])})
    return auth

# ---------- Interactive Buttons ----------
@app.post("/api/interactive/{btn_id}/respond")
async def respond_button(btn_id: int, user=Depends(get_current_user)):
    await db_run("INSERT OR IGNORE INTO interactive_responses (button_id, user_id) VALUES (?, ?)", [btn_id, user["id"]])
    return {"ok": True}
