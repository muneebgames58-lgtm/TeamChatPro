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
    """Execute SQL via Turso HTTP pipeline API (improved error/response handling)."""
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

            # Log the full response for debugging (can be removed later)
            logger.info(f"Turso response: {json.dumps(data, indent=2)[:500]}")

            results = data.get("results", [])
            if not results:
                return {"rows": [], "rows_written": 0, "rows_read": 0}

            # Process the first execute result
            exec_result = results[0]
            if exec_result.get("type") != "execute":
                return {"rows": [], "rows_written": 0, "rows_read": 0}

            response_data = exec_result.get("response", {})
            if response_data.get("type") == "error":
                error_msg = response_data.get("error", "unknown SQL error")
                logger.error(f"SQL error: {error_msg}")
                raise HTTPException(500, f"SQL error: {error_msg}")

            result = response_data.get("result", {})
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
                "rows_affected": result.get("rows_affected", 0)  # legacy
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

# ---------- Embedded Frontend HTML (your exact copy) ----------
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
    <link rel="manifest" href="/manifest.json" />
    <meta name="theme-color" content="#111b21" />
    <style>
      :root {
        --bg-main: #111b21;
        --bg-panel: #202c33;
        --bg-chat: #0b141a;
        --outgoing-bubble: #005c4b;
        --incoming-bubble: #202c33;
        --accent: #00a884;
        --red: #f15c6d;
        --text-primary: #e9edef;
        --text-secondary: #8696a0;
        --radius-bubble: 8px;
        --radius-modal: 12px;
        --sidebar-width: 35%;
        --font-family: 'Inter', sans-serif;
        --transition: 0.2s ease;
      }

      [data-theme="light"] {
        --bg-main: #f0f2f5;
        --bg-panel: #ffffff;
        --bg-chat: #efeae2;
        --outgoing-bubble: #d9fdd3;
        --incoming-bubble: #ffffff;
        --accent: #008069;
        --text-primary: #111b21;
        --text-secondary: #667781;
      }

      * {
        box-sizing: border-box;
        margin: 0;
        padding: 0;
      }

      body {
        font-family: var(--font-family);
        background: var(--bg-main);
        color: var(--text-primary);
        height: 100vh;
        overflow: hidden;
        -webkit-tap-highlight-color: transparent;
      }

      #app {
        display: flex;
        height: 100vh;
      }

      /* Sidebar */
      .sidebar {
        width: var(--sidebar-width);
        min-width: 320px;
        max-width: 500px;
        background: var(--bg-panel);
        display: flex;
        flex-direction: column;
        border-right: 1px solid rgba(0, 0, 0, 0.2);
        position: relative;
      }

      .sidebar-header {
        display: flex;
        align-items: center;
        padding: 10px 16px;
        background: var(--bg-panel);
        gap: 8px;
      }

      .sidebar-header .avatar {
        width: 40px;
        height: 40px;
        border-radius: 50%;
      }

      .search-box {
        padding: 0 12px 8px;
        position: relative;
      }

      .search-box input {
        width: 100%;
        padding: 8px 12px 8px 36px;
        border-radius: 8px;
        border: none;
        background: var(--bg-main);
        color: var(--text-primary);
        font-size: 14px;
      }

      .search-box .search-icon {
        position: absolute;
        left: 22px;
        top: 50%;
        transform: translateY(-50%);
        color: var(--text-secondary);
      }

      .channel-list {
        overflow-y: auto;
        flex: 1;
      }

      .channel-item {
        display: flex;
        align-items: center;
        padding: 10px 16px;
        cursor: pointer;
        transition: background var(--transition);
        gap: 10px;
        border-bottom: 1px solid rgba(255, 255, 255, 0.05);
      }

      .channel-item:hover {
        background: rgba(255, 255, 255, 0.05);
      }

      .channel-item.active {
        background: rgba(0, 168, 132, 0.15);
      }

      .channel-item .avatar,
      .channel-item .group-avatar {
        width: 44px;
        height: 44px;
        border-radius: 50%;
      }

      .channel-info {
        flex: 1;
        overflow: hidden;
      }

      .channel-name {
        font-weight: 500;
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .channel-last-msg {
        font-size: 13px;
        color: var(--text-secondary);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
      }

      .unread-badge {
        background: var(--accent);
        color: white;
        border-radius: 50%;
        min-width: 20px;
        height: 20px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 12px;
        padding: 0 4px;
      }

      /* Resize bar */
      .resize-bar {
        width: 5px;
        background: transparent;
        cursor: col-resize;
        position: absolute;
        right: -2px;
        top: 0;
        bottom: 0;
        z-index: 10;
      }

      /* Main Chat Area */
      .chat-area {
        flex: 1;
        display: flex;
        flex-direction: column;
        background: var(--bg-chat);
        background-size: cover;
        position: relative;
      }

      .chat-header {
        display: flex;
        align-items: center;
        padding: 10px 16px;
        background: var(--bg-panel);
        gap: 10px;
        border-bottom: 1px solid rgba(0, 0, 0, 0.1);
      }

      .chat-header .back-btn {
        display: none;
        background: none;
        border: none;
        color: var(--text-primary);
        font-size: 20px;
        cursor: pointer;
      }

      .channel-title {
        flex: 1;
        font-weight: 600;
      }

      .chat-messages {
        flex: 1;
        overflow-y: auto;
        padding: 20px;
        background-image: url('data:image/svg+xml;utf8,<svg xmlns="http://www.w3.org/2000/svg" width="100" height="100" viewBox="0 0 100 100"><path d="M30 20 L30 40 M40 20 L40 40 M50 20 L50 40 M60 20 L60 40 M70 20 L70 40" stroke="%23555" stroke-width="0.5" opacity="0.05"/></svg>');
        display: flex;
        flex-direction: column;
        gap: 4px;
      }

      .message-group {
        display: flex;
        flex-direction: column;
      }

      .message-row {
        display: flex;
        max-width: 75%;
        margin-bottom: 2px;
        position: relative;
      }

      .message-row.outgoing {
        align-self: flex-end;
      }

      .message-row.incoming {
        align-self: flex-start;
      }

      .message-bubble {
        padding: 8px 12px;
        border-radius: var(--radius-bubble);
        font-size: 14px;
        line-height: 1.4;
        word-break: break-word;
        position: relative;
      }

      .outgoing .message-bubble {
        background: var(--outgoing-bubble);
        border-top-right-radius: 2px;
      }

      .incoming .message-bubble {
        background: var(--incoming-bubble);
        border-top-left-radius: 2px;
      }

      .message-meta {
        display: flex;
        justify-content: flex-end;
        align-items: center;
        gap: 4px;
        font-size: 11px;
        color: var(--text-secondary);
        margin-top: 2px;
      }

      .message-reactions {
        display: flex;
        gap: 4px;
        margin-top: 2px;
      }

      .reaction-badge {
        background: rgba(0, 0, 0, 0.2);
        border-radius: 12px;
        padding: 2px 6px;
        font-size: 12px;
        cursor: pointer;
      }

      .reply-preview {
        background: rgba(0, 0, 0, 0.1);
        border-left: 3px solid var(--accent);
        padding: 4px 8px;
        margin-bottom: 4px;
        border-radius: 4px;
        font-size: 12px;
        color: var(--text-secondary);
        cursor: pointer;
      }

      .scroll-down-btn {
        position: absolute;
        bottom: 80px;
        right: 20px;
        background: var(--accent);
        color: white;
        border-radius: 50%;
        width: 40px;
        height: 40px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
        opacity: 0;
        pointer-events: none;
        transition: opacity var(--transition);
      }

      .scroll-down-btn.visible {
        opacity: 1;
        pointer-events: all;
      }

      /* Input Area */
      .chat-input {
        padding: 10px 16px;
        background: var(--bg-panel);
        display: flex;
        align-items: center;
        gap: 8px;
      }

      .chat-input textarea {
        flex: 1;
        resize: none;
        border-radius: 20px;
        border: none;
        padding: 8px 16px;
        background: var(--bg-main);
        color: var(--text-primary);
        font-family: var(--font-family);
        font-size: 14px;
        outline: none;
        max-height: 100px;
      }

      .send-btn {
        background: var(--accent);
        border: none;
        color: white;
        border-radius: 50%;
        width: 42px;
        height: 42px;
        display: flex;
        align-items: center;
        justify-content: center;
        cursor: pointer;
      }

      /* Modals */
      .modal-overlay {
        position: fixed;
        top: 0;
        left: 0;
        right: 0;
        bottom: 0;
        background: rgba(0, 0, 0, 0.7);
        display: flex;
        align-items: center;
        justify-content: center;
        z-index: 1000;
        animation: fadeIn 0.2s;
      }

      .modal {
        background: var(--bg-panel);
        border-radius: var(--radius-modal);
        max-width: 600px;
        width: 90%;
        max-height: 85vh;
        overflow-y: auto;
        padding: 24px;
        color: var(--text-primary);
      }

      .modal h2 {
        margin-bottom: 16px;
      }

      .modal input,
      .modal textarea,
      .modal select {
        width: 100%;
        padding: 10px;
        margin-bottom: 12px;
        background: var(--bg-main);
        border: 1px solid var(--text-secondary);
        border-radius: 6px;
        color: var(--text-primary);
      }

      .modal button {
        padding: 10px 20px;
        background: var(--accent);
        border: none;
        color: white;
        border-radius: 6px;
        cursor: pointer;
      }

      /* Context Menu */
      .context-menu {
        position: fixed;
        background: var(--bg-panel);
        border-radius: 8px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5);
        z-index: 2000;
        padding: 4px 0;
        min-width: 180px;
        display: none;
      }

      .context-menu-item {
        padding: 8px 16px;
        cursor: pointer;
        font-size: 14px;
        display: flex;
        align-items: center;
        gap: 8px;
      }

      .context-menu-item:hover {
        background: rgba(255, 255, 255, 0.1);
      }

      /* Toast */
      .toast-container {
        position: fixed;
        bottom: 20px;
        left: 50%;
        transform: translateX(-50%);
        z-index: 3000;
      }

      .toast {
        background: var(--accent);
        color: white;
        padding: 10px 24px;
        border-radius: 24px;
        margin-top: 8px;
        animation: slideUp 0.3s ease;
      }

      /* Skeleton */
      .skeleton {
        background: linear-gradient(90deg, var(--bg-main) 25%, rgba(255, 255, 255, 0.05) 50%, var(--bg-main) 75%);
        background-size: 200% 100%;
        animation: shimmer 1.5s infinite;
        border-radius: 8px;
      }

      /* Utility */
      .hidden {
        display: none !important;
      }

      .flex {
        display: flex;
        align-items: center;
        gap: 8px;
      }

      @keyframes fadeIn {
        from {
          opacity: 0;
        }

        to {
          opacity: 1;
        }
      }

      @keyframes slideUp {
        from {
          opacity: 0;
          transform: translateY(10px);
        }

        to {
          opacity: 1;
          transform: translateY(0);
        }
      }

      @keyframes shimmer {
        0% {
          background-position: 200% 0;
        }

        100% {
          background-position: -200% 0;
        }
      }

      /* Mobile responsive */
      @media (max-width: 768px) {
        .sidebar {
          width: 100% !important;
          max-width: 100% !important;
        }

        .chat-area {
          display: none;
        }

        .chat-area.mobile-open {
          display: flex;
        }

        .chat-header .back-btn {
          display: block;
        }
      }
    </style>
  </head>

  <body>
    <div id="app"></div>
    <script src="https://js.pusher.com/8.2.0/pusher.min.js"></script>
    <script>
      // ---------- GLOBALS & STATE ----------
      const API = '/api';
      let currentUser = null;
      let activeChannel = null;
      let channels = [];
      let channelMessages = new Map(); // channelId -> messages[]
      let typingUsers = {};
      let socket = null;
      let replyToMessageId = null;
      let selectedMessages = new Set(); // for forwarding
      let searchQuery = '';
      let activeView = 'all'; // all | unread | mentions | folder-xyz | starred
      let currentFolder = null;
      let lightboxImage = null;
      let recordingMedia = null; // MediaRecorder
      let audioChunks = [];
      let voiceNoteBlob = null;
      let globalSearchTimer = null;
      let scrollAtBottom = true;
      let isLoadingMore = false;
      // ---------- UTILS ----------
      const $ = s => document.querySelector(s);
      const $$ = s => document.querySelectorAll(s);
      const toast = msg => {
        let container = $('.toast-container');
        if (!container) {
          container = document.createElement('div');
          container.className = 'toast-container';
          document.body.appendChild(container);
        }
        const el = document.createElement('div');
        el.className = 'toast';
        el.textContent = msg;
        container.appendChild(el);
        setTimeout(() => el.remove(), 3000);
      };
      const formatDate = d => new Date(d).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric'
      });
      const formatTime = d => new Date(d).toLocaleTimeString('en-US', {
        hour: '2-digit',
        minute: '2-digit'
      });
      const formatFullTime = d => new Date(d).toLocaleString();
      const generateId = () => crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substr(2, 9);
      // ---------- API WRAPPERS ----------
      async function apiFetch(url, options = {}) {
        const headers = {
          ...options.headers
        };
        if (!(options.body instanceof FormData)) headers['Content-Type'] = 'application/json';
        const res = await fetch(API + url, {
          credentials: 'include',
          headers,
          ...options
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({
            detail: 'Request failed'
          }));
          throw new Error(err.detail || 'Error');
        }
        return res.json();
      }
      // ---------- AUTH ----------
      async function checkAuth() {
        try {
          currentUser = await apiFetch('/me');
        } catch {
          currentUser = null;
        }
      }
    async function login(username, password) {
      await apiFetch('/login', {
        method: 'POST',
        body: JSON.stringify({
          username: username.trim(),    // <-- added .trim()
          password
        })
      });
      await checkAuth();
    }
    
    async function register(username, email, password) {
      await apiFetch('/register', {
        method: 'POST',
        body: JSON.stringify({
          username: username.trim(),
          email: email.trim(),
          password
        })
      });
      await checkAuth();
    }
      async function logout() {
        await apiFetch('/logout', {
          method: 'POST'
        });
        currentUser = null;
        activeChannel = null;
        channels = [];
        channelMessages.clear();
        if (socket) socket.disconnect();
      }
      async function changePassword(oldPwd, newPwd) {
        await apiFetch('/change-password', {
          method: 'POST',
          body: JSON.stringify({
            old_password: oldPwd,
            new_password: newPwd
          })
        });
      }
      async function updateProfile(data) {
        await apiFetch('/profile', {
          method: 'PUT',
          body: JSON.stringify(data)
        });
        currentUser = await apiFetch('/me');
      }
      async function blockUser(userId) {
        await apiFetch(`/block/${userId}`, {
          method: 'POST'
        });
      }
      async function unblockUser(userId) {
        await apiFetch(`/block/${userId}`, {
          method: 'DELETE'
        });
      }
      // ---------- CHANNELS ----------
      async function loadChannels() {
        const data = await apiFetch('/channels');
        channels = data;
        renderSidebar();
      }
      async function createChannel(type, name, members = []) {
        await apiFetch('/channels', {
          method: 'POST',
          body: JSON.stringify({
            type,
            name,
            members
          })
        });
        await loadChannels();
      }
      async function leaveChannel(channelId) {
        await apiFetch(`/channels/${channelId}/leave`, {
          method: 'POST'
        });
        if (activeChannel === channelId) activeChannel = null;
        await loadChannels();
      }
      async function archiveChannel(channelId) {
        await apiFetch(`/channels/${channelId}/archive`, {
          method: 'POST'
        });
        await loadChannels();
      }
      async function pinChannel(channelId) {
        await apiFetch(`/channels/${channelId}/pin`, {
          method: 'POST'
        });
      }
      async function addMember(channelId, userId) {
        await apiFetch(`/channels/${channelId}/members`, {
          method: 'POST',
          body: JSON.stringify({
            user_id: userId
          })
        });
      }
      async function setDisappearing(channelId, ttl) {
        await apiFetch(`/channels/${channelId}/disappearing`, {
          method: 'POST',
          body: JSON.stringify({
            ttl
          })
        });
      }
      // ---------- MESSAGES ----------
      async function loadMessages(channelId, before = null) {
        if (!channelId) return;
        const url = before ? `/channels/${channelId}/messages?before=${before}` : `/channels/${channelId}/messages`;
        const msgs = await apiFetch(url);
        if (!channelMessages.has(channelId)) channelMessages.set(channelId, []);
        const existing = channelMessages.get(channelId);
        const ids = new Set(existing.map(m => m.id));
        const newMsgs = msgs.filter(m => !ids.has(m.id) && !m.is_deleted);
        if (newMsgs.length) {
          channelMessages.set(channelId, [...existing, ...newMsgs].sort((a, b) => new Date(a.created_at) - new Date(b.created_at)));
        }
      }
      async function sendMessage(content, type = 'text', file = null) {
        if (!activeChannel) return;
        if (type === 'text') {
          await apiFetch(`/channels/${activeChannel}/messages`, {
            method: 'POST',
            body: JSON.stringify({
              content,
              type,
              reply_to: replyToMessageId
            })
          });
        } else {
          const form = new FormData();
          form.append('content', content);
          form.append('type', type);
          form.append('file', file);
          if (replyToMessageId) form.append('reply_to', replyToMessageId);
          await fetch(API + `/channels/${activeChannel}/messages`, {
            method: 'POST',
            credentials: 'include',
            body: form
          });
        }
        replyToMessageId = null;
      }
      async function deleteMessage(msgId) {
        await apiFetch(`/messages/${msgId}`, {
          method: 'DELETE'
        });
      }
      async function editMessage(msgId, content) {
        await apiFetch(`/messages/${msgId}`, {
          method: 'PUT',
          body: JSON.stringify({
            content
          })
        });
      }
      async function forwardMessages(channelId, messageIds) {
        await apiFetch(`/channels/${channelId}/forward`, {
          method: 'POST',
          body: JSON.stringify({
            message_ids: messageIds
          })
        });
        selectedMessages.clear();
      }
      async function reactToMessage(msgId, emoji) {
        await apiFetch(`/messages/${msgId}/react`, {
          method: 'POST',
          body: JSON.stringify({
            emoji
          })
        });
      }
      async function pinMessage(msgId) {
        await apiFetch(`/messages/${msgId}/pin`, {
          method: 'POST'
        });
      }
      async function markRead(channelId) {
        await apiFetch(`/channels/${channelId}/read`, {
          method: 'POST'
        });
      }
      async function sendTyping(channelId) {
        await apiFetch(`/channels/${channelId}/typing`, {
          method: 'POST'
        });
      }
      // ---------- REAL TIME (Pusher) ----------
      function initPusher() {
        if (socket) socket.disconnect();
        socket = new Pusher(PUSHER_KEY, {
          cluster: PUSHER_CLUSTER,
          authEndpoint: API + '/pusher/auth',
          encrypted: true
        });
        socket.connection.bind('connected', () => {
          if (activeChannel) subscribeToChannel(activeChannel);
        });
      }

      function subscribeToChannel(channelId) {
        socket.unsubscribe(`private-channel-${channelId}`);
        const channel = socket.subscribe(`private-channel-${channelId}`);
        channel.bind('new-message', data => {
          const msgs = channelMessages.get(channelId) || [];
          msgs.push(data.message);
          channelMessages.set(channelId, msgs);
          if (activeChannel === channelId) renderMessages();
        });
        channel.bind('message-updated', data => {
          const msgs = channelMessages.get(channelId);
          if (!msgs) return;
          const idx = msgs.findIndex(m => m.id === data.message.id);
          if (idx > -1) msgs[idx] = {
            ...msgs[idx],
            ...data.message
          };
          if (activeChannel === channelId) renderMessages();
        });
        channel.bind('message-deleted', data => {
          const msgs = channelMessages.get(channelId);
          if (!msgs) return;
          const msg = msgs.find(m => m.id === data.message_id);
          if (msg) msg.is_deleted = true;
          if (activeChannel === channelId) renderMessages();
        });
        channel.bind('reaction-updated', data => {
          const msgs = channelMessages.get(channelId);
          if (!msgs) return;
          const msg = msgs.find(m => m.id === data.message_id);
          if (msg) msg.reactions = data.reactions;
          if (activeChannel === channelId) renderMessages();
        });
        channel.bind('typing', data => {
          typingUsers[channelId] = typingUsers[channelId] || {};
          typingUsers[channelId][data.user_id] = data.username;
          updateTypingIndicator();
        });
        channel.bind('read-receipt', data => {
          const msgs = channelMessages.get(channelId);
          if (!msgs) return;
          msgs.forEach(m => {
            if (data.message_ids.includes(m.id)) m.read_status = 'read';
          });
          if (activeChannel === channelId) renderMessages();
        });
      }

      function updateTypingIndicator() {
        if (!activeChannel) return;
        const typing = Object.values(typingUsers[activeChannel] || {});
        const el = $('#typing-indicator');
        if (el) el.textContent = typing.length ? typing.join(', ') + ' typing…' : '';
        setTimeout(() => {
          delete typingUsers[activeChannel];
          if (activeChannel) $('#typing-indicator').textContent = '';
        }, 5000);
      }
      // ---------- RENDERING ----------
      async function initApp() {
        await checkAuth();
        if (!currentUser) {
          renderAuthScreen();
        } else {
          renderMainUI();
          await loadChannels();
          initPusher();
          initEventListeners();
          if (activeChannel) switchChannel(activeChannel);
          checkUrlParams();
        }
      }

      function renderAuthScreen() {
        $('#app').innerHTML = `
    <div class="modal-overlay" style="display:flex;">
      <div class="modal">
        <h2>Welcome to Chatta</h2>
        <input id="auth-username" placeholder="Username" />
        <input id="auth-email" placeholder="Email (for register)" />
        <input id="auth-password" type="password" placeholder="Password" />
        <div class="flex">
          <button id="btn-login">Login</button>
          <button id="btn-register">Register</button>
        </div>
      </div>
    </div>`;
        document.getElementById('btn-login').onclick = async () => {
          const u = document.getElementById('auth-username').value;
          const p = document.getElementById('auth-password').value;
          try {
            await login(u, p);
            initApp();
          } catch (e) {
            toast(e.message);
          }
        };
        document.getElementById('btn-register').onclick = async () => {
          const u = document.getElementById('auth-username').value;
          const e = document.getElementById('auth-email').value;
          const p = document.getElementById('auth-password').value;
          try {
            await register(u, e, p);
            initApp();
          } catch (e) {
            toast(e.message);
          }
        };
      }

      function renderMainUI() {
        $('#app').innerHTML = `
    <div class="sidebar" id="sidebar">
      <div class="sidebar-header">
        <img src="${currentUser.avatar_url}" class="avatar" onclick="openProfile()" />
        <div style="flex:1">
          <div style="font-weight:600">${currentUser.username}</div>
          <div style="font-size:12px;color:var(--text-secondary)">${currentUser.status_preset}</div>
        </div>
        <button id="btn-logout" title="Logout">⏻</button>
        <button id="btn-theme" title="Toggle theme">🌓</button>
        <button id="btn-admin" title="Admin Panel" style="${currentUser.role === 'admin' ? '' : 'display:none'}">⚙</button>
      </div>
      <div class="search-box">
        <span class="search-icon">🔍</span>
        <input id="global-search" placeholder="Search or start new chat (Ctrl+K)" />
      </div>
      <div class="channel-list" id="channel-list"></div>
      <div class="resize-bar" id="resize-bar"></div>
    </div>
    <div class="chat-area" id="chat-area">
      <div class="chat-header">
        <button class="back-btn" id="mobile-back">←</button>
        <div class="channel-title" id="channel-title">Select a channel</div>
        <div id="typing-indicator" style="font-size:12px;color:var(--accent);"></div>
        <button id="btn-pinned" title="Pinned messages">📌</button>
        <button id="btn-media-gallery" title="Shared media">🖼</button>
        <button id="btn-search-in-chat" title="Search">🔎</button>
        <button id="btn-channel-info" title="Channel info">⚙</button>
      </div>
      <div class="chat-messages" id="chat-messages"></div>
      <div class="scroll-down-btn" id="scroll-down-btn">↓</div>
      <div class="chat-input">
        <button id="btn-emoji" title="Emoji">😊</button>
        <button id="btn-attach" title="Attach">📎</button>
        <textarea id="message-input" placeholder="Type a message..." rows="1"></textarea>
        <button id="btn-voice" title="Voice note">🎤</button>
        <button class="send-btn" id="btn-send">➤</button>
      </div>
    </div>
    <div class="context-menu" id="context-menu"></div>
    <div class="toast-container"></div>
    <div class="modal-overlay hidden" id="global-modal"></div>
    <div class="lightbox-overlay hidden" id="lightbox-overlay" style="position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.9);z-index:5000;display:flex;align-items:center;justify-content:center;">
      <img id="lightbox-img" style="max-width:90%;max-height:90%;" />
      <button id="lightbox-close" style="position:absolute;top:20px;right:20px;background:none;border:none;font-size:30px;color:white;">✕</button>
      <a id="lightbox-download" download style="position:absolute;bottom:20px;right:20px;background:var(--accent);padding:10px;border-radius:50%;color:white;">⬇</a>
    </div>`;
      }

      function renderSidebar() {
        const list = document.getElementById('channel-list');
        if (!list) return;
        let filteredChannels = channels;
        if (activeView === 'unread') filteredChannels = channels.filter(ch => ch.unread_count > 0);
        else if (activeView === 'starred') filteredChannels = channels.filter(ch => ch.starred);
        // else folder / all
        list.innerHTML = filteredChannels.map(ch => `
    <div class="channel-item ${ch.id === activeChannel ? 'active' : ''}" data-channel="${ch.id}">
      <img src="${ch.avatar_url || '/api/placeholder/44/44'}" class="avatar" />
      <div class="channel-info">
        <div class="channel-name">${ch.name} ${ch.unread_count ? `<span class="unread-badge">${ch.unread_count}</span>` : ''}</div>
        <div class="channel-last-msg">${ch.last_msg || ''}</div>
      </div>
    </div>`).join('');
        // event delegation
        document.querySelectorAll('.channel-item').forEach(el => {
          el.onclick = () => switchChannel(el.dataset.channel);
        });
      }
      async function switchChannel(channelId) {
        if (!channelId || activeChannel === channelId) return;
        activeChannel = channelId;
        document.getElementById('channel-title').textContent = channels.find(c => c.id === channelId)?.name || '';
        document.getElementById('chat-messages').innerHTML = '<div class="skeleton" style="height:400px;"></div>';
        await loadMessages(channelId);
        renderMessages();
        subscribeToChannel(channelId);
        markRead(channelId);
        renderSidebar();
        scrollToBottom(true);
        // Mobile: show chat area
        document.getElementById('chat-area').classList.add('mobile-open');
      }

      function renderMessages() {
        const container = document.getElementById('chat-messages');
        if (!container || !activeChannel) return;
        const msgs = channelMessages.get(activeChannel) || [];
        let html = '';
        let lastUser = null,
          lastTime = 0;
        msgs.forEach((msg, i) => {
          if (msg.is_deleted) {
            html += `<div class="message-row system"><div class="message-bubble" style="font-style:italic;color:var(--text-secondary);">This message was deleted</div></div>`;
            return;
          }
          const isOutgoing = msg.user_id === currentUser.id;
          const showAvatar = !isOutgoing && msg.user_id !== lastUser;
          const timeDiff = new Date(msg.created_at) - lastTime;
          const showTime = i === 0 || timeDiff > 300000;
          if (showTime) html += `<div class="message-time" style="text-align:center;font-size:12px;color:var(--text-secondary);margin:10px 0;">${formatDate(msg.created_at)}</div>`;
          html += `
      <div class="message-row ${isOutgoing ? 'outgoing' : 'incoming'}" data-id="${msg.id}" oncontextmenu="showContextMenu(event, '${msg.id}')">
        ${!isOutgoing && showAvatar ? `<img src="${msg.avatar_url || '/api/placeholder/30/30'}" class="avatar" style="width:30px;height:30px;align-self:flex-end;margin-right:6px;" />` : ''}
        <div style="display:flex;flex-direction:column;max-width:75%;">
          ${msg.reply_to ? `<div class="reply-preview" onclick="scrollToMessage('${msg.reply_to}')">↩ Replying to a message</div>` : ''}
          ${msg.thread_parent ? `<div class="thread-indicator" style="font-size:12px;color:var(--accent);cursor:pointer;" onclick="openThread('${msg.thread_parent}')">🧵 View thread</div>` : ''}
          <div class="message-bubble" style="${msg.type === 'image' ? 'padding:4px;' : ''} ${msg.type === 'interactive' ? 'background:transparent;border:1px solid var(--accent);' : ''}">
            ${msg.type === 'text' || msg.type === 'draft' ? formatContent(msg.content) : ''}
            ${msg.type === 'image' ? `<img src="${msg.content}" style="max-width:250px;border-radius:8px;cursor:pointer;" onclick="openLightbox('${msg.content}')" />` : ''}
            ${msg.type === 'video' ? `<video controls src="${msg.content}" style="max-width:250px;border-radius:8px;"></video>` : ''}
            ${msg.type === 'audio' ? `<audio controls src="${msg.content}" style="max-width:200px;"></audio>` : ''}
            ${msg.type === 'file' ? `<a href="${msg.content}" target="_blank" style="color:var(--accent);">📎 ${msg.filename || 'File'}</a>` : ''}
            ${msg.type === 'vcard' ? `<div class="vcard" style="background:var(--bg-main);padding:8px;border-radius:8px;">👤 ${msg.vcard_name}</div>` : ''}
            ${msg.type === 'interactive' ? `<div>${msg.content}</div><div style="display:flex;gap:6px;margin-top:6px;">${msg.buttons.map(b => `<button class="interactive-btn" data-btn-id="${b.id}" style="padding:6px 12px;border-radius:20px;border:1px solid var(--accent);background:var(--bg-panel);color:var(--text-primary);cursor:pointer;">${b.label}</button>`).join('')}</div>` : ''}
          </div>
          <div class="message-reactions">${(msg.reactions || []).map(r => `<span class="reaction-badge" onclick="showReactionDetail('${msg.id}', '${r.emoji}')" title="${r.users.map(u=>u.username).join(', ')}">${r.emoji} ${r.count}</span>`).join('')}</div>
          <div class="message-meta">
            ${msg.is_edited ? '<span>✎</span>' : ''}
            <span class="ticks" onclick="showReadReceipts('${msg.id}')">${msg.read_status === 'read' ? '✓✓' : (msg.read_status === 'delivered' ? '✓✓' : '✓')}</span>
            <span>${formatTime(msg.created_at)}</span>
          </div>
        </div>
      </div>`;
          lastUser = msg.user_id;
          lastTime = new Date(msg.created_at);
        });
        container.innerHTML = html;
        // Scroll management
        if (scrollAtBottom) container.scrollTop = container.scrollHeight;
        // Attach interactive button events
        document.querySelectorAll('.interactive-btn').forEach(btn => {
          btn.onclick = async () => {
            await apiFetch(`/interactive/${btn.dataset.btnId}/respond`, {
              method: 'POST'
            });
          };
        });
      }

      function formatContent(text) {
        return text
          .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
          .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
          .replace(/\*(.+?)\*/g, '<em>$1</em>')
          .replace(/`(.+?)`/g, '<code>$1</code>')
          .replace(/(https?:\/\/[^\s]+)/g, '<a href="$1" target="_blank" rel="noopener">$1</a>');
      }
      // ---------- CONTEXT MENU ----------
      window.showContextMenu = (e, msgId) => {
        e.preventDefault();
        const menu = document.getElementById('context-menu');
        menu.innerHTML = `
    <div class="context-menu-item" onclick="replyToMessage('${msgId}')">↩ Reply</div>
    <div class="context-menu-item" onclick="addToForward('${msgId}')">↪ Forward</div>
    <div class="context-menu-item" onclick="editMsg('${msgId}')">✎ Edit</div>
    <div class="context-menu-item" onclick="deleteMsg('${msgId}')">🗑 Delete</div>
    <div class="context-menu-item" onclick="copyMessageText('${msgId}')">📋 Copy</div>
    <div class="context-menu-item" onclick="pinMsg('${msgId}')">📌 Pin</div>
    <div class="context-menu-item" onclick="addReaction('${msgId}')">😀 React</div>
    <div class="context-menu-item" onclick="createTaskFromMsg('${msgId}')">✅ Task</div>
    <div class="context-menu-item" onclick="bookmarkMsg('${msgId}')">⭐ Bookmark</div>
    <div class="context-menu-item" onclick="tagMsg('${msgId}')">🏷 Tag</div>`;
        menu.style.display = 'block';
        menu.style.left = Math.min(e.clientX, window.innerWidth - 200) + 'px';
        menu.style.top = Math.min(e.clientY, window.innerHeight - 200) + 'px';
        const close = () => menu.style.display = 'none';
        document.addEventListener('click', close, {
          once: true
        });
      };
      async function replyToMessage(msgId) {
        replyToMessageId = msgId;
        toast('Reply mode activated');
      }
      async function addToForward(msgId) {
        selectedMessages.add(msgId);
        toast(`Selected, now forward to a channel`);
      }
      async function editMsg(msgId) {
        const newText = prompt('Edit message:');
        if (newText) await editMessage(msgId, newText);
      }
      async function deleteMsg(msgId) {
        await deleteMessage(msgId);
      }
      async function copyMessageText(msgId) {
        const msgs = channelMessages.get(activeChannel);
        const msg = msgs.find(m => m.id === msgId);
        if (msg) {
          await navigator.clipboard.writeText(msg.content);
          toast('Copied!');
        }
      }
      async function pinMsg(msgId) {
        await pinMessage(msgId);
        toast('Pinned');
      }
      async function addReaction(msgId) {
        const emoji = prompt('Enter emoji:');
        if (emoji) await reactToMessage(msgId, emoji);
      }
      async function createTaskFromMsg(msgId) {
        const title = prompt('Task title:');
        if (title) await apiFetch('/tasks', {
          method: 'POST',
          body: JSON.stringify({
            message_id: msgId,
            title
          })
        });
      }
      async function bookmarkMsg(msgId) {
        const folder = prompt('Bookmark folder (default: General):') || 'General';
        await apiFetch('/bookmarks', {
          method: 'POST',
          body: JSON.stringify({
            message_id: msgId,
            folder
          })
        });
      }
      async function tagMsg(msgId) {
        const tagName = prompt('Tag name:');
        if (tagName) await apiFetch(`/messages/${msgId}/tag`, {
          method: 'POST',
          body: JSON.stringify({
            tag: tagName
          })
        });
      }
      // ---------- MODALS & OVERLAYS ----------
      function openProfile() {
        /* profile editing modal */ }

      function openChannelInfo() {
        /* channel settings */ }

      function openLightbox(url) {
        const overlay = document.getElementById('lightbox-overlay');
        overlay.classList.remove('hidden');
        document.getElementById('lightbox-img').src = url;
        document.getElementById('lightbox-download').href = url;
        document.getElementById('lightbox-close').onclick = () => overlay.classList.add('hidden');
      }

      function openThread(parentId) {
        /* load thread messages in modal */ }

      function showReadReceipts(msgId) {
        /* modal with users */ }

      function showReactionDetail(msgId, emoji) {
        /* modal with user list */ }
      // ---------- VOICE NOTE ----------
      async function startVoice() {
        try {
          const stream = await navigator.mediaDevices.getUserMedia({
            audio: true
          });
          recordingMedia = new MediaRecorder(stream);
          audioChunks = [];
          recordingMedia.ondataavailable = e => audioChunks.push(e.data);
          recordingMedia.onstop = async () => {
            voiceNoteBlob = new Blob(audioChunks, {
              type: 'audio/webm'
            });
            // send as file
            const file = new File([voiceNoteBlob], 'voice.webm', {
              type: 'audio/webm'
            });
            await sendMessage('', 'audio', file);
          };
          recordingMedia.start();
          toast('Recording...');
        } catch (e) {
          toast('Microphone access denied');
        }
      }

      function stopVoice() {
        if (recordingMedia && recordingMedia.state === 'recording') recordingMedia.stop();
      }
      // ---------- DRAG AND DROP ----------
      function initDragDrop() {
        const chatArea = document.getElementById('chat-messages');
        chatArea.addEventListener('dragover', e => e.preventDefault());
        chatArea.addEventListener('drop', async e => {
          e.preventDefault();
          const files = e.dataTransfer.files;
          for (let f of files) {
            const type = f.type.startsWith('image') ? 'image' : f.type.startsWith('video') ? 'video' : f.type.startsWith('audio') ? 'audio' : 'file';
            await sendMessage(f.name, type, f);
          }
        });
      }
      // ---------- GLOBAL SEARCH ----------
      async function globalSearch(query) {
        const results = await apiFetch(`/search?q=${encodeURIComponent(query)}`);
        // display results in modal
      }
      async function searchInChannel(query) {
        const results = await apiFetch(`/channels/${activeChannel}/search?q=${query}`);
        // highlight messages
      }
      // ---------- INFINITE SCROLL ----------
      function initInfiniteScroll() {
        const container = document.getElementById('chat-messages');
        container.addEventListener('scroll', async () => {
          if (container.scrollTop === 0 && !isLoadingMore) {
            isLoadingMore = true;
            const msgs = channelMessages.get(activeChannel) || [];
            if (msgs.length > 0) {
              const oldestDate = msgs[0].created_at;
              await loadMessages(activeChannel, oldestDate);
              renderMessages();
              container.scrollTop = 50; // keep position
            }
            isLoadingMore = false;
          }
          scrollAtBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 50;
          const btn = document.getElementById('scroll-down-btn');
          if (btn) btn.classList.toggle('visible', !scrollAtBottom);
        });
      }
      // ---------- EVENT LISTENERS ----------
      function initEventListeners() {
        document.getElementById('btn-logout')?.addEventListener('click', async () => {
          await logout();
          initApp();
        });
        document.getElementById('btn-theme')?.addEventListener('click', toggleTheme);
        document.getElementById('btn-admin')?.addEventListener('click', openAdminPanel);
        document.getElementById('btn-send')?.addEventListener('click', sendTextMessage);
        document.getElementById('message-input')?.addEventListener('keydown', e => {
          if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendTextMessage();
          }
        });
        document.getElementById('message-input')?.addEventListener('input', () => {
          if (activeChannel) sendTyping(activeChannel);
        });
        document.getElementById('btn-attach')?.addEventListener('click', () => document.getElementById('file-input')?.click());
        document.getElementById('btn-voice')?.addEventListener('mousedown', startVoice);
        document.getElementById('btn-voice')?.addEventListener('mouseup', stopVoice);
        document.getElementById('btn-emoji')?.addEventListener('click', () => {
          const picker = document.createElement('div'); /* simple emoji grid */ ;
        });
        document.getElementById('scroll-down-btn')?.addEventListener('click', () => scrollToBottom(true));
        document.getElementById('mobile-back')?.addEventListener('click', () => {
          document.getElementById('chat-area').classList.remove('mobile-open');
        });
        document.getElementById('global-search')?.addEventListener('keydown', e => {
          if (e.key === 'Enter') globalSearch(e.target.value);
        });
        document.getElementById('btn-pinned')?.addEventListener('click', showPinnedMessages);
        document.getElementById('btn-media-gallery')?.addEventListener('click', showMediaGallery);
        document.getElementById('btn-search-in-chat')?.addEventListener('click', () => {
          const q = prompt('Search in chat:');
          if (q) searchInChannel(q);
        });
        document.getElementById('btn-channel-info')?.addEventListener('click', openChannelInfo);
        // Keyboard shortcut
        window.addEventListener('keydown', e => {
          if (e.ctrlKey && e.key === 'k') {
            e.preventDefault();
            document.getElementById('global-search').focus();
          }
          if (e.ctrlKey && e.key === 'f') {
            e.preventDefault();
            document.getElementById('btn-search-in-chat')?.click();
          }
        });
        // Resize sidebar
        initResize();
        initInfiniteScroll();
        initDragDrop();
        // Touch gestures
        initTouchGestures();
      }

      function sendTextMessage() {
        const input = document.getElementById('message-input');
        const content = input.value.trim();
        if (content) {
          sendMessage(content, 'text');
          input.value = '';
        }
      }

      function toggleTheme() {
        const html = document.documentElement;
        const current = html.getAttribute('data-theme');
        html.setAttribute('data-theme', current === 'dark' ? 'light' : 'dark');
      }

      function scrollToBottom(smooth) {
        const container = document.getElementById('chat-messages');
        container.scrollTo({
          top: container.scrollHeight,
          behavior: smooth ? 'smooth' : 'auto'
        });
        scrollAtBottom = true;
        document.getElementById('scroll-down-btn')?.classList.remove('visible');
      }

      function initResize() {
        const sidebar = document.getElementById('sidebar');
        const bar = document.getElementById('resize-bar');
        if (!bar) return;
        let isResizing = false;
        bar.addEventListener('mousedown', () => isResizing = true);
        window.addEventListener('mousemove', e => {
          if (!isResizing) return;
          const width = Math.min(Math.max(e.clientX, 320), 500);
          sidebar.style.width = width + 'px';
        });
        window.addEventListener('mouseup', () => isResizing = false);
      }

      function initTouchGestures() {
        let touchStartX = 0;
        const sidebar = document.getElementById('sidebar');
        sidebar.addEventListener('touchstart', e => {
          touchStartX = e.touches[0].clientX;
        });
        sidebar.addEventListener('touchend', e => {
          const diff = e.changedTouches[0].clientX - touchStartX;
          if (diff > 50) {
            /* show sidebar */ }
        });
      }
      // ---------- ADMIN ----------
      async function openAdminPanel() {
        const modal = document.getElementById('global-modal');
        modal.classList.remove('hidden');
        const data = await apiFetch('/admin/channels');
        modal.innerHTML = `<div class="modal"><h2>Admin Panel</h2>
    ${data.map(ch => `<div>${ch.name} - <button onclick="archiveChannel('${ch.id}')">Archive</button> <button onclick="forceJoin('${ch.id}')">Join</button></div>`).join('')}
    <button onclick="document.getElementById('global-modal').classList.add('hidden')">Close</button></div>`;
      }
      // ---------- PWA Registration ----------
      if ('serviceWorker' in navigator) {
        window.addEventListener('load', () => {
          navigator.serviceWorker.register('/sw.js');
        });
      }
      // ---------- START ----------
      document.addEventListener('DOMContentLoaded', initApp);
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
