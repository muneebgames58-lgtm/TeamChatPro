// ---------- GLOBALS & STATE ----------
const API = '/api';
let currentUser = null;
let activeChannel = null;
let channels = [];
let channelMessages = new Map();
let typingUsers = {};
let socket = null;
let replyToMessageId = null;
let selectedMessages = new Set();
let searchQuery = '';
let activeView = 'all';
let currentFolder = null;
let lightboxImage = null;
let recordingMedia = null;
let audioChunks = [];
let voiceNoteBlob = null;
let globalSearchTimer = null;
let scrollAtBottom = true;
let isLoadingMore = false;
let pusherKey = null;
let pusherCluster = null;

// Fetch config first
async function loadConfig() {
  const config = await apiFetch('/config');
  pusherKey = config.pusher_key;
  pusherCluster = config.pusher_cluster;
}
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
const formatDate = d => new Date(d).toLocaleDateString('en-US', { month:'short', day:'numeric', year:'numeric' });
const formatTime = d => new Date(d).toLocaleTimeString('en-US', { hour:'2-digit', minute:'2-digit' });
const formatFullTime = d => new Date(d).toLocaleString();
const generateId = () => crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).substr(2,9);

// ---------- API WRAPPERS ----------
async function apiFetch(url, options = {}) {
  const headers = { ...options.headers };
  if (!(options.body instanceof FormData)) headers['Content-Type'] = 'application/json';
  const res = await fetch(API + url, { credentials: 'include', headers, ...options });
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: 'Request failed' }));
    throw new Error(err.detail || 'Error');
  }
  return res.json();
}

// ---------- AUTH ----------
async function checkAuth() {
  try { currentUser = await apiFetch('/me'); } catch { currentUser = null; }
}
async function login(username, password) {
  await apiFetch('/login', { method: 'POST', body: JSON.stringify({ username, password }) });
  await checkAuth();
}
async function register(username, email, password) {
  await apiFetch('/register', { method: 'POST', body: JSON.stringify({ username, email, password }) });
  await checkAuth();
}
async function logout() {
  await apiFetch('/logout', { method: 'POST' });
  currentUser = null; activeChannel = null; channels = []; channelMessages.clear();
  if (socket) socket.disconnect();
}
async function changePassword(oldPwd, newPwd) {
  await apiFetch('/change-password', { method: 'POST', body: JSON.stringify({ old_password: oldPwd, new_password: newPwd }) });
}
async function updateProfile(data) {
  await apiFetch('/profile', { method: 'PUT', body: JSON.stringify(data) });
  currentUser = await apiFetch('/me');
}
async function blockUser(userId) {
  await apiFetch(`/block/${userId}`, { method: 'POST' });
}
async function unblockUser(userId) {
  await apiFetch(`/block/${userId}`, { method: 'DELETE' });
}

// ---------- CHANNELS ----------
async function loadChannels() {
  const data = await apiFetch('/channels');
  channels = data;
  renderSidebar();
}
async function createChannel(type, name, members = []) {
  await apiFetch('/channels', { method: 'POST', body: JSON.stringify({ type, name, members }) });
  await loadChannels();
}
async function leaveChannel(channelId) {
  await apiFetch(`/channels/${channelId}/leave`, { method: 'POST' });
  if (activeChannel === channelId) activeChannel = null;
  await loadChannels();
}
async function archiveChannel(channelId) {
  await apiFetch(`/channels/${channelId}/archive`, { method: 'POST' });
  await loadChannels();
}
async function pinChannel(channelId) {
  await apiFetch(`/channels/${channelId}/pin`, { method: 'POST' });
}
async function addMember(channelId, userId) {
  await apiFetch(`/channels/${channelId}/members`, { method: 'POST', body: JSON.stringify({ user_id: userId }) });
}
async function setDisappearing(channelId, ttl) {
  await apiFetch(`/channels/${channelId}/disappearing`, { method: 'POST', body: JSON.stringify({ ttl }) });
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
    channelMessages.set(channelId, [...existing, ...newMsgs].sort((a,b) => new Date(a.created_at) - new Date(b.created_at)));
  }
}
async function sendMessage(content, type = 'text', file = null) {
  if (!activeChannel) return;
  if (type === 'text') {
    await apiFetch(`/channels/${activeChannel}/messages`, { method: 'POST', body: JSON.stringify({ content, type, reply_to: replyToMessageId }) });
  } else {
    const form = new FormData();
    form.append('content', content);
    form.append('type', type);
    form.append('file', file);
    if (replyToMessageId) form.append('reply_to', replyToMessageId);
    await fetch(API + `/channels/${activeChannel}/messages`, { method: 'POST', credentials: 'include', body: form });
  }
  replyToMessageId = null;
}
async function deleteMessage(msgId) {
  await apiFetch(`/messages/${msgId}`, { method: 'DELETE' });
}
async function editMessage(msgId, content) {
  await apiFetch(`/messages/${msgId}`, { method: 'PUT', body: JSON.stringify({ content }) });
}
async function forwardMessages(channelId, messageIds) {
  await apiFetch(`/channels/${channelId}/forward`, { method: 'POST', body: JSON.stringify({ message_ids: messageIds }) });
  selectedMessages.clear();
}
async function reactToMessage(msgId, emoji) {
  await apiFetch(`/messages/${msgId}/react`, { method: 'POST', body: JSON.stringify({ emoji }) });
}
async function pinMessage(msgId) {
  await apiFetch(`/messages/${msgId}/pin`, { method: 'POST' });
}
async function markRead(channelId) {
  await apiFetch(`/channels/${channelId}/read`, { method: 'POST' });
}
async function sendTyping(channelId) {
  await apiFetch(`/channels/${channelId}/typing`, { method: 'POST' });
}

// ---------- REAL TIME (Pusher) ----------
function initPusher() {
  if (socket) socket.disconnect();
  socket = new Pusher(PUSHER_KEY, { cluster: PUSHER_CLUSTER, authEndpoint: API + '/pusher/auth', encrypted: true });
  socket.connection.bind('connected', () => { if (activeChannel) subscribeToChannel(activeChannel); });
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
    if (idx > -1) msgs[idx] = { ...msgs[idx], ...data.message };
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
    msgs.forEach(m => { if (data.message_ids.includes(m.id)) m.read_status = 'read'; });
    if (activeChannel === channelId) renderMessages();
  });
}
function updateTypingIndicator() {
  if (!activeChannel) return;
  const typing = Object.values(typingUsers[activeChannel] || {});
  const el = $('#typing-indicator');
  if (el) el.textContent = typing.length ? typing.join(', ') + ' typing…' : '';
  setTimeout(() => { delete typingUsers[activeChannel]; if (activeChannel) $('#typing-indicator').textContent = ''; }, 5000);
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
    try { await login(u, p); initApp(); } catch(e) { toast(e.message); }
  };
  document.getElementById('btn-register').onclick = async () => {
    const u = document.getElementById('auth-username').value;
    const e = document.getElementById('auth-email').value;
    const p = document.getElementById('auth-password').value;
    try { await register(u, e, p); initApp(); } catch(e) { toast(e.message); }
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
  let lastUser = null, lastTime = 0;
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
      await apiFetch(`/interactive/${btn.dataset.btnId}/respond`, { method: 'POST' });
    };
  });
}

function formatContent(text) {
  return text
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
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
  menu.style.left = Math.min(e.clientX, window.innerWidth-200) + 'px';
  menu.style.top = Math.min(e.clientY, window.innerHeight-200) + 'px';
  const close = () => menu.style.display = 'none';
  document.addEventListener('click', close, { once: true });
};

async function replyToMessage(msgId) { replyToMessageId = msgId; toast('Reply mode activated'); }
async function addToForward(msgId) { selectedMessages.add(msgId); toast(`Selected, now forward to a channel`); }
async function editMsg(msgId) {
  const newText = prompt('Edit message:');
  if (newText) await editMessage(msgId, newText);
}
async function deleteMsg(msgId) { await deleteMessage(msgId); }
async function copyMessageText(msgId) {
  const msgs = channelMessages.get(activeChannel);
  const msg = msgs.find(m => m.id === msgId);
  if (msg) { await navigator.clipboard.writeText(msg.content); toast('Copied!'); }
}
async function pinMsg(msgId) { await pinMessage(msgId); toast('Pinned'); }
async function addReaction(msgId) {
  const emoji = prompt('Enter emoji:');
  if (emoji) await reactToMessage(msgId, emoji);
}
async function createTaskFromMsg(msgId) {
  const title = prompt('Task title:');
  if (title) await apiFetch('/tasks', { method: 'POST', body: JSON.stringify({ message_id: msgId, title }) });
}
async function bookmarkMsg(msgId) {
  const folder = prompt('Bookmark folder (default: General):') || 'General';
  await apiFetch('/bookmarks', { method: 'POST', body: JSON.stringify({ message_id: msgId, folder }) });
}
async function tagMsg(msgId) {
  const tagName = prompt('Tag name:');
  if (tagName) await apiFetch(`/messages/${msgId}/tag`, { method: 'POST', body: JSON.stringify({ tag: tagName }) });
}

// ---------- MODALS & OVERLAYS ----------
function openProfile() { /* profile editing modal */ }
function openChannelInfo() { /* channel settings */ }
function openLightbox(url) {
  const overlay = document.getElementById('lightbox-overlay');
  overlay.classList.remove('hidden');
  document.getElementById('lightbox-img').src = url;
  document.getElementById('lightbox-download').href = url;
  document.getElementById('lightbox-close').onclick = () => overlay.classList.add('hidden');
}
function openThread(parentId) { /* load thread messages in modal */ }
function showReadReceipts(msgId) { /* modal with users */ }
function showReactionDetail(msgId, emoji) { /* modal with user list */ }

// ---------- VOICE NOTE ----------
async function startVoice() {
  try {
    const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
    recordingMedia = new MediaRecorder(stream);
    audioChunks = [];
    recordingMedia.ondataavailable = e => audioChunks.push(e.data);
    recordingMedia.onstop = async () => {
      voiceNoteBlob = new Blob(audioChunks, { type: 'audio/webm' });
      // send as file
      const file = new File([voiceNoteBlob], 'voice.webm', { type: 'audio/webm' });
      await sendMessage('', 'audio', file);
    };
    recordingMedia.start();
    toast('Recording...');
  } catch(e) { toast('Microphone access denied'); }
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
  document.getElementById('btn-logout')?.addEventListener('click', async () => { await logout(); initApp(); });
  document.getElementById('btn-theme')?.addEventListener('click', toggleTheme);
  document.getElementById('btn-admin')?.addEventListener('click', openAdminPanel);
  document.getElementById('btn-send')?.addEventListener('click', sendTextMessage);
  document.getElementById('message-input')?.addEventListener('keydown', e => {
    if (e.key === 'Enter' && !e.shiftKey) { e.preventDefault(); sendTextMessage(); }
  });
  document.getElementById('message-input')?.addEventListener('input', () => {
    if (activeChannel) sendTyping(activeChannel);
  });
  document.getElementById('btn-attach')?.addEventListener('click', () => document.getElementById('file-input')?.click());
  document.getElementById('btn-voice')?.addEventListener('mousedown', startVoice);
  document.getElementById('btn-voice')?.addEventListener('mouseup', stopVoice);
  document.getElementById('btn-emoji')?.addEventListener('click', () => {
    const picker = document.createElement('div'); /* simple emoji grid */;
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
    if (e.ctrlKey && e.key === 'k') { e.preventDefault(); document.getElementById('global-search').focus(); }
    if (e.ctrlKey && e.key === 'f') { e.preventDefault(); document.getElementById('btn-search-in-chat')?.click(); }
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
  container.scrollTo({ top: container.scrollHeight, behavior: smooth ? 'smooth' : 'auto' });
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
  sidebar.addEventListener('touchstart', e => { touchStartX = e.touches[0].clientX; });
  sidebar.addEventListener('touchend', e => {
    const diff = e.changedTouches[0].clientX - touchStartX;
    if (diff > 50) { /* show sidebar */ }
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
