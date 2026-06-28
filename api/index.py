from fastapi import FastAPI, Request, HTTPException, Depends, UploadFile, File, Form, Cookie, Response
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from jose import jwt, JWTError
import datetime, os, uuid, base64, json
from libsql_client import create_client
import pusher

app = FastAPI()
app.mount("/public", StaticFiles(directory="public"), name="public")

# ---------- Config ----------
TURSO_URL = os.environ["TURSO_DATABASE_URL"]
TURSO_TOKEN = os.environ["TURSO_AUTH_TOKEN"]
PUSHER_APP_ID = os.environ["PUSHER_APP_ID"]
PUSHER_KEY = os.environ["PUSHER_KEY"]
PUSHER_SECRET = os.environ["PUSHER_SECRET"]
PUSHER_CLUSTER = os.environ["PUSHER_CLUSTER"]
JWT_SECRET = os.environ["JWT_SECRET"]

pusher_client = pusher.Pusher(app_id=PUSHER_APP_ID, key=PUSHER_KEY, secret=PUSHER_SECRET, cluster=PUSHER_CLUSTER, ssl=True)
db = create_client(TURSO_URL, auth_token=TURSO_TOKEN)

# ---------- Helpers ----------
async def get_current_user(request: Request):
    token = request.cookies.get("token")
    if not token: raise HTTPException(401)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        result = await db.execute("SELECT * FROM users WHERE id = ?", [payload["user_id"]])
        if not result.rows: raise HTTPException(401)
        return dict(result.rows[0])
    except JWTError: raise HTTPException(401)

def serialize_message(row):
    msg = dict(row)
    msg["created_at"] = str(msg["created_at"])
    return msg

def group_reactions(rows):
    groups = {}
    for r in rows:
        emoji = r["emoji"]
        if emoji not in groups: groups[emoji] = {"emoji": emoji, "count": 0, "users": []}
        groups[emoji]["count"] += 1
        groups[emoji]["users"].append({"user_id": r["user_id"], "username": r.get("username","")})
    return list(groups.values())

# ---------- Auth ----------
@app.post("/api/register")
async def register(data: dict):
    await db.execute("INSERT INTO users (username, email, password_hash) VALUES (?, ?, ?)",
                     [data["username"], data["email"], data["password"]])  # hash in production
    return {"ok": True}

@app.post("/api/login")
async def login(data: dict, response: JSONResponse):
    user = await db.execute("SELECT * FROM users WHERE username = ? AND password_hash = ?",
                            [data["username"], data["password"]])
    if not user.rows: raise HTTPException(401)
    user = dict(user.rows[0])
    token = jwt.encode({"user_id": user["id"], "exp": datetime.datetime.utcnow() + datetime.timedelta(days=7)}, JWT_SECRET)
    response.set_cookie("token", token, httponly=True, secure=True, samesite="lax")
    # Create session
    sess_id = str(uuid.uuid4())
    await db.execute("INSERT INTO sessions (id, user_id, ip, browser) VALUES (?, ?, ?, ?)",
                     [sess_id, user["id"], "unknown", "unknown"])
    return {"ok": True}

@app.get("/api/me")
async def me(user=Depends(get_current_user)):
    return user

@app.post("/api/logout")
async def logout(response: JSONResponse, user=Depends(get_current_user)):
    response.delete_cookie("token")
    return {"ok": True}

@app.put("/api/profile")
async def update_profile(data: dict, user=Depends(get_current_user)):
    await db.execute("UPDATE users SET bio=?, phone=?, avatar_url=?, status_preset=? WHERE id=?",
                     [data.get("bio",user["bio"]), data.get("phone",user["phone"]), data.get("avatar_url",user["avatar_url"]), data.get("status_preset",user["status_preset"]), user["id"]])
    return {"ok": True}

@app.post("/api/change-password")
async def change_password(data: dict, user=Depends(get_current_user)):
    if data["old_password"] != user["password_hash"]: raise HTTPException(400, "Wrong password")
    await db.execute("UPDATE users SET password_hash=? WHERE id=?", [data["new_password"], user["id"]])
    return {"ok": True}

@app.post("/api/block/{user_id}")
async def block_user(user_id: int, user=Depends(get_current_user)):
    await db.execute("INSERT OR IGNORE INTO blocked_users (blocker_id, blocked_id) VALUES (?,?)", [user["id"], user_id])
    return {"ok": True}

@app.delete("/api/block/{user_id}")
async def unblock_user(user_id: int, user=Depends(get_current_user)):
    await db.execute("DELETE FROM blocked_users WHERE blocker_id=? AND blocked_id=?", [user["id"], user_id])
    return {"ok": True}

# ---------- Channels ----------
@app.get("/api/channels")
async def get_channels(user=Depends(get_current_user)):
    rows = await db.execute("""
        SELECT c.*, cm.unread_count, cm.starred,
            (SELECT content FROM messages WHERE channel_id=c.id AND is_deleted=0 ORDER BY created_at DESC LIMIT 1) as last_msg
        FROM channels c JOIN channel_members cm ON c.id=cm.channel_id
        WHERE cm.user_id=? AND c.is_archived=0
        ORDER BY last_msg DESC
    """, [user["id"]])
    channels = []
    for r in rows.rows:
        ch = dict(r)
        if ch["type"] == "direct":
            other = await db.execute("SELECT u.username, u.avatar_url FROM channel_members cm JOIN users u ON cm.user_id=u.id WHERE cm.channel_id=? AND cm.user_id!=? LIMIT 1", [ch["id"], user["id"]])
            if other.rows:
                ch["name"] = other.rows[0]["username"]
                ch["avatar_url"] = other.rows[0]["avatar_url"]
        channels.append(ch)
    return channels

@app.post("/api/channels")
async def create_channel(data: dict, user=Depends(get_current_user)):
    ch_id = str(uuid.uuid4())
    await db.execute("INSERT INTO channels (id, name, type, created_by) VALUES (?,?,?,?)",
                     [ch_id, data["name"], data["type"], user["id"]])
    await db.execute("INSERT INTO channel_members (channel_id, user_id, role) VALUES (?,?,'admin')", [ch_id, user["id"]])
    for member_id in data.get("members", []):
        await db.execute("INSERT INTO channel_members (channel_id, user_id) VALUES (?,?)", [ch_id, member_id])
    return {"id": ch_id}

@app.post("/api/channels/{ch_id}/leave")
async def leave_channel(ch_id: str, user=Depends(get_current_user)):
    await db.execute("DELETE FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/archive")
async def archive_channel(ch_id: str, user=Depends(get_current_user)):
    await db.execute("UPDATE channels SET is_archived=1 WHERE id=?", [ch_id])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/pin")
async def pin_channel(ch_id: str, user=Depends(get_current_user)):
    await db.execute("UPDATE channel_members SET starred=1 WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/members")
async def add_member(ch_id: str, data: dict, user=Depends(get_current_user)):
    await db.execute("INSERT INTO channel_members (channel_id, user_id) VALUES (?,?)", [ch_id, data["user_id"]])
    return {"ok": True}

@app.post("/api/channels/{ch_id}/disappearing")
async def set_disappearing(ch_id: str, data: dict, user=Depends(get_current_user)):
    ttl = data["ttl"]  # e.g., 86400, 604800, 7776000
    await db.execute("UPDATE messages SET disappearing_ttl=? WHERE channel_id=? AND created_at > ?", [ttl, ch_id, datetime.datetime.utcnow()])
    return {"ok": True}

# ---------- Messages ----------
@app.get("/api/channels/{ch_id}/messages")
async def get_messages(ch_id: str, before: str = None, user=Depends(get_current_user)):
    await _check_membership(ch_id, user["id"])
    query = "SELECT m.*, u.username, u.avatar_url FROM messages m JOIN users u ON m.user_id=u.id WHERE m.channel_id=? AND m.is_deleted=0"
    params = [ch_id]
    if before:
        query += " AND m.created_at < ?"
        params.append(before)
    query += " ORDER BY m.created_at DESC LIMIT 50"
    rows = await db.execute(query, params)
    messages = []
    for r in reversed(rows.rows):
        msg = serialize_message(r)
        # Reactions
        reacts = await db.execute("SELECT r.emoji, r.user_id, u.username FROM reactions r JOIN users u ON r.user_id=u.id WHERE r.message_id=?", [msg["id"]])
        msg["reactions"] = group_reactions(reacts.rows)
        # Read status
        receipts = await db.execute("SELECT user_id FROM read_receipts WHERE message_id=?", [msg["id"]])
        msg["read_status"] = "read" if any(rr["user_id"] == user["id"] for rr in receipts.rows) else ("delivered" if receipts.rows else "sent")
        messages.append(msg)
    return messages

@app.post("/api/channels/{ch_id}/messages")
async def send_message(ch_id: str, content: str = Form(None), type: str = Form("text"), file: UploadFile = File(None),
                       reply_to: str = Form(None), thread_parent: str = Form(None), user=Depends(get_current_user)):
    await _check_membership(ch_id, user["id"])
    msg_id = str(uuid.uuid4())
    file_url = content or ""
    filename = ""
    if file:
        file_content = await file.read()
        file_url = f"data:{file.content_type};base64,{base64.b64encode(file_content).decode()}"
        filename = file.filename
    await db.execute("INSERT INTO messages (id, channel_id, user_id, content, type, reply_to, thread_parent) VALUES (?,?,?,?,?,?,?)",
                     [msg_id, ch_id, user["id"], file_url, type, reply_to, thread_parent])
    # Handle disappearing TTL
    ch = await db.execute("SELECT * FROM channels WHERE id=?", [ch_id])
    if ch.rows:
        # If channel has a TTL set, apply it; else can be per-message
        pass
    msg = await db.execute("SELECT m.*, u.username, u.avatar_url FROM messages m JOIN users u ON m.user_id=u.id WHERE m.id=?", [msg_id])
    msg = serialize_message(msg.rows[0])
    pusher_client.trigger(f'private-channel-{ch_id}', 'new-message', {'message': msg})
    # Update unread for others
    members = await db.execute("SELECT user_id FROM channel_members WHERE channel_id=? AND user_id!=?", [ch_id, user["id"]])
    for m in members.rows:
        await db.execute("UPDATE channel_members SET unread_count = unread_count + 1 WHERE channel_id=? AND user_id=?", [ch_id, m["user_id"]])
    return {"id": msg_id}

@app.put("/api/messages/{msg_id}")
async def edit_message(msg_id: str, data: dict, user=Depends(get_current_user)):
    msg = await db.execute("SELECT * FROM messages WHERE id=? AND user_id=?", [msg_id, user["id"]])
    if not msg.rows: raise HTTPException(403)
    await db.execute("UPDATE messages SET content=?, is_edited=1, edited_at=CURRENT_TIMESTAMP WHERE id=?", [data["content"], msg_id])
    updated = await db.execute("SELECT * FROM messages WHERE id=?", [msg_id])
    pusher_client.trigger(f'private-channel-{msg.rows[0]["channel_id"]}', 'message-updated', {'message': serialize_message(updated.rows[0])})
    return {"ok": True}

@app.delete("/api/messages/{msg_id}")
async def delete_message(msg_id: str, user=Depends(get_current_user)):
    msg = await db.execute("SELECT * FROM messages WHERE id=?", [msg_id])
    if not msg.rows: raise HTTPException(404)
    ch_id = msg.rows[0]["channel_id"]
    mem = await db.execute("SELECT role FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    if not mem.rows or (msg.rows[0]["user_id"] != user["id"] and mem.rows[0]["role"] not in ("admin","moderator")):
        raise HTTPException(403)
    await db.execute("UPDATE messages SET is_deleted=1 WHERE id=?", [msg_id])
    pusher_client.trigger(f'private-channel-{ch_id}', 'message-deleted', {'message_id': msg_id})
    return {"ok": True}

@app.post("/api/channels/{ch_id}/forward")
async def forward_messages(ch_id: str, data: dict, user=Depends(get_current_user)):
    for msg_id in data["message_ids"]:
        orig = await db.execute("SELECT * FROM messages WHERE id=?", [msg_id])
        if orig.rows:
            o = orig.rows[0]
            new_id = str(uuid.uuid4())
            await db.execute("INSERT INTO messages (id, channel_id, user_id, content, type) VALUES (?,?,?,?,?)",
                             [new_id, ch_id, user["id"], o["content"], o["type"]])
    pusher_client.trigger(f'private-channel-{ch_id}', 'refresh', {})
    return {"ok": True}

# ---------- Reactions ----------
@app.post("/api/messages/{msg_id}/react")
async def react_to_message(msg_id: str, data: dict, user=Depends(get_current_user)):
    emoji = data["emoji"]
    await db.execute("INSERT OR REPLACE INTO reactions (message_id, user_id, emoji) VALUES (?,?,?)", [msg_id, user["id"], emoji])
    msg = await db.execute("SELECT channel_id FROM messages WHERE id=?", [msg_id])
    if msg.rows:
        reacts = await db.execute("SELECT r.emoji, r.user_id, u.username FROM reactions r JOIN users u ON r.user_id=u.id WHERE r.message_id=?", [msg_id])
        reaction_data = group_reactions(reacts.rows)
        pusher_client.trigger(f'private-channel-{msg.rows[0]["channel_id"]}', 'reaction-updated', {'message_id': msg_id, 'reactions': reaction_data})
    return {"ok": True}

# ---------- Typing ----------
@app.post("/api/channels/{ch_id}/typing")
async def typing_indicator(ch_id: str, user=Depends(get_current_user)):
    await db.execute("INSERT OR REPLACE INTO typing_status (channel_id, user_id, started_at) VALUES (?,?,CURRENT_TIMESTAMP)", [ch_id, user["id"]])
    pusher_client.trigger(f'private-channel-{ch_id}', 'typing', {'user_id': user["id"], 'username': user["username"]})
    return {"ok": True}

# ---------- Read Receipts ----------
@app.post("/api/channels/{ch_id}/read")
async def mark_read(ch_id: str, user=Depends(get_current_user)):
    msgs = await db.execute("SELECT id FROM messages WHERE channel_id=? AND is_deleted=0 AND id NOT IN (SELECT message_id FROM read_receipts WHERE user_id=?)", [ch_id, user["id"]])
    if msgs.rows:
        for m in msgs.rows:
            await db.execute("INSERT OR IGNORE INTO read_receipts (message_id, user_id) VALUES (?,?)", [m["id"], user["id"]])
        message_ids = [m["id"] for m in msgs.rows]
        pusher_client.trigger(f'private-channel-{ch_id}', 'read-receipt', {'message_ids': message_ids, 'user_id': user["id"]})
    await db.execute("UPDATE channel_members SET unread_count=0 WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return {"ok": True}

# ---------- Pins ----------
@app.post("/api/messages/{msg_id}/pin")
async def pin_message(msg_id: str, user=Depends(get_current_user)):
    msg = await db.execute("SELECT channel_id FROM messages WHERE id=?", [msg_id])
    if msg.rows:
        ch_id = msg.rows[0]["channel_id"]
        await db.execute("INSERT OR IGNORE INTO message_pins (channel_id, message_id, pinned_by) VALUES (?,?,?)", [ch_id, msg_id, user["id"]])
        pusher_client.trigger(f'private-channel-{ch_id}', 'message-pinned', {'message_id': msg_id})
    return {"ok": True}

# ---------- Tasks ----------
@app.post("/api/tasks")
async def create_task(data: dict, user=Depends(get_current_user)):
    await db.execute("INSERT INTO tasks (user_id, message_id, title) VALUES (?,?,?)", [user["id"], data.get("message_id"), data["title"]])
    return {"ok": True}

@app.get("/api/tasks")
async def get_tasks(user=Depends(get_current_user)):
    tasks = await db.execute("SELECT * FROM tasks WHERE user_id=? OR assigned_to=?", [user["id"], user["id"]])
    return tasks.rows

# ---------- Bookmarks ----------
@app.post("/api/bookmarks")
async def add_bookmark(data: dict, user=Depends(get_current_user)):
    await db.execute("INSERT INTO bookmarks (user_id, message_id, folder) VALUES (?,?,?)", [user["id"], data["message_id"], data.get("folder","General")])
    return {"ok": True}

@app.get("/api/bookmarks")
async def get_bookmarks(user=Depends(get_current_user)):
    bm = await db.execute("SELECT b.*, m.content, m.channel_id FROM bookmarks b JOIN messages m ON b.message_id=m.id WHERE b.user_id=?", [user["id"]])
    return bm.rows

# ---------- Tags ----------
@app.post("/api/messages/{msg_id}/tag")
async def add_tag(msg_id: str, data: dict, user=Depends(get_current_user)):
    tag_name = data["tag"]
    tag = await db.execute("SELECT id FROM tags WHERE name=?", [tag_name])
    if not tag.rows:
        await db.execute("INSERT INTO tags (name) VALUES (?)", [tag_name])
        tag = await db.execute("SELECT id FROM tags WHERE name=?", [tag_name])
    tag_id = tag.rows[0]["id"]
    await db.execute("INSERT OR IGNORE INTO message_tags (message_id, tag_id) VALUES (?,?)", [msg_id, tag_id])
    return {"ok": True}

# ---------- Drafts ----------
@app.post("/api/channels/{ch_id}/draft")
async def save_draft(ch_id: str, data: dict, user=Depends(get_current_user)):
    await db.execute("INSERT OR REPLACE INTO drafts (channel_id, user_id, content, updated_at) VALUES (?,?,?,CURRENT_TIMESTAMP)", [ch_id, user["id"], data["content"]])
    return {"ok": True}

@app.get("/api/channels/{ch_id}/draft")
async def get_draft(ch_id: str, user=Depends(get_current_user)):
    d = await db.execute("SELECT content FROM drafts WHERE channel_id=? AND user_id=?", [ch_id, user["id"]])
    return d.rows[0] if d.rows else {"content": ""}

# ---------- Custom Statuses ----------
@app.post("/api/status")
async def set_status(data: dict, user=Depends(get_current_user)):
    await db.execute("INSERT INTO custom_statuses (user_id, type, content, emoji) VALUES (?,?,?,?)",
                     [user["id"], data.get("type","text"), data.get("content"), data.get("emoji","")])
    return {"ok": True}

@app.get("/api/statuses")
async def get_statuses(user=Depends(get_current_user)):
    st = await db.execute("SELECT cs.*, u.username FROM custom_statuses cs JOIN users u ON cs.user_id=u.id WHERE cs.expires_at > CURRENT_TIMESTAMP OR cs.expires_at IS NULL ORDER BY cs.created_at DESC")
    return st.rows

# ---------- Search ----------
@app.get("/api/search")
async def global_search(q: str, user=Depends(get_current_user)):
    results = await db.execute("SELECT m.*, u.username FROM messages m JOIN users u ON m.user_id=u.id WHERE m.content LIKE ? AND m.is_deleted=0 LIMIT 20", [f"%{q}%"])
    return results.rows

@app.get("/api/channels/{ch_id}/search")
async def channel_search(ch_id: str, q: str, user=Depends(get_current_user)):
    results = await db.execute("SELECT m.*, u.username FROM messages m JOIN users u ON m.user_id=u.id WHERE m.channel_id=? AND m.content LIKE ? AND m.is_deleted=0 LIMIT 20", [ch_id, f"%{q}%"])
    return results.rows

# ---------- Admin ----------
@app.get("/api/admin/channels")
async def admin_channels(user=Depends(get_current_user)):
    if user["role"] != "admin": raise HTTPException(403)
    chs = await db.execute("SELECT * FROM channels")
    return chs.rows

# ---------- Pusher Auth ----------
@app.post("/api/pusher/auth")
async def pusher_auth(request: Request, user=Depends(get_current_user)):
    form = await request.form()
    channel_name = form["channel_name"]
    socket_id = form["socket_id"]
    if not channel_name.startswith("private-channel-"): raise HTTPException(403)
    ch_id = channel_name.split("-", 2)[2]
    await _check_membership(ch_id, user["id"])
    auth = pusher_client.authenticate(channel=channel_name, socket_id=socket_id, custom_data={"user_id": str(user["id"])})
    return auth
@app.get("/api/placeholder/{width}/{height}")
async def placeholder(width: int, height: int):
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}"><rect width="{width}" height="{height}" fill="#ccc"/><text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" font-size="20">{width}x{height}</text></svg>'
    return Response(content=svg, media_type="image/svg+xml")
# ---------- Interactive Buttons ----------
@app.post("/api/interactive/{btn_id}/respond")
async def respond_button(btn_id: int, user=Depends(get_current_user)):
    await db.execute("INSERT OR IGNORE INTO interactive_responses (button_id, user_id) VALUES (?,?)", [btn_id, user["id"]])
    return {"ok": True}

# ---------- Membership check helper ----------
async def _check_membership(ch_id, user_id):
    mem = await db.execute("SELECT 1 FROM channel_members WHERE channel_id=? AND user_id=?", [ch_id, user_id])
    if not mem.rows: raise HTTPException(403)
