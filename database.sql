-- Users & Auth
CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    avatar_url TEXT DEFAULT '/api/placeholder/200/200',
    bio TEXT DEFAULT '',
    phone TEXT DEFAULT '',
    status_preset TEXT DEFAULT 'online',
    custom_status_emoji TEXT DEFAULT '',
    custom_status_text TEXT DEFAULT '',
    custom_status_expiry TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Sessions (active login tracking)
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    ip TEXT,
    browser TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Blocked users
CREATE TABLE blocked_users (
    blocker_id INTEGER REFERENCES users(id),
    blocked_id INTEGER REFERENCES users(id),
    PRIMARY KEY (blocker_id, blocked_id)
);

-- Password reset tokens (optional, simplified)
CREATE TABLE password_reset_tokens (
    token TEXT PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    expires_at TIMESTAMP
);

-- Channels
CREATE TABLE channels (
    id TEXT PRIMARY KEY,  -- UUID
    name TEXT NOT NULL,
    type TEXT NOT NULL CHECK(type IN ('direct','group','announcement')),
    created_by INTEGER REFERENCES users(id),
    is_archived INTEGER DEFAULT 0,
    wallpaper TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE channel_members (
    channel_id TEXT REFERENCES channels(id),
    user_id INTEGER REFERENCES users(id),
    role TEXT DEFAULT 'member' CHECK(role IN ('admin','moderator','member')),
    starred INTEGER DEFAULT 0,
    unread_count INTEGER DEFAULT 0,
    last_read TIMESTAMP,
    PRIMARY KEY (channel_id, user_id)
);

-- Custom channel folders
CREATE TABLE folders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    name TEXT NOT NULL,
    emoji TEXT DEFAULT '📁'
);

CREATE TABLE folder_channels (
    folder_id INTEGER REFERENCES folders(id),
    channel_id TEXT REFERENCES channels(id),
    PRIMARY KEY (folder_id, channel_id)
);

-- Messages
CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    channel_id TEXT REFERENCES channels(id),
    user_id INTEGER REFERENCES users(id),
    content TEXT NOT NULL,
    type TEXT DEFAULT 'text' CHECK(type IN ('text','image','file','audio','video','system','draft','vcard','interactive')),
    reply_to TEXT REFERENCES messages(id),
    thread_parent TEXT REFERENCES messages(id),
    is_edited INTEGER DEFAULT 0,
    edited_at TIMESTAMP,
    is_deleted INTEGER DEFAULT 0,
    disappearing_ttl INTEGER,  -- seconds
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Pinned messages
CREATE TABLE message_pins (
    channel_id TEXT REFERENCES channels(id),
    message_id TEXT REFERENCES messages(id),
    pinned_by INTEGER REFERENCES users(id),
    pinned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (channel_id, message_id)
);

-- Reactions
CREATE TABLE reactions (
    message_id TEXT REFERENCES messages(id),
    user_id INTEGER REFERENCES users(id),
    emoji TEXT NOT NULL,
    PRIMARY KEY (message_id, user_id)
);

-- Typing indicators
CREATE TABLE typing_status (
    channel_id TEXT REFERENCES channels(id),
    user_id INTEGER REFERENCES users(id),
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (channel_id, user_id)
);

-- Read receipts
CREATE TABLE read_receipts (
    message_id TEXT REFERENCES messages(id),
    user_id INTEGER REFERENCES users(id),
    read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (message_id, user_id)
);

-- Bookmarks
CREATE TABLE bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    message_id TEXT REFERENCES messages(id),
    folder TEXT DEFAULT 'General',
    note TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tasks
CREATE TABLE tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    message_id TEXT REFERENCES messages(id),
    title TEXT NOT NULL,
    is_done INTEGER DEFAULT 0,
    assigned_to INTEGER REFERENCES users(id),
    due_date TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tags
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    color TEXT DEFAULT '#00a884'
);

CREATE TABLE message_tags (
    message_id TEXT REFERENCES messages(id),
    tag_id INTEGER REFERENCES tags(id),
    PRIMARY KEY (message_id, tag_id)
);

-- Drafts (channel-specific)
CREATE TABLE drafts (
    channel_id TEXT REFERENCES channels(id),
    user_id INTEGER REFERENCES users(id),
    content TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (channel_id, user_id)
);

-- Custom statuses (story-like)
CREATE TABLE custom_statuses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    type TEXT NOT NULL CHECK(type IN ('text','image','video')),
    content TEXT NOT NULL,
    emoji TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP
);

-- Contact sharing vcards
CREATE TABLE contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER REFERENCES users(id),
    vcard_data TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Interactive message buttons
CREATE TABLE interactive_buttons (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id TEXT REFERENCES messages(id),
    label TEXT NOT NULL,
    callback TEXT
);

CREATE TABLE interactive_responses (
    button_id INTEGER REFERENCES interactive_buttons(id),
    user_id INTEGER REFERENCES users(id),
    PRIMARY KEY (button_id, user_id)
);

-- Global search index (simplified, real search done via LIKE queries)
-- No additional table

-- Indexes
CREATE INDEX idx_messages_channel ON messages(channel_id, created_at);
CREATE INDEX idx_messages_thread ON messages(thread_parent);
CREATE INDEX idx_messages_content ON messages(content);