"""
Telegram File Sharing Bot with SaveFiles.com streaming.
Webhook mode only. Flask + MongoDB + SaveFiles.com.
"""

# ─────────────────────────────────────────────
# 1. Imports
# ─────────────────────────────────────────────
import asyncio
import hashlib
import io
import logging
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps

import requests
from flask import Flask, redirect, render_template_string, request
from pymongo import ASCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ─────────────────────────────────────────────
# 2. Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# 3. Environment Variables
# ─────────────────────────────────────────────
BOT_TOKEN             = os.environ["BOT_TOKEN"]
MONGO_URI             = os.environ["MONGO_URI"]
ADMIN_IDS_RAW         = os.environ.get("ADMIN_IDS", "")
BOT_USERNAME          = os.environ.get("BOT_USERNAME", "filepe_bot")
BASE_URL              = os.environ.get("BASE_URL", "https://yourapp.onrender.com").rstrip("/")
FLASK_SECRET          = os.environ.get("FLASK_SECRET", "changeme_secret_32chars_minimum!!")
SAVEFILES_API_KEY     = os.environ["SAVEFILES_API_KEY"]
UPI_ID                = os.environ.get("UPI_ID", "name@upi")
UPI_QR_URL            = os.environ.get("UPI_QR_URL", "")
SERVER_ID             = int(os.environ.get("SERVER_ID", "1"))
MAX_USERS             = int(os.environ.get("MAX_USERS", "500"))
NEXT_BOT_RAW          = os.environ.get("NEXT_BOT", "")
TOKEN_VALIDITY_HOURS  = int(os.environ.get("TOKEN_VALIDITY_HOURS", "24"))
SHORTENER_API_KEY     = os.environ.get("SHORTENER_API_KEY", "")
SHORTENER_DOMAIN      = os.environ.get("SHORTENER_DOMAIN", "api.shrtco.de")
PORT                  = int(os.environ.get("PORT", "8080"))

# ─────────────────────────────────────────────
# 4. Pre-computed Values
# ─────────────────────────────────────────────
ADMIN_IDS = set()
for _a in ADMIN_IDS_RAW.split(","):
    _a = _a.strip()
    if _a.isdigit():
        ADMIN_IDS.add(int(_a))

NEXT_BOT_NAME = ""
NEXT_BOT_LINK = ""
if NEXT_BOT_RAW and "|" in NEXT_BOT_RAW:
    NEXT_BOT_NAME = NEXT_BOT_RAW.split("|")[0].strip()
    NEXT_BOT_LINK = NEXT_BOT_RAW.split("|")[1].strip()

PREMIUM_PLANS = {
    "1month":   {"days": 30,    "price": 49,  "label": "1 Month"},
    "3months":  {"days": 90,    "price": 129, "label": "3 Months"},
    "1year":    {"days": 365,   "price": 399, "label": "1 Year"},
    "lifetime": {"days": 36500, "price": 799, "label": "Lifetime"},
}

SF_BASE       = "https://savefiles.com/api"
SF_UPLOAD_URL = "https://upload.savefiles.com/api"

# ─────────────────────────────────────────────
# 5. Active User Tracking
# ─────────────────────────────────────────────
_active_users: dict[int, float] = {}
_active_lock = threading.Lock()


def track_active_user(user_id: int):
    with _active_lock:
        _active_users[user_id] = time.time()
        cutoff = time.time() - 300
        expired = [k for k, v in _active_users.items() if v < cutoff]
        for k in expired:
            _active_users.pop(k, None)


def get_concurrent_users() -> int:
    with _active_lock:
        cutoff = time.time() - 300
        return sum(1 for v in _active_users.values() if v >= cutoff)


# ─────────────────────────────────────────────
# 6. MongoDB
# ─────────────────────────────────────────────
_mongo_client = MongoClient(
    MONGO_URI,
    maxPoolSize=50,
    minPoolSize=5,
    retryWrites=True,
    retryReads=True,
    serverSelectionTimeoutMS=5000,
)
try:
    _db = _mongo_client.get_default_database()
except Exception:
    _db = _mongo_client["filebot_db"]

users_col     = _db[f"users_bot{SERVER_ID}"]
access_col    = _db["user_access"]
savefiles_col = _db["savefiles"]
payments_col  = _db["payments"]
referrals_col = _db["referrals"]


def setup_indexes():
    users_col.create_index("user_id", unique=True)
    access_col.create_index("user_id", unique=True)
    access_col.create_index("token_expiry")
    access_col.create_index("premium_expiry")
    savefiles_col.create_index("unique_id", unique=True)
    savefiles_col.create_index("file_code")
    savefiles_col.create_index("upload_time")
    payments_col.create_index("utr")
    payments_col.create_index([("user_id", ASCENDING), ("status", ASCENDING)])
    referrals_col.create_index(
        [("referrer_id", ASCENDING), ("referred_user_id", ASCENDING)], unique=True
    )
    logger.info("MongoDB indexes ensured.")


# ─────────────────────────────────────────────
# 7. DB Helper Functions
# ─────────────────────────────────────────────
def get_user(user_id: int) -> dict | None:
    return users_col.find_one({"user_id": user_id})


def get_access(user_id: int) -> dict | None:
    return access_col.find_one({"user_id": user_id})


def is_premium(user_id: int) -> bool:
    doc = get_access(user_id)
    if not doc:
        return False
    expiry = doc.get("premium_expiry")
    if not expiry:
        return False
    if isinstance(expiry, str):
        expiry = datetime.fromisoformat(expiry)
    return expiry.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc)


def _invalidate_token_cache(user_id: int):
    with _token_cache_lock:
        _token_cache.pop(user_id, None)


def has_valid_token(user_id: int) -> bool:
    now = time.time()
    with _token_cache_lock:
        cached = _token_cache.get(user_id)
        if cached and now < cached[1]:
            return cached[0]
    if is_premium(user_id):
        result = True
    else:
        doc = get_access(user_id)
        if not doc:
            result = False
        else:
            expiry = doc.get("token_expiry")
            if not expiry:
                result = False
            else:
                if isinstance(expiry, str):
                    expiry = datetime.fromisoformat(expiry)
                result = expiry.replace(tzinfo=timezone.utc) > datetime.now(timezone.utc)
    with _token_cache_lock:
        if len(_token_cache) >= 10000:
            oldest = min(_token_cache, key=lambda k: _token_cache[k][1])
            _token_cache.pop(oldest, None)
        _token_cache[user_id] = (result, now + 60)
    return result


def grant_token(user_id: int):
    expiry = datetime.now(timezone.utc) + timedelta(hours=TOKEN_VALIDITY_HOURS)
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"token_expiry": expiry}},
        upsert=True,
    )
    _invalidate_token_cache(user_id)


def grant_premium(user_id: int, days: int):
    doc = get_access(user_id)
    now = datetime.now(timezone.utc)
    current_expiry = now
    if doc:
        pe = doc.get("premium_expiry")
        if pe:
            if isinstance(pe, str):
                pe = datetime.fromisoformat(pe)
            pe = pe.replace(tzinfo=timezone.utc)
            if pe > now:
                current_expiry = pe
    new_expiry = current_expiry + timedelta(days=days)
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": new_expiry}},
        upsert=True,
    )
    _invalidate_token_cache(user_id)


def revoke_premium(user_id: int):
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": datetime.now(timezone.utc) - timedelta(days=1)}},
        upsert=True,
    )
    _invalidate_token_cache(user_id)


def save_savefiles_file(data: dict) -> str:
    """Save file to MongoDB and return unique_id."""
    unique_id = data["unique_id"]
    savefiles_col.update_one(
        {"unique_id": unique_id},
        {"$set": data},
        upsert=True,
    )
    with _file_cache_lock:
        _file_cache.pop(unique_id, None)
    return unique_id


def get_savefiles_file(unique_id: str) -> dict | None:
    return savefiles_col.find_one({"unique_id": unique_id})


def get_savefiles_file_cached(unique_id: str) -> dict | None:
    now = time.time()
    with _file_cache_lock:
        cached = _file_cache.get(unique_id)
        if cached and now - cached[1] < 300:
            return cached[0]
    doc = savefiles_col.find_one({"unique_id": unique_id})
    if doc:
        with _file_cache_lock:
            _file_cache[unique_id] = (doc, now)
    return doc


def increment_savefiles_views(unique_id: str):
    savefiles_col.update_one(
        {"unique_id": unique_id},
        {"$inc": {"view_count": 1}},
    )
    with _file_cache_lock:
        _file_cache.pop(unique_id, None)


def make_savefiles_deep_link(unique_id: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=sf_{unique_id}"


def make_watch_url(user_id: int, unique_id: str) -> str:
    t = make_sf_token(user_id, unique_id)
    return f"{BASE_URL}/sf/watch?id={unique_id}&u={user_id}&t={t}"


def record_referral(referrer_id: int, referred_user_id: int) -> bool:
    try:
        referrals_col.insert_one({
            "referrer_id": referrer_id,
            "referred_user_id": referred_user_id,
            "timestamp": datetime.now(timezone.utc),
        })
        users_col.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referrals_count": 1}},
        )
        return True
    except DuplicateKeyError:
        return False


def check_referral_rewards(referrer_id: int) -> str | None:
    doc = users_col.find_one({"user_id": referrer_id})
    if not doc:
        return None
    count = doc.get("referrals_count", 0)
    if count > 0 and count % 10 == 0:
        grant_premium(referrer_id, 7)
        return f"🎁 Milestone: {count} referrals → 7-day premium granted!"
    if count > 0 and count % 5 == 0:
        grant_token(referrer_id)
        return f"⚡ Milestone: {count} referrals → bonus token hours granted!"
    return None


def is_bot_full() -> bool:
    return users_col.count_documents({}) >= MAX_USERS


# ─────────────────────────────────────────────
# 8. In-Memory Caches
# ─────────────────────────────────────────────
_token_cache: dict[int, tuple[bool, float]] = {}
_token_cache_lock = threading.Lock()

_file_cache: dict[str, tuple[dict, float]] = {}
_file_cache_lock = threading.Lock()

# ─────────────────────────────────────────────
# 9. SaveFiles API
# ─────────────────────────────────────────────
_sf_session = requests.Session()
_sf_adapter = requests.adapters.HTTPAdapter(
    pool_connections=20, pool_maxsize=20, max_retries=1
)
_sf_session.mount("https://", _sf_adapter)


def upload_to_savefiles(file_bytes: bytes, filename: str, max_retries: int = 3) -> dict | None:
    for attempt in range(1, max_retries + 1):
        try:
            resp = _sf_session.post(
                f"{SF_UPLOAD_URL}/file/upload",
                data={"key": SAVEFILES_API_KEY},
                files={"file": (filename, io.BytesIO(file_bytes), "application/octet-stream")},
                timeout=600,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == 200 or data.get("msg") == "OK":
                    result = data.get("result", data)
                    file_code = result.get("filecode") or result.get("file_code")
                    url = result.get("url") or f"https://savefiles.com/{file_code}"
                    return {
                        "file_code": file_code,
                        "url": url,
                        "stream_url": f"https://xvs.tt/{file_code}",
                    }
        except Exception as e:
            logger.error(f"SaveFiles upload attempt {attempt} failed: {e}")
        time.sleep(3 * attempt)
    return None


def savefiles_file_info(file_code: str) -> dict | None:
    try:
        resp = _sf_session.get(
            f"{SF_BASE}/file/info",
            params={"key": SAVEFILES_API_KEY, "file_code": file_code},
            timeout=15,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("status") == 200:
                return data.get("result", {})
    except Exception as e:
        logger.error(f"savefiles_file_info error: {e}")
    return None


def check_savefiles(file_code: str) -> bool:
    return bool(savefiles_file_info(file_code))


# ─────────────────────────────────────────────
# 10. URL Shortener
# ─────────────────────────────────────────────
def shorten_url(long_url: str) -> str:
    try:
        if SHORTENER_API_KEY and SHORTENER_DOMAIN != "api.shrtco.de":
            resp = requests.get(
                f"https://{SHORTENER_DOMAIN}/api",
                params={"api": SHORTENER_API_KEY, "url": long_url},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("shortenedUrl") or data.get("short_link") or long_url
        else:
            resp = requests.get(
                f"https://api.shrtco.de/v2/shorten",
                params={"url": long_url},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("result", {}).get("full_short_link", long_url)
    except Exception as e:
        logger.warning(f"URL shortener failed: {e}")
    return long_url


# ─────────────────────────────────────────────
# 11. Admin Decorator
# ─────────────────────────────────────────────
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not user or user.id not in ADMIN_IDS:
            await update.message.reply_text(
                "🚫 <b>Access Denied</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ ❌ Admin only command.",
                parse_mode="HTML",
            )
            return
        return await func(update, context)
    return wrapper


# ─────────────────────────────────────────────
# 12. Flask App + WATCH_HTML
# ─────────────────────────────────────────────
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET

WATCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ file_name }} — FileBot</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
:root {
  --bg: #0a0a0f;
  --card: #1a1a27;
  --accent: #6c63ff;
  --accent2: #ff6584;
  --text: #e8e8f0;
  --muted: #6b6b80;
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: 'Inter', sans-serif;
  background: var(--bg);
  color: var(--text);
  min-height: 100vh;
  position: relative;
  overflow-x: hidden;
}
body::before {
  content: '';
  position: fixed;
  inset: 0;
  background:
    radial-gradient(ellipse at 0% 0%, rgba(108,99,255,0.15) 0%, transparent 60%),
    radial-gradient(ellipse at 100% 100%, rgba(255,101,132,0.12) 0%, transparent 60%);
  pointer-events: none;
  z-index: 0;
}
.container { max-width: 860px; margin: 0 auto; padding: 20px 16px; position: relative; z-index: 1; }
.topbar {
  display: flex; align-items: center; justify-content: space-between;
  padding: 12px 0 20px;
}
.logo {
  font-size: 1.3rem; font-weight: 700;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
}
.badge {
  font-size: 0.7rem; font-weight: 600; letter-spacing: 0.08em;
  padding: 4px 12px; border-radius: 20px;
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: #fff;
}
.player-card {
  background: var(--card);
  border-radius: 20px;
  overflow: hidden;
  box-shadow: 0 0 40px rgba(108,99,255,0.18), 0 8px 32px rgba(0,0,0,0.5);
  margin-bottom: 16px;
}
.video-wrapper { position: relative; background: #000; }
video { width: 100%; display: block; max-height: 480px; background: #000; }
.accent-bar {
  height: 4px;
  background: linear-gradient(90deg, var(--accent), var(--accent2));
}
.spinner-overlay {
  display: none;
  position: absolute; inset: 0;
  background: rgba(10,10,15,0.7);
  align-items: center; justify-content: center;
  border-radius: 0;
}
.spinner-overlay.active { display: flex; }
.spinner {
  width: 44px; height: 44px;
  border: 3px solid rgba(108,99,255,0.3);
  border-top-color: var(--accent);
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}
@keyframes spin { to { transform: rotate(360deg); } }
.stats-bar {
  background: rgba(10,10,15,0.6);
  padding: 10px 16px;
  display: flex; gap: 16px; flex-wrap: wrap; align-items: center;
  font-size: 0.75rem; color: var(--muted);
  border-top: 1px solid rgba(255,255,255,0.05);
}
.stat { display: flex; align-items: center; gap: 5px; }
.stat strong { color: var(--text); }
.file-title { padding: 14px 16px 10px; font-weight: 600; font-size: 1rem; color: var(--text); }
.actions {
  display: flex; gap: 10px; padding: 0 16px 16px; flex-wrap: wrap;
}
.btn {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 9px 18px; border-radius: 10px;
  font-size: 0.85rem; font-weight: 600; cursor: pointer;
  border: none; text-decoration: none; transition: opacity 0.2s, transform 0.1s;
}
.btn:active { transform: scale(0.97); }
.btn-primary {
  background: linear-gradient(135deg, var(--accent), var(--accent2));
  color: #fff;
}
.btn-ghost {
  background: rgba(255,255,255,0.06);
  color: var(--text);
  border: 1px solid rgba(255,255,255,0.1);
}
.btn:hover { opacity: 0.88; }
.toast {
  position: fixed; bottom: 24px; left: 50%; transform: translateX(-50%) translateY(60px);
  background: var(--card); color: var(--text);
  padding: 10px 20px; border-radius: 10px;
  border: 1px solid rgba(108,99,255,0.4);
  font-size: 0.85rem; opacity: 0;
  transition: all 0.35s ease; z-index: 100;
}
.toast.show { opacity: 1; transform: translateX(-50%) translateY(0); }
@media (max-width: 600px) {
  .topbar { flex-direction: column; gap: 10px; }
  .stats-bar { gap: 10px; }
}
</style>
</head>
<body>
<div class="container">
  <div class="topbar">
    <div class="logo">🎬 FileBot</div>
    <div class="badge">PREMIUM STREAM</div>
  </div>
  <div class="player-card">
    <div class="video-wrapper">
      <video id="vid" controls preload="metadata" src="{{ video_url }}"></video>
      <div class="spinner-overlay" id="spinner"><div class="spinner"></div></div>
      <div class="accent-bar"></div>
    </div>
    <div class="file-title" id="fname">{{ file_name }}</div>
    <div class="stats-bar">
      <div class="stat">📡 <strong id="status">Connecting…</strong></div>
      <div class="stat">⏱ <strong id="dur">--:--</strong></div>
      <div class="stat">🖥 <strong id="res">--</strong></div>
      <div class="stat">📦 <strong id="buf">0%</strong> buffered</div>
      <div class="stat">👁 <strong>{{ view_count }}</strong> views</div>
    </div>
    <div class="actions">
      <a class="btn btn-primary" id="dlbtn" href="{{ video_url }}" download="{{ file_name }}">⬇️ Download</a>
      <button class="btn btn-ghost" onclick="toggleFullscreen()">⛶ Fullscreen</button>
      <button class="btn btn-ghost" onclick="copyLink()">🔗 Copy Link</button>
    </div>
  </div>
</div>
<div class="toast" id="toast"></div>
<script>
const vid = document.getElementById('vid');
const spinner = document.getElementById('spinner');
const statusEl = document.getElementById('status');
const durEl = document.getElementById('dur');
const resEl = document.getElementById('res');
const bufEl = document.getElementById('buf');

function fmt(s) {
  const m = Math.floor(s / 60), ss = Math.floor(s % 60);
  return m + ':' + String(ss).padStart(2,'0');
}
vid.addEventListener('waiting', () => { spinner.classList.add('active'); statusEl.textContent = 'Buffering…'; });
vid.addEventListener('canplay', () => { spinner.classList.remove('active'); });
vid.addEventListener('playing', () => { statusEl.textContent = 'Playing'; spinner.classList.remove('active'); });
vid.addEventListener('pause', () => { statusEl.textContent = 'Paused'; });
vid.addEventListener('ended', () => { statusEl.textContent = 'Ended'; });
vid.addEventListener('loadedmetadata', () => {
  durEl.textContent = fmt(vid.duration);
  if (vid.videoWidth) resEl.textContent = vid.videoWidth + '×' + vid.videoHeight;
  statusEl.textContent = 'Ready';
});
vid.addEventListener('timeupdate', () => { durEl.textContent = fmt(vid.duration - vid.currentTime) + ' left'; });
setInterval(() => {
  if (vid.buffered.length > 0 && vid.duration > 0) {
    bufEl.textContent = Math.round(vid.buffered.end(vid.buffered.length - 1) / vid.duration * 100) + '%';
  }
}, 1000);

document.addEventListener('keydown', e => {
  if (['INPUT','TEXTAREA'].includes(e.target.tagName)) return;
  if (e.code === 'Space') { e.preventDefault(); vid.paused ? vid.play() : vid.pause(); }
  else if (e.key === 'ArrowRight') vid.currentTime = Math.min(vid.duration, vid.currentTime + 10);
  else if (e.key === 'ArrowLeft') vid.currentTime = Math.max(0, vid.currentTime - 10);
  else if (e.key === 'ArrowUp') { e.preventDefault(); vid.volume = Math.min(1, vid.volume + 0.1); }
  else if (e.key === 'ArrowDown') { e.preventDefault(); vid.volume = Math.max(0, vid.volume - 0.1); }
  else if (e.key.toLowerCase() === 'f') toggleFullscreen();
});

function toggleFullscreen() {
  if (!document.fullscreenElement) vid.requestFullscreen();
  else document.exitFullscreen();
}

function copyLink() {
  navigator.clipboard.writeText(window.location.href).then(() => showToast('🔗 Link copied!'));
}

function showToast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}
</script>
</body>
</html>"""

ACCESS_DENIED_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Access Denied</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600&display=swap" rel="stylesheet">
<style>
body { font-family: Inter, sans-serif; background: #0a0a0f; color: #e8e8f0;
  display: flex; align-items: center; justify-content: center; min-height: 100vh; }
.box { text-align: center; padding: 40px; background: #1a1a27; border-radius: 20px;
  box-shadow: 0 0 40px rgba(108,99,255,0.15); max-width: 400px; }
.icon { font-size: 3rem; margin-bottom: 16px; }
h2 { font-size: 1.3rem; margin-bottom: 8px; }
p { color: #6b6b80; font-size: 0.9rem; }
</style>
</head>
<body>
<div class="box">
  <div class="icon">🔒</div>
  <h2>Access Denied</h2>
  <p>{{ reason }}</p>
</div>
</body>
</html>"""

INDEX_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>FileBot — Telegram File Sharing</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap" rel="stylesheet">
<style>
body { font-family: Inter, sans-serif; background: #0a0a0f; color: #e8e8f0;
  display: flex; align-items: center; justify-content: center; min-height: 100vh; }
.card { text-align: center; padding: 48px 40px; background: #1a1a27;
  border-radius: 24px; max-width: 420px; width: 90%;
  box-shadow: 0 0 60px rgba(108,99,255,0.18); }
h1 { font-size: 1.8rem; margin: 12px 0 8px;
  background: linear-gradient(135deg, #6c63ff, #ff6584);
  -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
p { color: #6b6b80; margin-bottom: 20px; }
a.btn { display: inline-block; padding: 11px 28px; border-radius: 12px;
  background: linear-gradient(135deg, #6c63ff, #ff6584);
  color: #fff; text-decoration: none; font-weight: 600; }
</style>
</head>
<body>
<div class="card">
  <div style="font-size:3rem">🤖</div>
  <h1>FileBot</h1>
  <p>Server {{ server_id }} &nbsp;•&nbsp; Premium Telegram File Streaming</p>
  <a class="btn" href="https://t.me/{{ bot_username }}">Open Bot</a>
</div>
</body>
</html>"""

# ─────────────────────────────────────────────
# 13. HMAC Token Functions
# ─────────────────────────────────────────────
def make_sf_token(user_id: int, unique_id: str) -> str:
    return hashlib.sha256(
        f"sf:{user_id}:{unique_id}:{FLASK_SECRET}".encode()
    ).hexdigest()[:16]


def verify_sf_access(unique_id: str):
    token = request.args.get("t", "")
    user_id_raw = request.args.get("u", "")
    if not user_id_raw:
        return None, "Missing auth"
    try:
        uid = int(user_id_raw)
    except ValueError:
        return None, "Invalid user"
    expected = make_sf_token(uid, unique_id)
    if token != expected:
        return None, "Invalid token"
    if not has_valid_token(uid):
        return None, "No valid token — please verify access via the bot"
    sf_file = get_savefiles_file_cached(unique_id)
    if not sf_file:
        return None, "File not found"
    return sf_file, None


# ─────────────────────────────────────────────
# 14. Flask Routes
# ─────────────────────────────────────────────
@flask_app.route("/")
def index():
    return render_template_string(
        INDEX_HTML, server_id=SERVER_ID, bot_username=BOT_USERNAME
    )


@flask_app.route("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json(force=True, silent=True)
    if data:
        t = threading.Thread(target=process_update_sync, args=(data,), daemon=True)
        t.start()
    return "OK", 200


@flask_app.route("/sf/watch")
def sf_watch():
    unique_id = request.args.get("id", "")
    sf_file, err = verify_sf_access(unique_id)
    if err:
        return render_template_string(ACCESS_DENIED_HTML, reason=err), 403

    file_code = sf_file.get("file_code", "")
    direct_url = (
        f"https://savefiles.com/api/file/direct"
        f"?key={SAVEFILES_API_KEY}&file_code={file_code}"
    )
    increment_savefiles_views(unique_id)
    uid_raw = request.args.get("u", "0")
    try:
        track_active_user(int(uid_raw))
    except ValueError:
        pass

    return render_template_string(
        WATCH_HTML,
        file_name=sf_file.get("file_name", "Video"),
        video_url=direct_url,
        view_count=sf_file.get("view_count", 0) + 1,
    )


@flask_app.route("/verify/<int:user_id>")
def verify_redirect(user_id):
    return redirect(f"https://t.me/{BOT_USERNAME}?start=verify_{user_id}")


# ─────────────────────────────────────────────
# 15. Telegram Handlers
# ─────────────────────────────────────────────
_bot_application: Application | None = None


def _ensure_user(user) -> bool:
    """Register user if new. Returns True if new."""
    existing = get_user(user.id)
    if existing:
        return False
    try:
        users_col.insert_one({
            "user_id": user.id,
            "username": user.username or "",
            "first_name": user.first_name or "",
            "referrals_count": 0,
            "referred_by": None,
            "joined_at": datetime.now(timezone.utc),
        })
        return True
    except DuplicateKeyError:
        return False


async def send_welcome(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    short_ref = shorten_url(ref_link)
    doc = get_user(user.id)
    ref_count = doc.get("referrals_count", 0) if doc else 0
    concurrent = get_concurrent_users()
    total = users_col.count_documents({})
    msg = (
        f"👋 <b>Welcome, {user.first_name}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎬 <b>File Sharing Bot</b> — Server {SERVER_ID}\n\n"
        f"📌 <b>Commands:</b>\n"
        f"┣ 💎 /premium — Get premium access\n"
        f"┣ 👥 /referral — Your referral link\n"
        f"┗ 📊 /mystatus — Token &amp; premium status\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{short_ref}</code>\n\n"
        f"🎁 Refer friends &amp; earn free premium!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Server {SERVER_ID}  •  👤 {total}/{MAX_USERS}  •  ⚡ {concurrent} active"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def send_redirect_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_id: str = ""):
    if file_id and NEXT_BOT_NAME:
        deep_link = f"https://t.me/{NEXT_BOT_NAME}?start=sf_{file_id}"
        kb = [[InlineKeyboardButton("▶️ Get File Now", url=deep_link)]]
    elif NEXT_BOT_LINK:
        kb = [[InlineKeyboardButton("➡️ Join Sister Bot", url=NEXT_BOT_LINK)]]
    else:
        await update.message.reply_text(
            "⚠️ <b>Bot Full</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ This bot is at capacity. Please try again later.",
            parse_mode="HTML",
        )
        return
    msg = (
        f"🤖 <b>Server {SERVER_ID} is Full</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ <b>Max users reached!</b>\n\n"
        f"┗ Please join our sister bot to continue.\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👇 Tap below to proceed:"
    )
    await update.message.reply_text(msg, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb))


async def handle_savefiles_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, unique_id: str):
    user = update.effective_user
    sf_file = get_savefiles_file_cached(unique_id)
    if not sf_file:
        await update.message.reply_text(
            "❌ <b>File Not Found</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ This file does not exist or has been removed.",
            parse_mode="HTML",
        )
        return

    if not has_valid_token(user.id):
        verify_url = f"{BASE_URL}/verify/{user.id}"
        short_url = shorten_url(verify_url)
        msg = (
            f"🔐 <b>Access Required</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚡ <b>You need to verify to access files.</b>\n\n"
            f"┣ 1️⃣ Click the link below\n"
            f"┣ 2️⃣ Complete the verification\n"
            f"┗ 3️⃣ Come back &amp; try again\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 <b>Verify Link:</b> <a href='{short_url}'>Tap Here</a>\n"
            f"⏳ Token valid for {TOKEN_VALIDITY_HOURS}h after verification"
        )
        await update.message.reply_text(msg, parse_mode="HTML", disable_web_page_preview=True)
        return

    size_mb = round(sf_file.get("file_size", 0) / 1024 / 1024, 2)
    watch_url = make_watch_url(user.id, unique_id)
    kb = [[InlineKeyboardButton("▶️ Watch Now", url=watch_url)]]
    msg = (
        f"🎬 <b>{sf_file.get('file_name', 'Video')}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"✅ File ready to stream!\n\n"
        f"📦 Size: {size_mb} MB\n"
        f"👁️ Views: {sf_file.get('view_count', 0)}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👇 Click to watch:"
    )
    await update.message.reply_text(
        msg, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb)
    )


async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    track_active_user(user.id)
    is_new = _ensure_user(user)
    payload = ctx.args[0] if ctx.args else ""

    if payload.startswith("sf_"):
        unique_id = payload[3:]
        if is_new and is_bot_full() and NEXT_BOT_LINK:
            await send_redirect_message(update, ctx, unique_id)
            return
        await handle_savefiles_request(update, ctx, unique_id)
        return

    if payload.startswith("ref_"):
        try:
            referrer_id = int(payload[4:])
            if referrer_id != user.id:
                if record_referral(referrer_id, user.id):
                    reward_msg = check_referral_rewards(referrer_id)
                    if reward_msg and _bot_application:
                        try:
                            await _bot_application.bot.send_message(
                                chat_id=referrer_id,
                                text=(
                                    f"🎉 <b>New Referral!</b>\n"
                                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                                    f"┣ 👤 {user.first_name} joined via your link\n"
                                    f"┗ {reward_msg if reward_msg else '⚡ Keep referring to earn rewards!'}"
                                ),
                                parse_mode="HTML",
                            )
                        except Exception:
                            pass
        except (ValueError, Exception):
            pass
        await send_welcome(update, ctx)
        return

    if payload.startswith("verify_"):
        try:
            uid = int(payload[7:])
            if uid == user.id:
                grant_token(user.id)
                await update.message.reply_text(
                    f"✅ <b>Access Granted!</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"🎉 Token activated successfully!\n\n"
                    f"┗ ⏳ Valid for {TOKEN_VALIDITY_HOURS} hours\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"📁 You can now access your files.",
                    parse_mode="HTML",
                )
                return
        except ValueError:
            pass

    if is_new and is_bot_full() and NEXT_BOT_LINK:
        await send_redirect_message(update, ctx)
        return

    await send_welcome(update, ctx)


async def mystatus_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    track_active_user(user.id)
    doc_access = get_access(user.id)
    doc_user = get_user(user.id)

    # Token status
    token_line = "┗ ❌ No token"
    if doc_access:
        te = doc_access.get("token_expiry")
        if te:
            if isinstance(te, str):
                te = datetime.fromisoformat(te)
            te = te.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if te > now:
                diff = te - now
                h = int(diff.total_seconds() // 3600)
                m = int((diff.total_seconds() % 3600) // 60)
                token_line = f"┗ ✅ Valid  •  ⏳ {h}h {m}m left"

    # Premium status
    premium_line = "┗ ❌ Not Active"
    if doc_access:
        pe = doc_access.get("premium_expiry")
        if pe:
            if isinstance(pe, str):
                pe = datetime.fromisoformat(pe)
            pe = pe.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if pe > now:
                diff = pe - now
                d = diff.days
                premium_line = f"┗ ✅ Active  •  📅 {d} days left"

    ref_count = doc_user.get("referrals_count", 0) if doc_user else 0
    joined = doc_user.get("joined_at", "") if doc_user else ""
    joined_str = ""
    if joined:
        if isinstance(joined, str):
            joined = datetime.fromisoformat(joined)
        joined_str = joined.strftime("%d %b %Y")

    msg = (
        f"📊 <b>Your Account Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔐 <b>Token Access</b>\n"
        f"{token_line}\n\n"
        f"💎 <b>Premium</b>\n"
        f"{premium_line}\n\n"
        f"👥 <b>Referrals</b>\n"
        f"┗ {ref_count} friends referred\n\n"
        f"📅 <b>Joined</b>\n"
        f"┗ {joined_str or 'Unknown'}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Server {SERVER_ID}"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def referral_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    track_active_user(user.id)
    doc = get_user(user.id)
    ref_count = doc.get("referrals_count", 0) if doc else 0
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    short_ref = shorten_url(ref_link)
    next_5 = 5 - (ref_count % 5)
    next_10 = 10 - ref_count if ref_count < 10 else "✅ Reached"
    msg = (
        f"👥 <b>Your Referral Program</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 <b>Stats:</b>\n"
        f"┣ 👤 Total Referrals: {ref_count}\n"
        f"┣ ⚡ Next bonus in: {next_5} referrals\n"
        f"┗ 💎 10-ref premium in: {next_10}\n\n"
        f"🎁 <b>Rewards:</b>\n"
        f"┣ Every 5 → bonus token hours\n"
        f"┗ 10 referrals → 7-day FREE premium\n\n"
        f"🔗 <b>Your Link:</b>\n"
        f"<code>{short_ref}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Share this link to earn rewards!"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def premium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_premium_menu(update, ctx)


async def show_premium_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    track_active_user(user.id)
    lines = []
    kb_rows = []
    for key, plan in PREMIUM_PLANS.items():
        lines.append(f"┣ {plan['label']}: ₹{plan['price']}")
        kb_rows.append([InlineKeyboardButton(
            f"💎 {plan['label']} — ₹{plan['price']}",
            callback_data=f"plan_{key}"
        )])
    lines[-1] = lines[-1].replace("┣", "┗")
    plans_text = "\n".join(lines)
    msg = (
        f"💎 <b>Premium Plans</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⚡ <b>Benefits:</b>\n"
        f"┣ ✅ No token verification needed\n"
        f"┣ 🚀 Priority streaming access\n"
        f"┗ 🎁 Support the project\n\n"
        f"💳 <b>Plans:</b>\n"
        f"{plans_text}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👇 Select a plan:"
    )
    if isinstance(update, Update) and update.message:
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb_rows))
    elif isinstance(update, Update) and update.callback_query:
        await update.callback_query.edit_message_text(msg, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(kb_rows))


async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""
    user = query.from_user

    if data.startswith("plan_"):
        plan_key = data[5:]
        plan = PREMIUM_PLANS.get(plan_key)
        if not plan:
            return
        upi_line = f"<code>{UPI_ID}</code>"
        qr_line = f"\n🖼 QR Code: {UPI_QR_URL}" if UPI_QR_URL else ""
        msg = (
            f"💎 <b>{plan['label']} Premium — ₹{plan['price']}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💳 <b>Payment Details:</b>\n"
            f"┣ UPI ID: {upi_line}{qr_line}\n"
            f"┗ Amount: ₹{plan['price']}\n\n"
            f"📝 <b>After Payment:</b>\n"
            f"┣ 1️⃣ Note your UTR/Transaction ID\n"
            f"┗ 2️⃣ Send: <code>/utr YOUR_UTR_HERE</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Activated within minutes of approval"
        )
        await query.edit_message_text(msg, parse_mode="HTML")


async def utr_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return
    track_active_user(user.id)
    args = ctx.args
    if not args:
        await update.message.reply_text(
            "❌ <b>Usage:</b> <code>/utr YOUR_UTR_NUMBER</code>",
            parse_mode="HTML",
        )
        return
    utr = args[0].strip()
    if len(utr) < 6:
        await update.message.reply_text(
            "❌ <b>Invalid UTR</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ UTR must be at least 6 characters.",
            parse_mode="HTML",
        )
        return

    existing = payments_col.find_one({"utr": utr})
    if existing:
        await update.message.reply_text(
            "❌ <b>Duplicate UTR</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ This UTR has already been submitted.",
            parse_mode="HTML",
        )
        return

    payments_col.insert_one({
        "user_id": user.id,
        "username": user.username or "",
        "utr": utr,
        "plan": "unknown",
        "price": 0,
        "status": "pending",
        "created_at": datetime.now(timezone.utc),
    })

    await update.message.reply_text(
        f"✅ <b>UTR Submitted</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"┣ 🔢 UTR: <code>{utr}</code>\n"
        f"┗ 📋 Status: Pending review\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Admin will verify shortly.",
        parse_mode="HTML",
    )

    for admin_id in ADMIN_IDS:
        try:
            await _bot_application.bot.send_message(
                chat_id=admin_id,
                text=(
                    f"💳 <b>New Payment Submitted</b>\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"┣ 👤 User: {user.first_name} (ID: <code>{user.id}</code>)\n"
                    f"┣ 🔢 UTR: <code>{utr}</code>\n"
                    f"┗ 📋 Status: Pending\n\n"
                    f"━━━━━━━━━━━━━━━━━━━━━\n"
                    f"Use /approve {user.id} or /reject {user.id}"
                ),
                parse_mode="HTML",
            )
        except Exception:
            pass


async def handle_savefiles_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin uploads a file → bot downloads → uploads to SaveFiles → saves to DB."""
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return

    msg = update.message
    file_obj = None
    file_name = "file"
    file_size = 0

    if msg.document:
        file_obj = msg.document
        file_name = file_obj.file_name or "document"
        file_size = file_obj.file_size or 0
    elif msg.video:
        file_obj = msg.video
        file_name = f"video_{int(time.time())}.mp4"
        file_size = file_obj.file_size or 0
    elif msg.audio:
        file_obj = msg.audio
        file_name = file_obj.file_name or f"audio_{int(time.time())}.mp3"
        file_size = file_obj.file_size or 0
    else:
        return

    MAX_BYTES = 20 * 1024 * 1024
    if file_size > MAX_BYTES:
        size_mb = round(file_size / 1024 / 1024, 2)
        await msg.reply_text(
            f"⚠️ <b>File Too Large</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"┣ 📦 File size: {size_mb} MB\n"
            f"┗ 🔒 Bot limit: 20 MB\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Options:</b>\n"
            f"┣ 1️⃣ Upload to savefiles.com manually\n"
            f"┗ 2️⃣ Use: <code>/sfadd FILE_CODE {file_name}</code>",
            parse_mode="HTML",
        )
        return

    wait_msg = await msg.reply_text("⏳ Uploading to SaveFiles.com…")

    try:
        loop = asyncio.get_event_loop()
        tg_file = await file_obj.get_file()
        file_bytes = await loop.run_in_executor(
            None,
            lambda: requests.get(tg_file.file_path, timeout=120).content,
        )

        result = await loop.run_in_executor(
            None, lambda: upload_to_savefiles(file_bytes, file_name)
        )

        if not result:
            await wait_msg.edit_text(
                "❌ <b>Upload Failed</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ SaveFiles.com upload failed. Try again.",
                parse_mode="HTML",
            )
            return

        unique_id = hashlib.md5(f"{result['file_code']}{time.time()}".encode()).hexdigest()
        file_type = "video" if msg.video else ("audio" if msg.audio else "document")

        save_savefiles_file({
            "unique_id": unique_id,
            "file_code": result["file_code"],
            "stream_url": result["stream_url"],
            "file_name": file_name,
            "file_size": file_size,
            "file_type": file_type,
            "telegram_file_id": file_obj.file_id,
            "upload_time": datetime.now(timezone.utc),
            "view_count": 0,
            "last_checked": datetime.now(timezone.utc),
        })

        deep_link = make_savefiles_deep_link(unique_id)
        await wait_msg.edit_text(
            f"✅ <b>Uploaded Successfully!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"┣ 📁 File: {file_name}\n"
            f"┣ 🔑 Code: <code>{result['file_code']}</code>\n"
            f"┗ 🆔 ID: <code>{unique_id}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🔗 <b>Deep Link:</b>\n<code>{deep_link}</code>",
            parse_mode="HTML",
        )
        try:
            await msg.delete()
        except Exception:
            pass

    except Exception as e:
        logger.error(f"Upload error: {e}")
        await wait_msg.edit_text(
            f"❌ <b>Error:</b> {str(e)[:100]}",
            parse_mode="HTML",
        )


@admin_only
async def sfadd_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text(
            "❌ <b>Usage:</b> <code>/sfadd FILE_CODE filename.mp4</code>",
            parse_mode="HTML",
        )
        return
    file_code = args[0].strip()
    file_name = " ".join(args[1:]).strip()

    info = await asyncio.get_event_loop().run_in_executor(
        None, lambda: savefiles_file_info(file_code)
    )
    if not info:
        await update.message.reply_text(
            "❌ <b>File Not Found</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ Could not verify file on SaveFiles.com.",
            parse_mode="HTML",
        )
        return

    unique_id = hashlib.md5(f"{file_code}{time.time()}".encode()).hexdigest()
    file_size = int(info.get("size", 0))

    save_savefiles_file({
        "unique_id": unique_id,
        "file_code": file_code,
        "stream_url": f"https://xvs.tt/{file_code}",
        "file_name": file_name,
        "file_size": file_size,
        "file_type": "video" if file_name.endswith((".mp4", ".mkv", ".avi", ".mov")) else "document",
        "telegram_file_id": "",
        "upload_time": datetime.now(timezone.utc),
        "view_count": 0,
        "last_checked": datetime.now(timezone.utc),
    })
    deep_link = make_savefiles_deep_link(unique_id)
    await update.message.reply_text(
        f"✅ <b>File Added!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"┣ 📁 Name: {file_name}\n"
        f"┣ 🔑 Code: <code>{file_code}</code>\n"
        f"┗ 🆔 ID: <code>{unique_id}</code>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 <b>Link:</b>\n<code>{deep_link}</code>",
        parse_mode="HTML",
    )


@admin_only
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    total = users_col.count_documents({})
    premium = access_col.count_documents(
        {"premium_expiry": {"$gt": datetime.now(timezone.utc)}}
    )
    concurrent = get_concurrent_users()
    slots = MAX_USERS - total
    files = savefiles_col.count_documents({})
    pending = payments_col.count_documents({"status": "pending"})
    ref_total = referrals_col.count_documents({})
    pct = min(100, round(total / MAX_USERS * 100)) if MAX_USERS else 0
    bar_filled = pct // 10
    bar = "█" * bar_filled + "░" * (10 - bar_filled)
    next_bot_line = f"🔗 Next Bot: {NEXT_BOT_NAME}" if NEXT_BOT_NAME else "🔗 No sister bot configured"
    msg = (
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🤖 <b>Server {SERVER_ID}</b>\n\n"
        f"👥 <b>Users</b>\n"
        f"┣ 👤 Total: {total}/{MAX_USERS}\n"
        f"┣ 💎 Premium: {premium}\n"
        f"┣ ⚡ Active Now: {concurrent}\n"
        f"┗ 🆓 Slots Left: {slots}\n\n"
        f"📈 <b>Load</b>\n"
        f"┗ [{bar}] {pct}%\n\n"
        f"📁 <b>Content</b>\n"
        f"┣ 🎬 Files: {files}\n"
        f"┣ 💳 Pending Payments: {pending}\n"
        f"┗ 👥 Total Referrals: {ref_total}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"{next_bot_line}"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


@admin_only
async def sffiles_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    docs = list(savefiles_col.find({}, sort=[("upload_time", -1)], limit=10))
    if not docs:
        await update.message.reply_text(
            "📁 <b>No Files</b>\n━━━━━━━━━━━━━━━━━━━━━\n┗ No files uploaded yet.",
            parse_mode="HTML",
        )
        return
    lines = []
    for i, doc in enumerate(docs, 1):
        size_mb = round(doc.get("file_size", 0) / 1024 / 1024, 2)
        name = doc.get("file_name", "Unknown")[:30]
        views = doc.get("view_count", 0)
        uid = doc.get("unique_id", "")
        tree = "┣" if i < len(docs) else "┗"
        lines.append(f"{tree} {i}. {name} ({size_mb} MB, 👁 {views})")
    msg = (
        f"📁 <b>Last {len(docs)} Files</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        + "\n".join(lines)
        + f"\n\n━━━━━━━━━━━━━━━━━━━━━\n🎬 Total: {savefiles_col.count_documents({})} files"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


@admin_only
async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text(
            "📢 <b>Usage:</b> Reply to a message with /broadcast",
            parse_mode="HTML",
        )
        return
    reply_msg = update.message.reply_to_message
    user_ids = [doc["user_id"] for doc in users_col.find({}, {"user_id": 1})]
    sent = 0
    failed = 0
    status_msg = await update.message.reply_text(f"📢 Broadcasting to {len(user_ids)} users…")
    for uid in user_ids:
        try:
            await reply_msg.copy(chat_id=uid)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await status_msg.edit_text(
        f"📢 <b>Broadcast Complete</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ ✅ Sent: {sent}\n"
        f"┗ ❌ Failed: {failed}",
        parse_mode="HTML",
    )


@admin_only
async def approve_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("❌ Usage: /approve user_id [plan]", parse_mode="HTML")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
        return

    plan_key = args[1] if len(args) > 1 else "1month"
    plan = PREMIUM_PLANS.get(plan_key, PREMIUM_PLANS["1month"])

    payments_col.update_many(
        {"user_id": target_id, "status": "pending"},
        {"$set": {"status": "approved"}},
    )
    grant_premium(target_id, plan["days"])

    await update.message.reply_text(
        f"✅ <b>Payment Approved</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ 👤 User: <code>{target_id}</code>\n"
        f"┗ 💎 Plan: {plan['label']} granted",
        parse_mode="HTML",
    )
    try:
        await _bot_application.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎉 <b>Premium Activated!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"┣ 💎 Plan: {plan['label']}\n"
                f"┗ ⏳ Enjoy {plan['days']} days of premium!\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"🚀 All files now unlocked!"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


@admin_only
async def reject_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("❌ Usage: /reject user_id", parse_mode="HTML")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
        return

    payments_col.update_many(
        {"user_id": target_id, "status": "pending"},
        {"$set": {"status": "rejected"}},
    )
    await update.message.reply_text(
        f"❌ <b>Payment Rejected</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"┗ 👤 User: <code>{target_id}</code>",
        parse_mode="HTML",
    )
    try:
        await _bot_application.bot.send_message(
            chat_id=target_id,
            text=(
                f"❌ <b>Payment Rejected</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"┗ Your payment could not be verified.\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"Please contact support or resubmit."
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


@admin_only
async def addpremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("❌ Usage: /addpremium user_id days", parse_mode="HTML")
        return
    try:
        target_id = int(args[0])
        days = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.", parse_mode="HTML")
        return

    grant_premium(target_id, days)
    await update.message.reply_text(
        f"✅ <b>Premium Granted</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"┣ 👤 User: <code>{target_id}</code>\n"
        f"┗ 📅 Days: {days}",
        parse_mode="HTML",
    )
    try:
        await _bot_application.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎁 <b>Premium Granted!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"┗ 📅 {days} days of premium added!\n\n"
                f"🚀 Enjoy unlimited access!"
            ),
            parse_mode="HTML",
        )
    except Exception:
        pass


@admin_only
async def removepremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("❌ Usage: /removepremium user_id", parse_mode="HTML")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
        return

    revoke_premium(target_id)
    await update.message.reply_text(
        f"✅ <b>Premium Revoked</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"┗ 👤 User: <code>{target_id}</code>",
        parse_mode="HTML",
    )


# ─────────────────────────────────────────────
# 16. Build Application
# ─────────────────────────────────────────────
def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Public
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("mystatus", mystatus_cmd))
    app.add_handler(CommandHandler("referral", referral_cmd))
    app.add_handler(CommandHandler("premium", premium_cmd))
    app.add_handler(CommandHandler("utr", utr_cmd))

    # Admin
    app.add_handler(CommandHandler("stats", stats_cmd))
    app.add_handler(CommandHandler("sffiles", sffiles_cmd))
    app.add_handler(CommandHandler("sfadd", sfadd_cmd))
    app.add_handler(CommandHandler("broadcast", broadcast_cmd))
    app.add_handler(CommandHandler("approve", approve_cmd))
    app.add_handler(CommandHandler("reject", reject_cmd))
    app.add_handler(CommandHandler("addpremium", addpremium_cmd))
    app.add_handler(CommandHandler("removepremium", removepremium_cmd))

    # Callbacks
    app.add_handler(CallbackQueryHandler(callback_handler))

    # File upload (admin only — non-command messages)
    app.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & (filters.Document.ALL | filters.VIDEO | filters.AUDIO),
            handle_savefiles_upload,
        )
    )
    return app


# ─────────────────────────────────────────────
# 17. Webhook Mode
# ─────────────────────────────────────────────
_bot_loop: asyncio.AbstractEventLoop | None = None


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def init_application():
    global _bot_application
    _bot_application = build_application()
    await _bot_application.initialize()
    await _bot_application.start()
    logger.info("Bot application initialized and started.")


def process_update_sync(data: dict):
    from telegram import Update as TGUpdate
    future = asyncio.run_coroutine_threadsafe(
        _bot_application.process_update(TGUpdate.de_json(data, _bot_application.bot)),
        _bot_loop,
    )
    try:
        future.result(timeout=60)
    except Exception as e:
        logger.error(f"process_update error: {e}")


def set_webhook():
    url = f"{BASE_URL}/webhook/{BOT_TOKEN}"
    resp = requests.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
        json={
            "url": url,
            "drop_pending_updates": True,
            "allowed_updates": ["message", "callback_query"],
        },
        timeout=15,
    )
    if resp.ok and resp.json().get("ok"):
        logger.info(f"Webhook set: {url}")
    else:
        logger.warning(f"Webhook set failed: {resp.text}")


# ─────────────────────────────────────────────
# 18. Main
# ─────────────────────────────────────────────
def main():
    global _bot_loop

    # Setup DB indexes
    setup_indexes()

    # 1. Create background event loop
    _bot_loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_event_loop, args=(_bot_loop,), daemon=True)
    loop_thread.start()

    # 2. Initialize bot in background loop
    future = asyncio.run_coroutine_threadsafe(init_application(), _bot_loop)
    future.result(timeout=30)

    # 3. Set webhook
    set_webhook()

    # 4. Run Flask in main thread
    logger.info(f"Starting Flask on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
