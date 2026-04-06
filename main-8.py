"""
Telegram File Sharing + Streaming Bot
SaveFiles.com Edition — Production Ready
No Pyrogram, No Telethon, No Pixeldrain, No Catbox
Pure SaveFiles.com streaming with premium HTML5 player
"""

import os
import io
import re
import uuid
import hashlib
import logging
import asyncio
import threading
import time
import requests
import urllib.parse
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask, request, jsonify, Response,
    render_template_string, abort, make_response, redirect
)
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, Bot
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    CallbackQueryHandler, filters, ContextTypes
)
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────
BOT_TOKEN          = os.environ.get("BOT_TOKEN", "")
MONGO_URI          = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
ADMIN_IDS          = [int(x) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
BOT_USERNAME       = os.environ.get("BOT_USERNAME", "YourBot")
BASE_URL           = os.environ.get("BASE_URL", "http://localhost:5000")
FLASK_SECRET       = os.environ.get("FLASK_SECRET", uuid.uuid4().hex)
PORT               = int(os.environ.get("PORT", "5000"))

# SaveFiles (Primary & Only Storage)
SAVEFILES_API_KEY  = os.environ.get("SAVEFILES_API_KEY", "")
SF_BASE            = "https://savefiles.com/api"
SF_UPLOAD_URL      = "https://savefiles.com/api"

# Payment
UPI_ID             = os.environ.get("UPI_ID", "yourname@upi")
UPI_QR_URL         = os.environ.get("UPI_QR_URL", "")

# Multi-bot
SERVER_ID          = int(os.environ.get("SERVER_ID", "1"))
MAX_USERS          = int(os.environ.get("MAX_USERS", "500"))
NEXT_BOT_RAW       = os.environ.get("NEXT_BOT", "")
NEXT_BOT_NAME      = NEXT_BOT_RAW.split("|")[0] if "|" in NEXT_BOT_RAW else ""
NEXT_BOT_LINK      = NEXT_BOT_RAW.split("|")[1] if "|" in NEXT_BOT_RAW else ""

# Token & Shortener
TOKEN_VALIDITY_HOURS = int(os.environ.get("TOKEN_VALIDITY_HOURS", "24"))
SHORTENER_API_KEY  = os.environ.get("SHORTENER_API_KEY", "")
SHORTENER_DOMAIN   = os.environ.get("SHORTENER_DOMAIN", "api.shrtco.de")

# Referral rewards
REFERRAL_REWARD_HOURS  = int(os.environ.get("REFERRAL_REWARD_HOURS", "6"))
REFERRAL_PREMIUM_COUNT = int(os.environ.get("REFERRAL_PREMIUM_COUNT", "10"))

# Premium plans
PREMIUM_PLANS = {
    "1month":   {"days": 30,    "price": 49,  "label": "1 Month"},
    "3months":  {"days": 90,    "price": 129, "label": "3 Months"},
    "1year":    {"days": 365,   "price": 399, "label": "1 Year"},
    "lifetime": {"days": 36500, "price": 799, "label": "Lifetime"},
}

# ─────────────────────────────────────────────
#  ACTIVE USER TRACKING
# ─────────────────────────────────────────────
_active_users: dict = {}
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
        return sum(1 for t in _active_users.values() if t >= cutoff)


# ─────────────────────────────────────────────
#  MONGODB SETUP
# ─────────────────────────────────────────────
mongo_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=50,
    minPoolSize=5,
    retryWrites=True,
    retryReads=True,
)
db = mongo_client["filebot"]

# Collections
users_col     = db[f"users_bot{SERVER_ID}"]   # Per-bot registration
access_col    = db["user_access"]              # SHARED: token + premium
savefiles_col = db["savefiles"]               # SHARED: SaveFiles files
payments_col  = db["payments"]                # SHARED: payments
referrals_col = db["referrals"]               # SHARED: referrals


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
        [("referrer_id", ASCENDING), ("referred_user_id", ASCENDING)],
        unique=True
    )
    logger.info(f"✅ MongoDB indexes created | Collection: users_bot{SERVER_ID}")


setup_indexes()

# ─────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────

def get_user(user_id: int) -> dict:
    user = users_col.find_one({"user_id": user_id})
    if not user:
        user = {
            "user_id":         user_id,
            "referrals_count": 0,
            "referred_by":     None,
            "joined_at":       datetime.utcnow(),
        }
        users_col.insert_one(user)
    return user


def get_access(user_id: int) -> dict:
    access = access_col.find_one({"user_id": user_id})
    if not access:
        access = {
            "user_id":        user_id,
            "token_expiry":   None,
            "premium_expiry": None,
        }
        access_col.insert_one(access)
    return access


def is_premium(user_id: int) -> bool:
    access = get_access(user_id)
    exp = access.get("premium_expiry")
    return exp is not None and exp > datetime.utcnow()


def grant_token(user_id: int, hours: int = TOKEN_VALIDITY_HOURS):
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"token_expiry": datetime.utcnow() + timedelta(hours=hours)}},
        upsert=True
    )
    invalidate_token_cache(user_id)


def grant_premium(user_id: int, days: int):
    access = get_access(user_id)
    base = access.get("premium_expiry") or datetime.utcnow()
    if base < datetime.utcnow():
        base = datetime.utcnow()
    new_expiry = base + timedelta(days=days)
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": new_expiry}},
        upsert=True
    )
    invalidate_token_cache(user_id)
    return new_expiry


def revoke_premium(user_id: int):
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": None}},
        upsert=True
    )
    invalidate_token_cache(user_id)


def is_bot_full() -> bool:
    return users_col.count_documents({}) >= MAX_USERS


def record_referral(referrer_id: int, referred_id: int) -> bool:
    if referrer_id == referred_id:
        return False
    try:
        referrals_col.insert_one({
            "referrer_id":      referrer_id,
            "referred_user_id": referred_id,
            "timestamp":        datetime.utcnow()
        })
        users_col.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referrals_count": 1}}
        )
        return True
    except DuplicateKeyError:
        return False


def check_referral_rewards(referrer_id: int) -> str:
    user = get_user(referrer_id)
    count = user.get("referrals_count", 0)
    if count % 5 == 0:
        grant_token(referrer_id, hours=REFERRAL_REWARD_HOURS * (count // 5))
    if count >= REFERRAL_PREMIUM_COUNT and not is_premium(referrer_id):
        grant_premium(referrer_id, 7)
        return "premium"
    return "token"


# ─────────────────────────────────────────────
#  TOKEN CACHE
# ─────────────────────────────────────────────
_token_cache: dict = {}
_token_cache_lock = threading.Lock()


def has_valid_token(user_id: int) -> bool:
    if is_premium(user_id):
        return True
    now = datetime.utcnow()
    with _token_cache_lock:
        cached = _token_cache.get(user_id)
        if cached:
            result, cache_until = cached
            if now < cache_until:
                return result
    result = _check_token_db(user_id)
    with _token_cache_lock:
        _token_cache[user_id] = (result, now + timedelta(seconds=60))
        if len(_token_cache) > 10000:
            cutoff = now - timedelta(minutes=5)
            expired = [k for k, v in _token_cache.items() if v[1] < cutoff]
            for k in expired:
                _token_cache.pop(k, None)
    return result


def _check_token_db(user_id: int) -> bool:
    access = get_access(user_id)
    exp = access.get("token_expiry")
    return exp is not None and exp > datetime.utcnow()


def invalidate_token_cache(user_id: int):
    with _token_cache_lock:
        _token_cache.pop(user_id, None)


# ─────────────────────────────────────────────
#  FILE DOC CACHE
# ─────────────────────────────────────────────
_file_cache: dict = {}
_file_cache_lock = threading.Lock()


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
            if len(_file_cache) > 500:
                oldest = sorted(_file_cache.items(), key=lambda x: x[1][1])[:100]
                for k, _ in oldest:
                    _file_cache.pop(k, None)
    return doc


def invalidate_file_cache(unique_id: str):
    with _file_cache_lock:
        _file_cache.pop(unique_id, None)


# ─────────────────────────────────────────────
#  SAVEFILES API
# ─────────────────────────────────────────────

# Persistent session for connection reuse
_sf_session = requests.Session()
_sf_adapter = requests.adapters.HTTPAdapter(
    pool_connections=20, pool_maxsize=20, max_retries=1
)
_sf_session.mount("https://", _sf_adapter)


def upload_to_savefiles(file_bytes: bytes, filename: str, max_retries: int = 3) -> dict | None:
    """
    Upload to SaveFiles.com using 2-step process:
    Step 1: Get upload server URL
    Step 2: Upload file to that server
    """
    if not SAVEFILES_API_KEY:
        logger.error("SAVEFILES_API_KEY not set!")
        return None

    for attempt in range(1, max_retries + 1):
        try:
            size_mb = len(file_bytes) / 1024 / 1024
            logger.info(f"SaveFiles upload attempt {attempt}/{max_retries}: {filename} ({size_mb:.1f} MB)")

            # ── Step 1: Get upload server ─────────────────
            server_resp = _sf_session.get(
                "https://savefiles.com/api/upload/server",
                params={"key": SAVEFILES_API_KEY},
                timeout=15
            )
            logger.info(f"Upload server response [{server_resp.status_code}]: {server_resp.text[:300]}")

            upload_url = None
            if server_resp.status_code == 200:
                try:
                    server_data = server_resp.json()
                    # Get upload URL from response
                    result = server_data.get("result", {})
                    if isinstance(result, dict):
                        upload_url = result.get("url") or result.get("upload_url")
                    if not upload_url and server_data.get("status") == 200:
                        upload_url = server_data.get("url")
                except Exception:
                    pass

            # Fallback upload URLs if server endpoint fails
            if not upload_url:
                upload_url = "https://savefiles.com/api/file/upload"
                logger.info(f"Using fallback upload URL: {upload_url}")

            # ── Step 2: Upload file ───────────────────────
            upload_resp = _sf_session.post(
                upload_url,
                data={"key": SAVEFILES_API_KEY},
                files={"file": (filename, io.BytesIO(file_bytes), "application/octet-stream")},
                timeout=600
            )

            logger.info(f"Upload response [{upload_resp.status_code}]: {upload_resp.text[:500]}")

            if upload_resp.status_code == 200:
                try:
                    data = upload_resp.json()
                except Exception:
                    logger.warning(f"Non-JSON upload response: {upload_resp.text[:200]}")
                    continue

                # Parse response — handle list or dict
                result = data.get("result") or data.get("files") or data
                if isinstance(result, list) and result:
                    result = result[0]
                if not isinstance(result, dict):
                    result = data

                file_code = (
                    result.get("filecode") or result.get("file_code") or
                    result.get("code") or result.get("hash") or
                    data.get("filecode") or data.get("file_code")
                )
                url = (
                    result.get("url") or result.get("file_url") or
                    result.get("download_url") or
                    (f"https://savefiles.com/{file_code}" if file_code else "")
                )

                if file_code:
                    logger.info(f"✅ SaveFiles upload success: {file_code}")
                    return {
                        "file_code":  file_code,
                        "url":        url,
                        "stream_url": f"https://xvs.tt/{file_code}",
                    }

                logger.warning(f"No file_code in response: {data}")

        except requests.exceptions.Timeout:
            logger.error(f"SaveFiles timeout attempt {attempt}")
        except Exception as e:
            logger.error(f"SaveFiles upload error: {e}")
        time.sleep(3 * attempt)
    return None


def savefiles_file_info(file_code: str) -> dict | None:
    """Get file metadata from SaveFiles — handles multiple response formats."""
    endpoints = [
        "https://savefiles.com/api/file/info",
    ]
    for endpoint in endpoints:
        try:
            resp = _sf_session.get(
                endpoint,
                params={"key": SAVEFILES_API_KEY, "file_code": file_code},
                timeout=15
            )
            logger.info(f"SaveFiles info [{resp.status_code}]: {resp.text[:200]}")
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    continue

                # Various response formats
                if data.get("status") == 200 or data.get("msg") == "OK":
                    result = data.get("result") or data.get("file") or data
                    if isinstance(result, list) and result:
                        result = result[0]
                    if result and isinstance(result, dict):
                        return result

                # Some APIs return result directly
                if data.get("file_code") or data.get("filecode") or data.get("name"):
                    return data

        except Exception as e:
            logger.error(f"SaveFiles info error ({endpoint}): {e}")
    return None


def check_savefiles(file_code: str) -> bool:
    return bool(savefiles_file_info(file_code))


def save_savefiles_file(unique_id: str, file_code: str, stream_url: str,
                         file_name: str, file_size: int, file_type: str,
                         telegram_file_id: str = "") -> dict:
    doc = {
        "unique_id":         unique_id,
        "file_code":         file_code,
        "stream_url":        stream_url,
        "file_name":         file_name,
        "file_size":         file_size,
        "file_type":         file_type,
        "telegram_file_id":  telegram_file_id,
        "upload_time":       datetime.utcnow(),
        "view_count":        0,
        "last_checked":      datetime.utcnow(),
    }
    savefiles_col.update_one(
        {"unique_id": unique_id},
        {"$set": doc},
        upsert=True
    )
    logger.info(f"✅ SaveFiles saved: {file_name} | code: {file_code}")
    return doc


def make_sf_deep_link(unique_id: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=sf_{unique_id}"


# ─────────────────────────────────────────────
#  URL SHORTENER
# ─────────────────────────────────────────────

def shorten_url(long_url: str) -> str:
    if not SHORTENER_API_KEY:
        try:
            resp = requests.get(
                f"https://api.shrtco.de/v2/shorten?url={urllib.parse.quote(long_url)}",
                timeout=10
            )
            if resp.status_code == 200:
                return resp.json()["result"]["full_short_link"]
        except Exception as e:
            logger.warning(f"shrtco.de failed: {e}")
        return long_url
    try:
        resp = requests.get(
            f"https://{SHORTENER_DOMAIN}/api?api={SHORTENER_API_KEY}&url={urllib.parse.quote(long_url)}",
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("shortenedUrl") or data.get("short_link") or long_url
    except Exception as e:
        logger.warning(f"Shortener failed: {e}")
    return long_url


# ─────────────────────────────────────────────
#  ADMIN DECORATOR
# ─────────────────────────────────────────────

def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        if update.effective_user.id not in ADMIN_IDS:
            await update.message.reply_text("❌ Admin only command.")
            return
        return await func(update, ctx, *args, **kwargs)
    return wrapper


# ─────────────────────────────────────────────
#  FLASK APP + WATCH HTML
# ─────────────────────────────────────────────
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET

WATCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{{ file_name }}</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    :root {
      --bg: #0a0a0f; --surface: #12121a; --card: #1a1a27;
      --border: #ffffff12; --accent: #6c63ff; --accent2: #ff6584;
      --text: #e8e8f0; --muted: #6b6b80; --glow: rgba(108,99,255,0.35);
    }
    html, body { height: 100%; background: var(--bg); color: var(--text);
      font-family: 'Inter', sans-serif; overflow-x: hidden; }
    body::before { content: ''; position: fixed; inset: 0;
      background:
        radial-gradient(ellipse 80% 50% at 20% 10%, rgba(108,99,255,0.12) 0%, transparent 60%),
        radial-gradient(ellipse 60% 40% at 80% 90%, rgba(255,101,132,0.08) 0%, transparent 60%);
      pointer-events: none; z-index: 0; }
    .page { position: relative; z-index: 1; min-height: 100vh;
      display: flex; flex-direction: column; align-items: center; padding: 24px 16px 40px; }
    .topbar { width: 100%; max-width: 820px; display: flex;
      align-items: center; justify-content: space-between; margin-bottom: 28px; }
    .logo { display: flex; align-items: center; gap: 10px; text-decoration: none; }
    .logo-icon { width: 36px; height: 36px;
      background: linear-gradient(135deg, var(--accent), var(--accent2));
      border-radius: 10px; display: flex; align-items: center;
      justify-content: center; font-size: 18px; box-shadow: 0 0 16px var(--glow); }
    .logo-text { font-size: 1rem; font-weight: 700;
      background: linear-gradient(90deg, var(--accent), var(--accent2));
      -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .badge { background: linear-gradient(135deg, var(--accent), var(--accent2));
      color: #fff; font-size: 0.7rem; font-weight: 600; padding: 4px 10px;
      border-radius: 20px; letter-spacing: 0.5px; text-transform: uppercase; }
    .player-card { width: 100%; max-width: 820px; background: var(--card);
      border: 1px solid var(--border); border-radius: 20px; overflow: hidden;
      box-shadow: 0 0 0 1px var(--border), 0 20px 60px rgba(0,0,0,0.6),
        0 0 80px rgba(108,99,255,0.08); }
    .video-wrap { position: relative; width: 100%; background: #000; }
    video { width: 100%; display: block; max-height: 480px; background: #000; }
    .video-wrap::after { content: ''; position: absolute; bottom: 0; left: 0; right: 0;
      height: 4px; background: linear-gradient(90deg, var(--accent), var(--accent2)); }
    .info { padding: 20px 24px 16px; border-bottom: 1px solid var(--border); }
    .file-title { font-size: 1.05rem; font-weight: 600; color: var(--text);
      white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .file-meta { margin-top: 6px; font-size: 0.78rem; color: var(--muted);
      display: flex; gap: 16px; flex-wrap: wrap; }
    .stats { margin: 0 24px 20px; padding: 14px 18px; background: var(--surface);
      border: 1px solid var(--border); border-radius: 12px;
      display: flex; gap: 24px; flex-wrap: wrap; }
    .stat { display: flex; flex-direction: column; gap: 2px; }
    .stat-label { font-size: 0.68rem; color: var(--muted);
      text-transform: uppercase; letter-spacing: 0.8px; font-weight: 500; }
    .stat-value { font-size: 0.88rem; font-weight: 600; color: var(--text); }
    .stat-value.accent { color: var(--accent); }
    .actions { padding: 16px 24px 20px; display: flex; gap: 12px; flex-wrap: wrap; }
    .btn { display: inline-flex; align-items: center; gap: 8px; padding: 10px 20px;
      border-radius: 10px; font-size: 0.85rem; font-weight: 600;
      text-decoration: none; cursor: pointer; border: none; transition: all 0.2s ease; }
    .btn-primary { background: linear-gradient(135deg, var(--accent), #8b7cf8);
      color: #fff; box-shadow: 0 4px 16px rgba(108,99,255,0.35); }
    .btn-primary:hover { transform: translateY(-1px); box-shadow: 0 6px 24px rgba(108,99,255,0.5); }
    .btn-ghost { background: var(--surface); color: var(--text);
      border: 1px solid var(--border); }
    .btn-ghost:hover { background: var(--card); border-color: var(--accent); color: var(--accent); }
    .loader { display: none; position: absolute; inset: 0;
      background: rgba(10,10,15,0.85); align-items: center;
      justify-content: center; flex-direction: column; gap: 16px; z-index: 10; }
    .spinner { width: 44px; height: 44px; border: 3px solid var(--border);
      border-top-color: var(--accent); border-radius: 50%;
      animation: spin 0.8s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
    .loader-text { font-size: 0.85rem; color: var(--muted); }
    .footer { margin-top: 28px; text-align: center; font-size: 0.75rem;
      color: var(--muted); line-height: 1.7; }
    @media (max-width: 600px) {
      .page { padding: 16px 12px 32px; }
      .topbar { margin-bottom: 16px; }
      .info { padding: 16px 16px 12px; }
      .actions { padding: 12px 16px 16px; gap: 8px; }
      .stats { margin: 0 16px 16px; gap: 16px; }
      video { max-height: 240px; }
    }
  </style>
</head>
<body>
  <div class="page">
    <div class="topbar">
      <a class="logo" href="#">
        <div class="logo-icon">🎬</div>
        <span class="logo-text">FileBot</span>
      </a>
      <span class="badge">Premium Stream</span>
    </div>
    <div class="player-card">
      <div class="video-wrap">
        <div class="loader" id="loader">
          <div class="spinner"></div>
          <span class="loader-text">Loading stream...</span>
        </div>
        <video id="player" controls autoplay preload="metadata" playsinline>
          <source src="{{ video_url }}" type="video/mp4">
          <source src="{{ video_url }}" type="video/webm">
          Your browser does not support HTML5 video.
        </video>
      </div>
      <div class="info">
        <div class="file-title">{{ file_name }}</div>
        <div class="file-meta">
          <span>🔒 Secured Stream</span>
          <span>⚡ Direct CDN</span>
          <span>♾️ Always Available</span>
        </div>
      </div>
      <div class="stats">
        <div class="stat">
          <span class="stat-label">Status</span>
          <span class="stat-value accent" id="stat-status">Connecting...</span>
        </div>
        <div class="stat">
          <span class="stat-label">Duration</span>
          <span class="stat-value" id="stat-duration">--:--</span>
        </div>
        <div class="stat">
          <span class="stat-label">Resolution</span>
          <span class="stat-value" id="stat-res">--</span>
        </div>
        <div class="stat">
          <span class="stat-label">Buffered</span>
          <span class="stat-value" id="stat-buf">0%</span>
        </div>
        <div class="stat">
          <span class="stat-label">Views</span>
          <span class="stat-value accent">{{ view_count }}</span>
        </div>
      </div>
      <div class="actions">
        <a class="btn btn-primary" href="{{ video_url }}" download="{{ file_name }}">⬇️ Download</a>
        <button class="btn btn-ghost" onclick="toggleFullscreen()">⛶ Fullscreen</button>
        <button class="btn btn-ghost" onclick="copyLink()">🔗 Copy Link</button>
      </div>
    </div>
    <div class="footer">
      Powered by <strong>FileBot</strong> · Secure Streaming<br>
      🔒 Your access is protected &amp; encrypted
    </div>
  </div>
  <script>
    const video = document.getElementById('player');
    const loader = document.getElementById('loader');
    video.addEventListener('waiting', () => { loader.style.display = 'flex'; });
    video.addEventListener('playing', () => {
      loader.style.display = 'none';
      document.getElementById('stat-status').textContent = '▶ Playing';
      document.getElementById('stat-status').style.color = '#4ade80';
    });
    video.addEventListener('pause', () => {
      document.getElementById('stat-status').textContent = '⏸ Paused';
      document.getElementById('stat-status').style.color = '#facc15';
    });
    video.addEventListener('error', () => {
      loader.style.display = 'none';
      document.getElementById('stat-status').textContent = '✗ Error';
      document.getElementById('stat-status').style.color = '#f87171';
    });
    video.addEventListener('loadedmetadata', () => {
      const d = video.duration;
      if (isFinite(d)) {
        const m = Math.floor(d / 60), s = Math.floor(d % 60).toString().padStart(2, '0');
        document.getElementById('stat-duration').textContent = m + ':' + s;
      }
      if (video.videoWidth)
        document.getElementById('stat-res').textContent = video.videoWidth + '×' + video.videoHeight;
      loader.style.display = 'none';
    });
    setInterval(() => {
      if (video.buffered.length > 0 && video.duration) {
        const pct = Math.round((video.buffered.end(video.buffered.length - 1) / video.duration) * 100);
        document.getElementById('stat-buf').textContent = pct + '%';
      }
    }, 1000);
    function toggleFullscreen() {
      document.fullscreenElement ? document.exitFullscreen() :
        video.requestFullscreen().catch(() => video.webkitEnterFullscreen && video.webkitEnterFullscreen());
    }
    function copyLink() {
      navigator.clipboard.writeText(window.location.href).then(() => {
        const btn = event.target;
        btn.textContent = '✅ Copied!';
        setTimeout(() => btn.textContent = '🔗 Copy Link', 2000);
      });
    }
    document.addEventListener('keydown', e => {
      if (e.code === 'Space') { e.preventDefault(); video.paused ? video.play() : video.pause(); }
      if (e.code === 'ArrowRight') video.currentTime += 10;
      if (e.code === 'ArrowLeft') video.currentTime -= 10;
      if (e.code === 'ArrowUp') video.volume = Math.min(1, video.volume + 0.1);
      if (e.code === 'ArrowDown') video.volume = Math.max(0, video.volume - 0.1);
      if (e.code === 'KeyF') toggleFullscreen();
    });
  </script>
</body>
</html>"""


# ─────────────────────────────────────────────
#  HMAC TOKEN FUNCTIONS
# ─────────────────────────────────────────────

def make_sf_token(user_id: int, unique_id: str) -> str:
    return hashlib.sha256(
        f"sf:{user_id}:{unique_id}:{FLASK_SECRET}".encode()
    ).hexdigest()[:16]


def make_watch_url(user_id: int, unique_id: str) -> str:
    t = make_sf_token(user_id, unique_id)
    return f"{BASE_URL}/sf/watch?id={unique_id}&u={user_id}&t={t}"


def verify_sf_access(unique_id: str):
    token       = request.args.get("t", "")
    user_id_raw = request.args.get("u", "")
    if not user_id_raw:
        return None, "Missing auth"
    try:
        uid = int(user_id_raw)
    except ValueError:
        return None, "Bad user ID"
    expected = make_sf_token(uid, unique_id)
    if token != expected:
        return None, "Invalid token"
    if not has_valid_token(uid):
        return None, "No valid token"
    sf_file = get_savefiles_file_cached(unique_id)
    if not sf_file:
        return None, "File not found"
    return sf_file, None


def access_denied_html(err: str) -> str:
    return f"""<html><body style="background:#0a0a0f;color:#e8e8f0;
    font-family:Inter,sans-serif;display:flex;align-items:center;
    justify-content:center;height:100vh;flex-direction:column;gap:16px">
    <div style="font-size:48px">🔒</div>
    <h2 style="color:#f87171">Access Denied</h2>
    <p style="color:#6b6b80">{err}</p>
    <p style="color:#6b6b80;font-size:0.85rem">Open the file link via Telegram bot</p>
    </body></html>"""


# ─────────────────────────────────────────────
#  FLASK ROUTES
# ─────────────────────────────────────────────

@flask_app.route("/sf/watch")
def sf_watch():
    unique_id = request.args.get("id", "")
    if not unique_id:
        abort(400)
    sf_file, err = verify_sf_access(unique_id)
    if err:
        return access_denied_html(err), 403

    file_code  = sf_file.get("file_code", "")
    direct_url = (
        f"https://savefiles.com/api/file/direct"
        f"?key={SAVEFILES_API_KEY}&file_code={file_code}"
    )

    resp = make_response(render_template_string(
        WATCH_HTML,
        file_name  = sf_file.get("file_name", "Video"),
        video_url  = direct_url,
        view_count = sf_file.get("view_count", 0),
    ))
    resp.headers["Cache-Control"] = "private, max-age=300"
    return resp


@flask_app.route("/verify/<int:user_id>")
def verify_redirect(user_id):
    return redirect(f"https://t.me/{BOT_USERNAME}?start=verify_{user_id}")


@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@flask_app.route("/")
def index():
    return f"<h2>🎬 FileBot</h2><p>Use @{BOT_USERNAME} on Telegram</p>"


# ─────────────────────────────────────────────
#  TELEGRAM HANDLERS
# ─────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    payload = ctx.args[0] if ctx.args else ""

    track_active_user(user.id)

    # ── SaveFiles file request ────────────────
    if payload.startswith("sf_"):
        unique_id   = payload[3:]
        is_new_user = users_col.find_one({"user_id": user.id}) is None
        if is_new_user and is_bot_full() and NEXT_BOT_LINK:
            await send_redirect_message(update, ctx, file_id=unique_id)
            return
        get_user(user.id)
        await handle_sf_request(update, ctx, unique_id)
        return

    # ── Referral ──────────────────────────────
    if payload.startswith("ref_"):
        try:
            referrer_id = int(payload[4:])
            is_new_user = users_col.find_one({"user_id": user.id}) is None
            if is_new_user and is_bot_full() and NEXT_BOT_LINK:
                await send_redirect_message(update, ctx)
                return
            get_user(user.id)
            if record_referral(referrer_id, user.id):
                reward = check_referral_rewards(referrer_id)
                try:
                    reward_msg = (
                        "🏆 You earned 7-day Premium!" if reward == "premium"
                        else "⏰ You earned bonus token hours!"
                    )
                    await ctx.bot.send_message(
                        referrer_id,
                        f"🎉 <b>New Referral!</b>\n\n"
                        f"User <code>{user.id}</code> joined via your link!\n"
                        f"{reward_msg}",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
        except (ValueError, IndexError):
            pass
        get_user(user.id)
        await send_welcome(update, ctx)
        return

    # ── Token verification ────────────────────
    if payload.startswith("verify_"):
        try:
            uid = int(payload[7:])
            if uid == user.id:
                grant_token(user.id)
                await update.message.reply_text(
                    "✅ <b>Verification Successful!</b>\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "🎉 You now have access for <b>24 hours!</b>\n\n"
                    "📌 <b>What to do next:</b>\n"
                    "┗ Click your file link again to access it\n\n"
                    "━━━━━━━━━━━━━━━━━━━━━\n"
                    "💎 Get Premium to skip verification forever!",
                    parse_mode="HTML"
                )
                return
        except (ValueError, IndexError):
            pass

    # ── New user redirect if bot full ─────────
    is_new_user = users_col.find_one({"user_id": user.id}) is None
    if is_new_user and is_bot_full() and NEXT_BOT_LINK:
        await send_redirect_message(update, ctx)
        return

    get_user(user.id)
    await send_welcome(update, ctx)


async def send_welcome(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user        = update.effective_user
    ref_link    = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    total_users = users_col.count_documents({})
    concurrent  = get_concurrent_users()

    text = (
        f"👋 <b>Welcome, {user.first_name}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🎬 <b>File Sharing Bot</b> — Server {SERVER_ID}\n\n"
        f"📌 <b>Commands:</b>\n"
        f"┣ 💎 /premium — Get premium access\n"
        f"┣ 👥 /referral — Your referral link\n"
        f"┗ 📊 /mystatus — Token & premium status\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"🎁 Refer friends &amp; earn free premium!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Server {SERVER_ID}  •  "
        f"👤 {total_users}/{MAX_USERS}  •  "
        f"⚡ {concurrent} active"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu"),
        InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}")
    ]])
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


async def send_redirect_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_id: str = ""):
    user      = update.effective_user
    next_name = f"@{NEXT_BOT_NAME}" if NEXT_BOT_NAME else "our sister bot"

    if file_id and NEXT_BOT_NAME:
        deep_link = f"https://t.me/{NEXT_BOT_NAME}?start=sf_{file_id}"
        text = (
            f"👋 <b>Hello {user.first_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ This bot is currently full!\n\n"
            f"✅ Your file is ready on our sister bot.\n"
            f"Click below — opens automatically!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Sister bot: <b>{next_name}</b>"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Get My File Now", url=deep_link)
        ]])
    else:
        text = (
            f"👋 <b>Hello {user.first_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ This bot is full ({MAX_USERS} users)!\n\n"
            f"✅ Join our sister bot:\n"
            f"┗ Same files &amp; features!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 <b>{next_name}</b>"
        )
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("➡️ Join Sister Bot", url=NEXT_BOT_LINK)
        ]])

    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


async def handle_sf_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, unique_id: str):
    """Serve SaveFiles file to user."""
    user    = update.effective_user
    sf_file = get_savefiles_file_cached(unique_id)

    if not sf_file:
        await update.message.reply_text(
            "❌ <b>File not found!</b>\n\nThis link may be invalid.",
            parse_mode="HTML"
        )
        return

    if not has_valid_token(user.id):
        await send_verification_prompt(update, ctx, unique_id)
        return

    file_type = sf_file.get("file_type", "document")
    file_name = sf_file.get("file_name", "file")
    file_code = sf_file.get("file_code", "")
    file_size = sf_file.get("file_size", 0)
    size_mb   = file_size / 1024 / 1024 if file_size else 0

    # Auto re-check Pixeldrain (once per hour)
    last_checked = sf_file.get("last_checked")
    if last_checked and (datetime.utcnow() - last_checked).total_seconds() > 3600:
        if not check_savefiles(file_code):
            tg_fid = sf_file.get("telegram_file_id", "")
            if tg_fid:
                msg = await update.message.reply_text("🔄 Re-uploading file...")
                try:
                    file_obj   = await ctx.bot.get_file(tg_fid)
                    file_bytes = bytes(await file_obj.download_as_bytearray())
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: upload_to_savefiles(file_bytes, file_name)
                    )
                    if result:
                        savefiles_col.update_one(
                            {"unique_id": unique_id},
                            {"$set": {
                                "file_code":    result["file_code"],
                                "stream_url":   result["stream_url"],
                                "last_checked": datetime.utcnow(),
                            }}
                        )
                        file_code = result["file_code"]
                        invalidate_file_cache(unique_id)
                        await msg.delete()
                    else:
                        await msg.edit_text("❌ File unavailable. Contact admin.")
                        return
                except Exception as e:
                    logger.error(f"Re-upload error: {e}")
                    await msg.edit_text("❌ File unavailable. Contact admin.")
                    return
            else:
                await update.message.reply_text(
                    "❌ <b>File unavailable!</b>\n\nContact admin to re-upload.",
                    parse_mode="HTML"
                )
                return
        savefiles_col.update_one(
            {"unique_id": unique_id},
            {"$set": {"last_checked": datetime.utcnow()}}
        )

    # Increment views
    savefiles_col.update_one({"unique_id": unique_id}, {"$inc": {"view_count": 1}})
    views = sf_file.get("view_count", 0) + 1

    if file_type == "video":
        watch_url = make_watch_url(user.id, unique_id)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Watch Now", url=watch_url)
        ]])
        await update.message.reply_text(
            f"🎬 <b>{file_name}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ File ready to stream!\n\n"
            f"📦 Size: {size_mb:.1f} MB\n"
            f"👁️ Views: {views}\n"
            f"🔒 Personal secure link\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"👇 Click to watch:",
            parse_mode="HTML",
            reply_markup=kb
        )
    else:
        stream_url = sf_file.get("stream_url", "")
        await update.message.reply_text(
            f"📁 <b>{file_name}</b>\n\n"
            f"📦 Size: {size_mb:.1f} MB\n\n"
            f"🔗 Download:\n<code>{stream_url}</code>",
            parse_mode="HTML"
        )


async def send_verification_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE, unique_id: str):
    user      = update.effective_user
    verify_url = f"{BASE_URL}/verify/{user.id}"
    short_url  = shorten_url(verify_url)

    text = (
        "🔐 <b>Verification Required</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ You need to verify to access this file.\n\n"
        "📋 <b>Simple Steps:</b>\n"
        "┣ 1️⃣ Click <b>Verify Now</b> below\n"
        "┣ 2️⃣ Complete the short page\n"
        "┗ 3️⃣ Come back &amp; click file link again\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⏳ Token valid for <b>24 hours</b>\n"
        "💎 Get Premium to skip forever!"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Verify Now", url=short_url),
        InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu")
    ]])
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


async def mystatus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    db_user = get_user(user.id)
    access  = get_access(user.id)

    token_exp = access.get("token_expiry")
    prem_exp  = access.get("premium_expiry")
    refs      = db_user.get("referrals_count", 0)
    joined    = db_user.get("joined_at", datetime.utcnow())
    joined_str = joined.strftime("%d %b %Y") if hasattr(joined, "strftime") else str(joined)[:10]

    if token_exp and token_exp > datetime.utcnow():
        remaining = token_exp - datetime.utcnow()
        h, rem    = divmod(int(remaining.total_seconds()), 3600)
        m         = rem // 60
        token_str = f"✅ Valid  •  ⏳ {h}h {m}m left"
    else:
        token_str = "❌ No active token"

    prem_str = "❌ Not Active"
    if prem_exp and prem_exp > datetime.utcnow():
        prem_str = f"✅ Active until {prem_exp.strftime('%d %b %Y')}"

    await update.message.reply_text(
        f"📊 <b>Your Account Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔐 <b>Token Access</b>\n"
        f"┗ {token_str}\n\n"
        f"💎 <b>Premium</b>\n"
        f"┗ {prem_str}\n\n"
        f"👥 <b>Referrals</b>\n"
        f"┗ {refs} friends referred\n\n"
        f"📅 <b>Joined</b>\n"
        f"┗ {joined_str}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🤖 Server {SERVER_ID}",
        parse_mode="HTML"
    )


async def referral_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user     = update.effective_user
    db_user  = get_user(user.id)
    refs     = db_user.get("referrals_count", 0)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    next_milestone = max(0, REFERRAL_PREMIUM_COUNT - refs)

    await update.message.reply_text(
        f"👥 <b>Referral Program</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"📊 <b>Your Stats:</b>\n"
        f"┣ 👤 Total Referred: <b>{refs}</b>\n"
        f"┗ 🎯 Need <b>{next_milestone} more</b> for 7-day Premium\n\n"
        f"🎁 <b>Rewards:</b>\n"
        f"┣ ⏰ Every 5 referrals → Bonus token hours\n"
        f"┗ 🏆 {REFERRAL_PREMIUM_COUNT} referrals → 7-day Premium FREE!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Share now and start earning! 🚀",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton(
                "📤 Share Now",
                url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}&text=Join+this+amazing+bot!"
            )
        ]])
    )


async def premium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_premium_menu(update.message)


async def show_premium_menu(msg_or_query):
    plan_lines = "\n".join([
        f"┣ 💎 <b>{v['label']}</b>  —  ₹{v['price']}"
        for v in list(PREMIUM_PLANS.values())[:-1]
    ] + [
        f"┗ 👑 <b>{list(PREMIUM_PLANS.values())[-1]['label']}</b>"
        f"  —  ₹{list(PREMIUM_PLANS.values())[-1]['price']}"
    ])
    text = (
        f"💎 <b>Premium Access</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>Available Plans:</b>\n"
        f"{plan_lines}\n\n"
        f"✅ <b>Premium Benefits:</b>\n"
        f"┣ ⚡ Skip daily verification\n"
        f"┣ ♾️ Unlimited file access\n"
        f"┣ 🚀 Faster streaming\n"
        f"┗ 🎯 Priority support\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"👇 Select a plan to proceed:"
    )
    buttons = [
        [InlineKeyboardButton(f"{v['label']} – ₹{v['price']}", callback_data=f"buy_{k}")]
        for k, v in PREMIUM_PLANS.items()
    ]
    kb = InlineKeyboardMarkup(buttons)
    if hasattr(msg_or_query, "reply_text"):
        await msg_or_query.reply_text(text, parse_mode="HTML", reply_markup=kb)
    else:
        await msg_or_query.edit_message_text(text, parse_mode="HTML", reply_markup=kb)


async def callback_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q    = update.callback_query
    await q.answer()
    data = q.data

    if data == "premium_menu":
        await show_premium_menu(q)
        return

    if data.startswith("buy_"):
        plan_key = data[4:]
        plan     = PREMIUM_PLANS.get(plan_key)
        if not plan:
            return
        ctx.user_data["pending_plan"] = plan_key
        upi_text = (
            f"💳 <b>Complete Your Payment</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 <b>Plan:</b> {plan['label']}\n"
            f"💰 <b>Amount:</b> ₹{plan['price']}\n\n"
            f"📲 <b>Pay to UPI ID:</b>\n"
            f"<code>{UPI_ID}</code>\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📋 <b>After Payment:</b>\n"
            f"Send your UTR/Transaction ID:\n"
            f"<code>/utr YOUR_UTR_NUMBER</code>\n\n"
            f"📌 Example: <code>/utr 123456789012</code>\n\n"
            f"⚠️ <b>Important:</b> Do NOT close this chat until approved!"
        )
        buttons = []
        if UPI_QR_URL:
            buttons.append([InlineKeyboardButton("📷 View QR Code", url=UPI_QR_URL)])
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="premium_menu")])
        await q.edit_message_text(upi_text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(buttons))


async def utr_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    args = ctx.args
    if not args:
        await update.message.reply_text("❗ Usage: /utr <your_UTR_number>")
        return
    utr = args[0].strip()
    if not re.match(r'^[A-Za-z0-9]{10,22}$', utr):
        await update.message.reply_text("❌ Invalid UTR format. Please check and try again.")
        return
    if payments_col.find_one({"utr": utr}):
        await update.message.reply_text("❌ This UTR has already been submitted.")
        return

    plan_key = ctx.user_data.get("pending_plan", "1month")
    plan     = PREMIUM_PLANS.get(plan_key, PREMIUM_PLANS["1month"])

    payments_col.insert_one({
        "user_id":    user.id,
        "username":   user.username or "N/A",
        "utr":        utr,
        "plan":       plan_key,
        "price":      plan["price"],
        "status":     "pending",
        "created_at": datetime.utcnow()
    })

    await update.message.reply_text(
        f"✅ <b>Payment Submitted!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔢 <b>UTR Number:</b>\n"
        f"<code>{utr}</code>\n\n"
        f"📦 <b>Plan:</b> {plan['label']}\n"
        f"💰 <b>Amount:</b> ₹{plan['price']}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ Admin will verify within a few hours\n"
        f"🔔 You will be notified once approved\n\n"
        f"📞 Contact admin if not approved in 24h",
        parse_mode="HTML"
    )

    uid_v      = user.id
    uname_v    = user.username or "N/A"
    plan_label = plan['label']
    plan_price = plan['price']

    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                admin_id,
                f"🔔 <b>New Payment Request!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>User:</b> <code>{uid_v}</code>\n"
                f"📛 <b>Username:</b> @{uname_v}\n"
                f"📦 <b>Plan:</b> {plan_label}\n"
                f"💰 <b>Amount:</b> ₹{plan_price}\n"
                f"🔢 <b>UTR:</b> <code>{utr}</code>\n\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n"
                f"✅ Approve: <code>/approve {uid_v}</code>\n"
                f"❌ Reject:  <code>/reject {uid_v}</code>",
                parse_mode="HTML"
            )
        except Exception:
            pass


# ─────────────────────────────────────────────
#  ADMIN HANDLERS
# ─────────────────────────────────────────────

@admin_only
async def approve_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /approve <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return

    payment = payments_col.find_one(
        {"user_id": uid, "status": "pending"},
        sort=[("created_at", -1)]
    )
    if not payment:
        await update.message.reply_text(f"No pending payment for {uid}.")
        return

    plan   = PREMIUM_PLANS.get(payment.get("plan", "1month"), PREMIUM_PLANS["1month"])
    expiry = grant_premium(uid, plan["days"])
    payments_col.update_one({"_id": payment["_id"]}, {"$set": {"status": "approved"}})

    await update.message.reply_text(
        f"✅ Approved! User {uid} has premium until {expiry.strftime('%d %b %Y')}."
    )
    try:
        await ctx.bot.send_message(
            uid,
            f"🎉 <b>Payment Approved!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ <b>{plan['label']} Premium</b> activated!\n\n"
            f"📅 <b>Expires:</b> {expiry.strftime('%d %b %Y')}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚡ Skip verification on all files!\n"
            f"🚀 Enjoy unlimited access!",
            parse_mode="HTML"
        )
    except Exception:
        pass


@admin_only
async def reject_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /reject <user_id>")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("Invalid user ID.")
        return

    payment = payments_col.find_one(
        {"user_id": uid, "status": "pending"},
        sort=[("created_at", -1)]
    )
    if not payment:
        await update.message.reply_text(f"No pending payment for {uid}.")
        return

    payments_col.update_one({"_id": payment["_id"]}, {"$set": {"status": "rejected"}})
    await update.message.reply_text(f"❌ Rejected payment for user {uid}.")
    try:
        await ctx.bot.send_message(
            uid,
            "❌ <b>Payment Rejected</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━\n\n"
            "⚠️ Your payment could not be verified.\n\n"
            "📋 <b>Possible reasons:</b>\n"
            "┣ Invalid UTR number\n"
            "┣ Payment not received\n"
            "┗ Wrong UPI ID used\n\n"
            "━━━━━━━━━━━━━━━━━━━━━\n"
            "💬 Contact admin for help\n"
            "🔄 Or try again with correct UTR",
            parse_mode="HTML"
        )
    except Exception:
        pass


@admin_only
async def addpremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /addpremium <user_id> <days>")
        return
    try:
        uid  = int(args[0])
        days = int(args[1])
    except ValueError:
        await update.message.reply_text("Invalid arguments.")
        return
    expiry = grant_premium(uid, days)
    await update.message.reply_text(
        f"✅ Added {days}-day premium to {uid}.\nExpires: {expiry.strftime('%d %b %Y')}"
    )
    try:
        await ctx.bot.send_message(uid, f"🎁 Admin gifted you {days}-day Premium! Enjoy! 🚀")
    except Exception:
        pass


@admin_only
async def removepremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /removepremium <user_id>")
        return
    uid = int(args[0])
    revoke_premium(uid)
    await update.message.reply_text(f"✅ Premium removed from {uid}.")


@admin_only
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    total_users   = users_col.count_documents({})
    premium_users = users_col.count_documents({
        "user_id": {"$in": [
            a["user_id"] for a in access_col.find(
                {"premium_expiry": {"$gt": datetime.utcnow()}},
                {"user_id": 1}
            )
        ]}
    })
    total_files   = savefiles_col.count_documents({})
    pending_pay   = payments_col.count_documents({"status": "pending"})
    total_refs    = referrals_col.count_documents({})
    concurrent    = get_concurrent_users()
    slots_left    = max(0, MAX_USERS - total_users)
    load_pct      = int((total_users / MAX_USERS) * 100) if MAX_USERS > 0 else 0
    filled        = int(load_pct / 10)
    bar           = "█" * filled + "░" * (10 - filled)

    await update.message.reply_text(
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🤖 <b>Server {SERVER_ID}</b>\n\n"
        f"👥 <b>Users</b>\n"
        f"┣ 👤 Total: <b>{total_users}/{MAX_USERS}</b>\n"
        f"┣ 💎 Premium: <b>{premium_users}</b>\n"
        f"┣ ⚡ Active Now: <b>{concurrent}</b>\n"
        f"┗ 🆓 Slots Left: <b>{slots_left}</b>\n\n"
        f"📈 <b>Load</b>\n"
        f"┗ [{bar}] {load_pct}%\n\n"
        f"📁 <b>Content</b>\n"
        f"┣ 🎬 Files: <b>{total_files}</b>\n"
        f"┣ 💳 Pending Payments: <b>{pending_pay}</b>\n"
        f"┗ 👥 Total Referrals: <b>{total_refs}</b>\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🔗 Next Bot: {NEXT_BOT_NAME or '❌ Not set'}",
        parse_mode="HTML"
    )


@admin_only
async def sffiles_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    files = list(savefiles_col.find().sort("upload_time", -1).limit(10))
    if not files:
        await update.message.reply_text("No files uploaded yet.")
        return
    lines = ["📁 <b>Last 10 Files:</b>\n"]
    for f in files:
        link    = make_sf_deep_link(f["unique_id"])
        size_mb = (f.get("file_size", 0) or 0) / 1024 / 1024
        views   = f.get("view_count", 0)
        lines.append(
            f"▪️ <b>{f.get('file_name', '?')}</b>\n"
            f"   📦 {size_mb:.1f} MB • 👁️ {views} views\n"
            f"   <code>{link}</code>"
        )
    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode="HTML",
        disable_web_page_preview=True
    )


@admin_only
async def sfadd_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Add large file by SaveFiles file code: /sfadd FILE_CODE name.mp4"""
    args = ctx.args
    if not args:
        await update.message.reply_text(
            "📋 <b>Usage:</b>\n"
            "<code>/sfadd FILE_CODE filename.mp4</code>\n\n"
            "<b>Steps for large files (&gt;20MB):</b>\n"
            "┣ 1️⃣ Upload to savefiles.com manually\n"
            "┣ 2️⃣ Copy file code from URL\n"
            "┗ 3️⃣ Send: <code>/sfadd CODE filename.mp4</code>",
            parse_mode="HTML"
        )
        return

    file_code = args[0].strip()
    file_name = " ".join(args[1:]).strip() if len(args) > 1 else f"file_{file_code}"
    file_type = (
        "video" if any(file_name.lower().endswith(e)
                       for e in [".mp4", ".mkv", ".webm", ".avi", ".mov"])
        else "document"
    )

    msg = await update.message.reply_text("🔍 Verifying on SaveFiles...")

    info = savefiles_file_info(file_code)
    if not info:
        await msg.edit_text(
            f"❌ <b>File not found!</b>\n\n"
            f"Code: <code>{file_code}</code>\n"
            f"Make sure it's uploaded to savefiles.com",
            parse_mode="HTML"
        )
        return

    file_size  = int(info.get("size", 0))
    size_mb    = file_size / 1024 / 1024 if file_size else 0
    stream_url = f"https://xvs.tt/{file_code}"
    unique_id  = uuid.uuid4().hex[:16]

    save_savefiles_file(
        unique_id  = unique_id,
        file_code  = file_code,
        stream_url = stream_url,
        file_name  = file_name,
        file_size  = file_size,
        file_type  = file_type,
    )

    deep_link = make_sf_deep_link(unique_id)

    await msg.edit_text(
        f"✅ <b>File Added!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📁 <b>Name:</b> <code>{file_name}</code>\n"
        f"📦 <b>Size:</b> {size_mb:.1f} MB\n"
        f"🆔 <b>Code:</b> <code>{file_code}</code>\n\n"
        f"🔗 <b>Universal Share Link:</b>\n"
        f"<code>{deep_link}</code>\n\n"
        f"✅ This ONE link works on ALL bots!",
        parse_mode="HTML"
    )


@admin_only
async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a message with /broadcast to send it to all users.")
        return
    reply_msg = update.message.reply_to_message
    all_users = [u["user_id"] for u in users_col.find({}, {"user_id": 1})]
    status    = await update.message.reply_text(f"📡 Broadcasting to {len(all_users)} users...")
    success, failed = 0, 0
    for uid in all_users:
        try:
            await reply_msg.copy(chat_id=uid)
            success += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await status.edit_text(
        f"✅ Broadcast complete!\n✔️ Delivered: {success}\n❌ Failed: {failed}"
    )


@admin_only
async def handle_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Admin uploads file → SaveFiles → Universal link."""
    msg       = update.message
    user      = update.effective_user
    tg_file   = None
    file_type = "document"
    file_name = ""
    file_size = 0

    if msg.video:
        tg_file, file_type = msg.video, "video"
        file_name = msg.video.file_name or f"video_{int(time.time())}.mp4"
        file_size = msg.video.file_size or 0
    elif msg.document:
        tg_file, file_type = msg.document, "document"
        file_name = msg.document.file_name or f"file_{int(time.time())}"
        file_size = msg.document.file_size or 0
    elif msg.audio:
        tg_file, file_type = msg.audio, "audio"
        file_name = msg.audio.file_name or f"audio_{int(time.time())}.mp3"
        file_size = msg.audio.file_size or 0
    else:
        return

    size_mb         = file_size / 1024 / 1024 if file_size else 0
    MAX_TG_BOT_SIZE = 20 * 1024 * 1024  # 20 MB

    if file_size > MAX_TG_BOT_SIZE:
        await msg.reply_text(
            f"⚠️ <b>File too large ({size_mb:.0f} MB)!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📏 Bot API limit: 20 MB\n\n"
            f"✅ <b>Use this instead:</b>\n"
            f"┣ 1️⃣ Upload to savefiles.com manually\n"
            f"┣ 2️⃣ Copy the file code from URL\n"
            f"┗ 3️⃣ Send: <code>/sfadd FILE_CODE {file_name}</code>",
            parse_mode="HTML"
        )
        return

    processing = await msg.reply_text(
        f"⚡ <b>Processing...</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📁 <b>{file_name}</b>\n"
        f"📦 {size_mb:.1f} MB\n\n"
        f"⏳ Please wait...",
        parse_mode="HTML"
    )

    try:
        await processing.edit_text(f"⬇️ Downloading {size_mb:.1f} MB from Telegram...")
        file_obj   = await ctx.bot.get_file(tg_file.file_id)
        file_bytes = bytes(await file_obj.download_as_bytearray())

        await processing.edit_text(f"☁️ Uploading to SaveFiles.com...")
        result = await asyncio.get_event_loop().run_in_executor(
            None, lambda: upload_to_savefiles(file_bytes, file_name)
        )

        if not result:
            await processing.edit_text(
                "❌ <b>SaveFiles upload failed!</b>\n\n"
                "Please check SAVEFILES_API_KEY or try again.",
                parse_mode="HTML"
            )
            return

        unique_id = uuid.uuid4().hex[:16]
        save_savefiles_file(
            unique_id        = unique_id,
            file_code        = result["file_code"],
            stream_url       = result["stream_url"],
            file_name        = file_name,
            file_size        = file_size,
            file_type        = file_type,
            telegram_file_id = tg_file.file_id,
        )

        deep_link = make_sf_deep_link(unique_id)

        # Delete original message
        try:
            await ctx.bot.delete_message(msg.chat_id, msg.message_id)
        except Exception:
            pass

        await processing.edit_text(
            f"✅ <b>File Uploaded Successfully!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📁 <b>Name:</b> <code>{file_name}</code>\n"
            f"📦 <b>Size:</b> {size_mb:.1f} MB\n"
            f"🆔 <b>Code:</b> <code>{result['file_code']}</code>\n\n"
            f"🔗 <b>Universal Share Link:</b>\n"
            f"<code>{deep_link}</code>\n\n"
            f"✅ This ONE link works on ALL bots!\n"
            f"📌 Share only this link — nothing else needed.",
            parse_mode="HTML"
        )
        logger.info(f"Admin {user.id} uploaded: {file_name} → {result['file_code']}")

    except Exception as e:
        logger.error(f"Upload error: {e}")
        await processing.edit_text(
            f"❌ <b>Upload Failed!</b>\n\n<code>{str(e)[:200]}</code>",
            parse_mode="HTML"
        )


# ─────────────────────────────────────────────
#  BUILD APPLICATION
# ─────────────────────────────────────────────

def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    # Public
    app.add_handler(CommandHandler("start",         start))
    app.add_handler(CommandHandler("mystatus",       mystatus))
    app.add_handler(CommandHandler("referral",       referral_cmd))
    app.add_handler(CommandHandler("premium",        premium_cmd))
    app.add_handler(CommandHandler("utr",            utr_cmd))

    # Admin
    app.add_handler(CommandHandler("stats",          stats_cmd))
    app.add_handler(CommandHandler("sffiles",        sffiles_cmd))
    app.add_handler(CommandHandler("sfadd",          sfadd_cmd))
    app.add_handler(CommandHandler("broadcast",      broadcast_cmd))
    app.add_handler(CommandHandler("approve",        approve_cmd))
    app.add_handler(CommandHandler("reject",         reject_cmd))
    app.add_handler(CommandHandler("addpremium",     addpremium_cmd))
    app.add_handler(CommandHandler("removepremium",  removepremium_cmd))

    # File upload (admin only)
    app.add_handler(MessageHandler(
        filters.User(ADMIN_IDS) & (filters.VIDEO | filters.Document.ALL | filters.AUDIO),
        handle_upload
    ))

    # Callbacks
    app.add_handler(CallbackQueryHandler(callback_handler))

    return app


# ─────────────────────────────────────────────
#  WEBHOOK MODE
# ─────────────────────────────────────────────
_bot_application = None
_bot_loop: asyncio.AbstractEventLoop | None = None


def run_event_loop(loop: asyncio.AbstractEventLoop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


async def init_application():
    global _bot_application
    _bot_application = build_application()
    await _bot_application.initialize()
    await _bot_application.start()
    logger.info("✅ Bot application initialized (webhook mode)")


def process_update_sync(data: dict):
    global _bot_application, _bot_loop
    from telegram import Update as TGUpdate
    try:
        update = TGUpdate.de_json(data, _bot_application.bot)
        future = asyncio.run_coroutine_threadsafe(
            _bot_application.process_update(update),
            _bot_loop
        )
        future.result(timeout=60)
    except Exception as e:
        logger.error(f"Update processing error: {e}")


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    global _bot_application
    if _bot_application is None:
        abort(503)
    data = request.get_json(force=True, silent=True)
    if not data:
        abort(400)
    t = threading.Thread(target=process_update_sync, args=(data,), daemon=True)
    t.start()
    return "OK", 200


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    global _bot_loop

    logger.info("🚀 Starting FileBot (SaveFiles Edition)")

    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set!")
        return
    if not SAVEFILES_API_KEY:
        logger.warning("⚠️  SAVEFILES_API_KEY not set — uploads will fail!")
    if not ADMIN_IDS:
        logger.warning("⚠️  No ADMIN_IDS set!")

    # Start persistent event loop in background thread
    _bot_loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_event_loop, args=(_bot_loop,), daemon=True)
    loop_thread.start()
    logger.info("✅ Async event loop started")

    # Initialize bot application
    future = asyncio.run_coroutine_threadsafe(init_application(), _bot_loop)
    future.result(timeout=30)

    # Set webhook
    webhook_url = f"{BASE_URL}/webhook/{BOT_TOKEN}"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json={
                "url": webhook_url,
                "drop_pending_updates": True,
                "allowed_updates": ["message", "callback_query"]
            },
            timeout=15
        )
        result = resp.json()
        if result.get("ok"):
            logger.info(f"✅ Webhook set: {webhook_url}")
        else:
            logger.error(f"❌ Webhook failed: {result}")
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")

    logger.info(f"✅ Starting Flask on port {PORT}")
    flask_app.run(
        host="0.0.0.0",
        port=PORT,
        debug=False,
        use_reloader=False,
        threaded=True
    )


if __name__ == "__main__":
    main()
