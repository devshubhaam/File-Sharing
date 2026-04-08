"""
Telegram File Sharing Bot — Direct Send Edition
No external storage. Pure Telegram file_id system.
Admin uploads → Bot saves file_id → Users get file directly in Telegram
"""

import os
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

from flask import Flask, request, jsonify, abort, redirect
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
TOKEN_VALIDITY_HOURS  = int(os.environ.get("TOKEN_VALIDITY_HOURS", "24"))
SHORTENER_API_KEY     = os.environ.get("SHORTENER_API_KEY", "")
SHORTENER_DOMAIN      = os.environ.get("SHORTENER_DOMAIN", "api.shrtco.de")

# Referral
REFERRAL_REWARD_HOURS  = int(os.environ.get("REFERRAL_REWARD_HOURS", "6"))
REFERRAL_PREMIUM_COUNT = int(os.environ.get("REFERRAL_PREMIUM_COUNT", "10"))

# Storage channel (optional — for permanent file_id)
STORAGE_CHANNEL_ID = os.environ.get("STORAGE_CHANNEL_ID", "")

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
        for k in [k for k, v in _active_users.items() if v < cutoff]:
            _active_users.pop(k, None)


def get_concurrent_users() -> int:
    with _active_lock:
        return sum(1 for t in _active_users.values() if t >= time.time() - 300)


# ─────────────────────────────────────────────
#  MONGODB
# ─────────────────────────────────────────────
mongo_client = MongoClient(
    MONGO_URI,
    serverSelectionTimeoutMS=5000,
    maxPoolSize=50, minPoolSize=5,
    retryWrites=True, retryReads=True,
)
db = mongo_client["filebot"]

users_col    = db[f"users_bot{SERVER_ID}"]
access_col   = db["user_access"]
files_col    = db["tg_files"]
payments_col = db["payments"]
referrals_col= db["referrals"]


def setup_indexes():
    users_col.create_index("user_id", unique=True)
    access_col.create_index("user_id", unique=True)
    access_col.create_index("token_expiry")
    access_col.create_index("premium_expiry")
    files_col.create_index("unique_id", unique=True)
    files_col.create_index("file_id")
    files_col.create_index("upload_time")
    payments_col.create_index("utr")
    payments_col.create_index([("user_id", ASCENDING), ("status", ASCENDING)])
    referrals_col.create_index(
        [("referrer_id", ASCENDING), ("referred_user_id", ASCENDING)],
        unique=True
    )
    logger.info(f"✅ MongoDB ready | Collection: users_bot{SERVER_ID}")


setup_indexes()

# ─────────────────────────────────────────────
#  DATABASE HELPERS
# ─────────────────────────────────────────────

def get_user(user_id: int) -> dict:
    user = users_col.find_one({"user_id": user_id})
    if not user:
        user = {"user_id": user_id, "referrals_count": 0,
                "referred_by": None, "joined_at": datetime.utcnow()}
        users_col.insert_one(user)
    return user


def get_access(user_id: int) -> dict:
    access = access_col.find_one({"user_id": user_id})
    if not access:
        access = {"user_id": user_id, "token_expiry": None, "premium_expiry": None}
        access_col.insert_one(access)
    return access


def is_premium(user_id: int) -> bool:
    exp = get_access(user_id).get("premium_expiry")
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
    base   = access.get("premium_expiry") or datetime.utcnow()
    if base < datetime.utcnow():
        base = datetime.utcnow()
    expiry = base + timedelta(days=days)
    access_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": expiry}},
        upsert=True
    )
    invalidate_token_cache(user_id)
    return expiry


def revoke_premium(user_id: int):
    access_col.update_one({"user_id": user_id}, {"$set": {"premium_expiry": None}}, upsert=True)
    invalidate_token_cache(user_id)


def is_bot_full() -> bool:
    return users_col.count_documents({}) >= MAX_USERS


def record_referral(referrer_id: int, referred_id: int) -> bool:
    if referrer_id == referred_id:
        return False
    try:
        referrals_col.insert_one({
            "referrer_id": referrer_id, "referred_user_id": referred_id,
            "timestamp": datetime.utcnow()
        })
        users_col.update_one({"user_id": referrer_id}, {"$inc": {"referrals_count": 1}})
        return True
    except DuplicateKeyError:
        return False


def check_referral_rewards(referrer_id: int) -> str:
    count = get_user(referrer_id).get("referrals_count", 0)
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
_token_cache_lock  = threading.Lock()


def has_valid_token(user_id: int) -> bool:
    if is_premium(user_id):
        return True
    now = datetime.utcnow()
    with _token_cache_lock:
        cached = _token_cache.get(user_id)
        if cached and now < cached[1]:
            return cached[0]
    result = _check_token_db(user_id)
    with _token_cache_lock:
        _token_cache[user_id] = (result, now + timedelta(seconds=60))
        if len(_token_cache) > 10000:
            cutoff = now - timedelta(minutes=5)
            for k in [k for k, v in _token_cache.items() if v[1] < cutoff]:
                _token_cache.pop(k, None)
    return result


def _check_token_db(user_id: int) -> bool:
    exp = get_access(user_id).get("token_expiry")
    return exp is not None and exp > datetime.utcnow()


def invalidate_token_cache(user_id: int):
    with _token_cache_lock:
        _token_cache.pop(user_id, None)


# ─────────────────────────────────────────────
#  FILE HELPERS
# ─────────────────────────────────────────────

def save_file(unique_id: str, file_id: str, file_name: str,
              file_size: int, file_type: str) -> dict:
    doc = {
        "unique_id":  unique_id,
        "file_id":    file_id,
        "file_name":  file_name,
        "file_size":  file_size,
        "file_type":  file_type,
        "upload_time": datetime.utcnow(),
        "view_count": 0,
    }
    files_col.update_one({"unique_id": unique_id}, {"$set": doc}, upsert=True)
    logger.info(f"✅ File saved: {file_name} | unique_id: {unique_id}")
    return doc


def get_file(unique_id: str) -> dict | None:
    return files_col.find_one({"unique_id": unique_id})


def make_deep_link(unique_id: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start=file_{unique_id}"


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
        except Exception:
            pass
        return long_url
    try:
        resp = requests.get(
            f"https://{SHORTENER_DOMAIN}/api?api={SHORTENER_API_KEY}&url={urllib.parse.quote(long_url)}",
            timeout=10
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("shortenedUrl") or data.get("short_link") or long_url
    except Exception:
        pass
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
#  FLASK
# ─────────────────────────────────────────────
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET


@flask_app.route("/verify/<int:user_id>")
def verify_redirect(user_id):
    return redirect(f"https://t.me/{BOT_USERNAME}?start=verify_{user_id}")


@flask_app.route("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.utcnow().isoformat()})


@flask_app.route("/")
def index():
    return f"<h2>🤖 FileBot</h2><p>Use @{BOT_USERNAME} on Telegram</p>"


# ─────────────────────────────────────────────
#  TELEGRAM HANDLERS
# ─────────────────────────────────────────────

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user    = update.effective_user
    payload = ctx.args[0] if ctx.args else ""

    track_active_user(user.id)

    # ── File request ─────────────────────────
    if payload.startswith("file_"):
        unique_id   = payload[5:]
        is_new_user = users_col.find_one({"user_id": user.id}) is None
        if is_new_user and is_bot_full() and NEXT_BOT_LINK:
            await send_redirect_message(update, ctx, file_id=unique_id)
            return
        get_user(user.id)
        await handle_file_request(update, ctx, unique_id)
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
                    rm = "🏆 You earned 7-day Premium!" if reward == "premium" else "⏰ You earned bonus token hours!"
                    await ctx.bot.send_message(
                        referrer_id,
                        f"🎉 <b>New Referral!</b>\n\n"
                        f"User joined via your link!\n{rm}",
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

    # ── New user — redirect if full ───────────
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

    await update.message.reply_text(
        f"👋 <b>Welcome, {user.first_name}!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🤖 <b>File Sharing Bot</b> — Server {SERVER_ID}\n\n"
        f"📌 <b>Commands:</b>\n"
        f"┣ 💎 /premium — Get premium access\n"
        f"┣ 👥 /referral — Your referral link\n"
        f"┗ 📊 /mystatus — Token &amp; premium status\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"🎁 Refer friends &amp; earn free premium!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Server {SERVER_ID}  •  "
        f"👤 {total_users}/{MAX_USERS}  •  "
        f"⚡ {concurrent} active",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu"),
            InlineKeyboardButton("📤 Share", url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}")
        ]])
    )


async def send_redirect_message(update: Update, ctx: ContextTypes.DEFAULT_TYPE, file_id: str = ""):
    user      = update.effective_user
    next_name = f"@{NEXT_BOT_NAME}" if NEXT_BOT_NAME else "our sister bot"

    if file_id and NEXT_BOT_NAME:
        deep_link = f"https://t.me/{NEXT_BOT_NAME}?start=file_{file_id}"
        text = (
            f"👋 <b>Hello {user.first_name}!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ This bot is currently full!\n\n"
            f"✅ Your file is ready on our sister bot.\n"
            f"Click below — opens automatically!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"🤖 Sister bot: <b>{next_name}</b>"
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Get My File Now", url=deep_link)]])
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
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("➡️ Join Sister Bot", url=NEXT_BOT_LINK)]])

    await update.message.reply_text(text, parse_mode="HTML", reply_markup=kb)


async def handle_file_request(update: Update, ctx: ContextTypes.DEFAULT_TYPE, unique_id: str):
    """Send file directly to user via Telegram file_id."""
    user    = update.effective_user
    tg_file = get_file(unique_id)

    if not tg_file:
        await update.message.reply_text(
            "❌ <b>File not found!</b>\n\nThis link may be invalid.",
            parse_mode="HTML"
        )
        return

    # Check token
    if not has_valid_token(user.id):
        await send_verification_prompt(update, ctx, unique_id)
        return

    file_id   = tg_file["file_id"]
    file_name = tg_file.get("file_name", "file")
    file_type = tg_file.get("file_type", "document")
    file_size = tg_file.get("file_size", 0)
    size_mb   = file_size / 1024 / 1024 if file_size else 0

    # Track views
    files_col.update_one({"unique_id": unique_id}, {"$inc": {"view_count": 1}})
    views = tg_file.get("view_count", 0) + 1

    msg = await update.message.reply_text("⏳ Sending file...")

    try:
        caption = (
            f"📁 <b>{file_name}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📦 Size: {size_mb:.1f} MB\n"
            f"👁️ Views: {views}\n"
            f"🤖 Served by Bot {SERVER_ID}"
        )

        if file_type == "video":
            await ctx.bot.send_video(
                chat_id=user.id,
                video=file_id,
                caption=caption,
                parse_mode="HTML",
                supports_streaming=True,
            )
        elif file_type == "audio":
            await ctx.bot.send_audio(
                chat_id=user.id,
                audio=file_id,
                caption=caption,
                parse_mode="HTML",
            )
        else:
            await ctx.bot.send_document(
                chat_id=user.id,
                document=file_id,
                caption=caption,
                parse_mode="HTML",
            )
        await msg.delete()
        logger.info(f"✅ File sent: {file_name} → user {user.id}")

    except Exception as e:
        logger.error(f"File send error: {e}")
        await msg.edit_text(
            f"❌ <b>Failed to send file!</b>\n\n"
            f"Error: <code>{str(e)[:100]}</code>",
            parse_mode="HTML"
        )


async def send_verification_prompt(update: Update, ctx: ContextTypes.DEFAULT_TYPE, unique_id: str):
    user       = update.effective_user
    verify_url = f"{BASE_URL}/verify/{user.id}"
    short_url  = shorten_url(verify_url)

    await update.message.reply_text(
        "🔐 <b>Verification Required</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━\n\n"
        "⚠️ You need to verify to access this file.\n\n"
        "📋 <b>Simple Steps:</b>\n"
        "┣ 1️⃣ Click <b>Verify Now</b> below\n"
        "┣ 2️⃣ Complete the short page\n"
        "┗ 3️⃣ Come back &amp; click file link again\n\n"
        "━━━━━━━━━━━━━━━━━━━━━\n"
        "⏳ Token valid for <b>24 hours</b>\n"
        "💎 Get Premium to skip forever!",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Verify Now", url=short_url),
            InlineKeyboardButton("💎 Get Premium", callback_data="premium_menu")
        ]])
    )


async def mystatus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    user      = update.effective_user
    db_user   = get_user(user.id)
    access    = get_access(user.id)
    token_exp = access.get("token_expiry")
    prem_exp  = access.get("premium_expiry")
    refs      = db_user.get("referrals_count", 0)
    joined    = db_user.get("joined_at", datetime.utcnow())
    joined_str= joined.strftime("%d %b %Y") if hasattr(joined, "strftime") else str(joined)[:10]

    if token_exp and token_exp > datetime.utcnow():
        remaining = token_exp - datetime.utcnow()
        h, rem = divmod(int(remaining.total_seconds()), 3600)
        token_str = f"✅ Valid  •  ⏳ {h}h {rem//60}m left"
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
    refs     = get_user(user.id).get("referrals_count", 0)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    next_mil = max(0, REFERRAL_PREMIUM_COUNT - refs)

    await update.message.reply_text(
        f"👥 <b>Referral Program</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔗 <b>Your Referral Link:</b>\n"
        f"<code>{ref_link}</code>\n\n"
        f"📊 <b>Your Stats:</b>\n"
        f"┣ 👤 Total Referred: <b>{refs}</b>\n"
        f"┗ 🎯 Need <b>{next_mil} more</b> for 7-day Premium\n\n"
        f"🎁 <b>Rewards:</b>\n"
        f"┣ ⏰ Every 5 referrals → Bonus token hours\n"
        f"┗ 🏆 {REFERRAL_PREMIUM_COUNT} referrals → 7-day Premium FREE!\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📤 Share now and start earning! 🚀",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("📤 Share Now",
                url=f"https://t.me/share/url?url={urllib.parse.quote(ref_link)}&text=Join+this+amazing+bot!")
        ]])
    )


async def premium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await show_premium_menu(update.message)


async def show_premium_menu(msg_or_query):
    plan_lines = "\n".join(
        [f"┣ 💎 <b>{v['label']}</b>  —  ₹{v['price']}" for v in list(PREMIUM_PLANS.values())[:-1]] +
        [f"┗ 👑 <b>{list(PREMIUM_PLANS.values())[-1]['label']}</b>  —  ₹{list(PREMIUM_PLANS.values())[-1]['price']}"]
    )
    text = (
        f"💎 <b>Premium Access</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📦 <b>Available Plans:</b>\n"
        f"{plan_lines}\n\n"
        f"✅ <b>Premium Benefits:</b>\n"
        f"┣ ⚡ Skip daily verification\n"
        f"┣ ♾️ Unlimited file access\n"
        f"┣ 🚀 Priority support\n"
        f"┗ 🎯 Access all bots\n\n"
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
        await update.message.reply_text("❌ Invalid UTR format.")
        return
    if payments_col.find_one({"utr": utr}):
        await update.message.reply_text("❌ This UTR has already been submitted.")
        return

    plan_key = ctx.user_data.get("pending_plan", "1month")
    plan     = PREMIUM_PLANS.get(plan_key, PREMIUM_PLANS["1month"])
    payments_col.insert_one({
        "user_id": user.id, "username": user.username or "N/A",
        "utr": utr, "plan": plan_key, "price": plan["price"],
        "status": "pending", "created_at": datetime.utcnow()
    })

    await update.message.reply_text(
        f"✅ <b>Payment Submitted!</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🔢 <b>UTR:</b> <code>{utr}</code>\n"
        f"📦 <b>Plan:</b> {plan['label']}\n"
        f"💰 <b>Amount:</b> ₹{plan['price']}\n\n"
        f"⏳ Admin will verify within a few hours\n"
        f"🔔 You will be notified once approved",
        parse_mode="HTML"
    )
    for admin_id in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                admin_id,
                f"🔔 <b>New Payment Request!</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👤 <b>User:</b> <code>{user.id}</code>\n"
                f"📛 <b>Username:</b> @{user.username or 'N/A'}\n"
                f"📦 <b>Plan:</b> {plan['label']}\n"
                f"💰 <b>Amount:</b> ₹{plan['price']}\n"
                f"🔢 <b>UTR:</b> <code>{utr}</code>\n\n"
                f"✅ Approve: <code>/approve {user.id}</code>\n"
                f"❌ Reject:  <code>/reject {user.id}</code>",
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
    uid     = int(args[0])
    payment = payments_col.find_one({"user_id": uid, "status": "pending"}, sort=[("created_at", -1)])
    if not payment:
        await update.message.reply_text(f"No pending payment for {uid}.")
        return
    plan   = PREMIUM_PLANS.get(payment.get("plan", "1month"), PREMIUM_PLANS["1month"])
    expiry = grant_premium(uid, plan["days"])
    payments_col.update_one({"_id": payment["_id"]}, {"$set": {"status": "approved"}})
    await update.message.reply_text(f"✅ Approved! User {uid} has premium until {expiry.strftime('%d %b %Y')}.")
    try:
        await ctx.bot.send_message(uid,
            f"🎉 <b>Payment Approved!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"✅ <b>{plan['label']} Premium</b> activated!\n"
            f"📅 Expires: {expiry.strftime('%d %b %Y')}\n\n"
            f"🚀 Enjoy unlimited access!", parse_mode="HTML")
    except Exception:
        pass


@admin_only
async def reject_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("Usage: /reject <user_id>")
        return
    uid     = int(args[0])
    payment = payments_col.find_one({"user_id": uid, "status": "pending"}, sort=[("created_at", -1)])
    if not payment:
        await update.message.reply_text(f"No pending payment for {uid}.")
        return
    payments_col.update_one({"_id": payment["_id"]}, {"$set": {"status": "rejected"}})
    await update.message.reply_text(f"❌ Rejected payment for user {uid}.")
    try:
        await ctx.bot.send_message(uid,
            "❌ <b>Payment Rejected</b>\n\n"
            "Your payment could not be verified.\n"
            "Contact admin for help.", parse_mode="HTML")
    except Exception:
        pass


@admin_only
async def addpremium_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if len(args) < 2:
        await update.message.reply_text("Usage: /addpremium <user_id> <days>")
        return
    uid, days = int(args[0]), int(args[1])
    expiry = grant_premium(uid, days)
    await update.message.reply_text(f"✅ Added {days}-day premium to {uid}. Expires: {expiry.strftime('%d %b %Y')}")
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
    revoke_premium(int(args[0]))
    await update.message.reply_text(f"✅ Premium removed from {args[0]}.")


@admin_only
async def stats_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    total_users  = users_col.count_documents({})
    prem_count   = access_col.count_documents({"premium_expiry": {"$gt": datetime.utcnow()}})
    total_files  = files_col.count_documents({})
    pending_pay  = payments_col.count_documents({"status": "pending"})
    total_refs   = referrals_col.count_documents({})
    concurrent   = get_concurrent_users()
    slots_left   = max(0, MAX_USERS - total_users)
    load_pct     = int((total_users / MAX_USERS) * 100) if MAX_USERS else 0
    bar          = "█" * int(load_pct / 10) + "░" * (10 - int(load_pct / 10))

    await update.message.reply_text(
        f"📊 <b>Bot Statistics</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🤖 <b>Server {SERVER_ID}</b>\n\n"
        f"👥 <b>Users</b>\n"
        f"┣ 👤 Total: <b>{total_users}/{MAX_USERS}</b>\n"
        f"┣ 💎 Premium: <b>{prem_count}</b>\n"
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
async def files_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    file_list = list(files_col.find().sort("upload_time", -1).limit(10))
    if not file_list:
        await update.message.reply_text("No files uploaded yet.")
        return
    lines = ["📁 <b>Last 10 Files:</b>\n"]
    for f in file_list:
        link    = make_deep_link(f["unique_id"])
        size_mb = (f.get("file_size", 0) or 0) / 1024 / 1024
        views   = f.get("view_count", 0)
        lines.append(
            f"▪️ <b>{f.get('file_name', '?')}</b>\n"
            f"   📦 {size_mb:.1f} MB • 👁️ {views} views\n"
            f"   <code>{link}</code>"
        )
    await update.message.reply_text(
        "\n\n".join(lines), parse_mode="HTML", disable_web_page_preview=True
    )


@admin_only
async def broadcast_cmd(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a message with /broadcast to send to all users.")
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
    await status.edit_text(f"✅ Broadcast done!\n✔️ Delivered: {success}\n❌ Failed: {failed}")


@admin_only
async def handle_upload(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """
    Admin sends file → Bot saves file_id → Returns share link.
    No download, no external storage — pure Telegram!
    Works for ANY file size Telegram allows.
    """
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

    size_mb   = file_size / 1024 / 1024 if file_size else 0
    processing = await msg.reply_text(
        f"⚡ <b>Saving file...</b>\n\n📁 {file_name}\n📦 {size_mb:.1f} MB",
        parse_mode="HTML"
    )

    try:
        file_id   = tg_file.file_id
        unique_id = uuid.uuid4().hex[:16]

        # If storage channel configured — forward there for permanent file_id
        if STORAGE_CHANNEL_ID:
            try:
                if file_type == "video":
                    sent = await ctx.bot.send_video(
                        chat_id=int(STORAGE_CHANNEL_ID),
                        video=file_id,
                        caption=file_name,
                        supports_streaming=True
                    )
                    file_id = sent.video.file_id
                elif file_type == "audio":
                    sent = await ctx.bot.send_audio(
                        chat_id=int(STORAGE_CHANNEL_ID),
                        audio=file_id,
                        caption=file_name
                    )
                    file_id = sent.audio.file_id
                else:
                    sent = await ctx.bot.send_document(
                        chat_id=int(STORAGE_CHANNEL_ID),
                        document=file_id,
                        caption=file_name
                    )
                    file_id = sent.document.file_id
                logger.info(f"✅ Forwarded to storage channel: {file_id[:30]}")
            except Exception as e:
                logger.warning(f"Storage channel forward failed: {e} — using original file_id")

        # Save to DB
        save_file(unique_id, file_id, file_name, file_size, file_type)

        deep_link = make_deep_link(unique_id)

        # Delete original message
        try:
            await ctx.bot.delete_message(msg.chat_id, msg.message_id)
        except Exception:
            pass

        await processing.edit_text(
            f"✅ <b>File Saved!</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📁 <b>Name:</b> <code>{file_name}</code>\n"
            f"📦 <b>Size:</b> {size_mb:.1f} MB\n\n"
            f"🔗 <b>Share Link:</b>\n"
            f"<code>{deep_link}</code>\n\n"
            f"✅ This ONE link works on ALL bots!\n"
            f"📌 Share only this link.",
            parse_mode="HTML"
        )
        logger.info(f"Admin {user.id} uploaded: {file_name}")

    except Exception as e:
        logger.error(f"Upload error: {e}")
        await processing.edit_text(
            f"❌ <b>Failed!</b>\n\n<code>{str(e)[:200]}</code>",
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
    app.add_handler(CommandHandler("files",          files_cmd))
    app.add_handler(CommandHandler("broadcast",      broadcast_cmd))
    app.add_handler(CommandHandler("approve",        approve_cmd))
    app.add_handler(CommandHandler("reject",         reject_cmd))
    app.add_handler(CommandHandler("addpremium",     addpremium_cmd))
    app.add_handler(CommandHandler("removepremium",  removepremium_cmd))

    # File upload (admin)
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
    logger.info("✅ Bot initialized (webhook mode)")


def process_update_sync(data: dict):
    global _bot_application, _bot_loop
    from telegram import Update as TGUpdate
    try:
        update = TGUpdate.de_json(data, _bot_application.bot)
        future = asyncio.run_coroutine_threadsafe(
            _bot_application.process_update(update), _bot_loop
        )
        future.result(timeout=60)
    except Exception as e:
        logger.error(f"Update error: {e}")


@flask_app.route(f"/webhook/{BOT_TOKEN}", methods=["POST"])
def webhook():
    if _bot_application is None:
        abort(503)
    data = request.get_json(force=True, silent=True)
    if not data:
        abort(400)
    threading.Thread(target=process_update_sync, args=(data,), daemon=True).start()
    return "OK", 200


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    global _bot_loop

    logger.info("🚀 Starting FileBot (Direct Telegram Edition)")

    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN not set!")
        return
    if not ADMIN_IDS:
        logger.warning("⚠️  No ADMIN_IDS set!")

    _bot_loop = asyncio.new_event_loop()
    threading.Thread(target=run_event_loop, args=(_bot_loop,), daemon=True).start()
    logger.info("✅ Event loop started")

    future = asyncio.run_coroutine_threadsafe(init_application(), _bot_loop)
    future.result(timeout=30)

    webhook_url = f"{BASE_URL}/webhook/{BOT_TOKEN}"
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook",
            json={"url": webhook_url, "drop_pending_updates": True,
                  "allowed_updates": ["message", "callback_query"]},
            timeout=15
        )
        if resp.json().get("ok"):
            logger.info(f"✅ Webhook set: {webhook_url}")
        else:
            logger.error(f"❌ Webhook failed: {resp.json()}")
    except Exception as e:
        logger.error(f"❌ Webhook error: {e}")

    logger.info(f"✅ Flask starting on port {PORT}")
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False, threaded=True)


if __name__ == "__main__":
    main()
