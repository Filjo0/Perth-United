# app.py
#
# This is the main server for your app. It is a Flask API,
# a Telegram Bot, and a Scraper Scheduler all in one.

import asyncio
import atexit
import logging
import os
import re
import sqlite3
import threading

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from telegram import Update, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes

# Import the main function from your scraper script
try:
    from scraper import get_division_data
except ImportError:
    print("Error: scraper.py not found. Make sure it's in the same directory.")
    exit(1)

# --- Configuration ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
MINI_APP_URL = os.environ.get("MINI_APP_URL")
SCRAPE_INTERVAL_MINUTES = 15
DB_FILE = "futsal_data.db"

LIVE_CACHE = {
    'players': [],
    'fixtures': [],
    'msl_ladder': [],
    'mslb_ladder': [],
    'last_updated': None
}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# --- Database Setup (SQLite) ---
def init_db():
    with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            chat_id INTEGER PRIMARY KEY
        )
        ''')
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fixtures (
            id TEXT PRIMARY KEY, division TEXT, date_time TEXT,
            opponent TEXT, location TEXT, score TEXT, round TEXT
        )
        ''')
        conn.commit()


# --- Push Notification Logic ---
async def send_telegram_message(bot, chat_id, message_text):
    try:
        await bot.send_message(chat_id=chat_id, text=message_text)
        logger.info(f"Sent message to {chat_id}")
    except Exception as e:
        logger.error(f"Failed to send message to {chat_id}: {e}")
        if "bot was blocked by the user" in str(e):
            with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
                conn.cursor().execute("DELETE FROM subscriptions WHERE chat_id = ?", (chat_id,))
                conn.commit()
                logger.info(f"Removed unsubscribed user: {chat_id}")


# --- Background Scraper & Notifier Job ---
def scheduled_scraper_job(application_instance: Application):
    logger.info("--- [SCHEDULER]: Running scheduled scraper job... ---")
    bot_instance = application_instance.bot

    try:
        msl_data = asyncio.run(get_division_data('MSL'))
        mslb_data = asyncio.run(get_division_data('MSLB'))

        if not msl_data or not mslb_data:
            logger.warning("[SCHEDULER]: Scraping failed. Skipping update.")
            return

        global LIVE_CACHE
        new_fixtures = msl_data.get('fixtures', []) + mslb_data.get('fixtures', [])
        LIVE_CACHE['players'] = msl_data.get('players', []) + mslb_data.get('players', [])
        LIVE_CACHE['fixtures'] = new_fixtures
        LIVE_CACHE['msl_ladder'] = msl_data.get('ladder', [])
        LIVE_CACHE['mslb_ladder'] = mslb_data.get('ladder', [])
        LIVE_CACHE['last_updated'] = pd.Timestamp.now().isoformat()

        logger.info(f"[SCHEDULER]: Live cache updated. Found {len(new_fixtures)} total fixtures.")

        with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
            cursor = conn.cursor()
            old_fixtures_q = cursor.execute("SELECT id, date_time FROM fixtures").fetchall()
            old_fixtures_map = {row[0]: row[1] for row in old_fixtures_q}

            notifications_to_send = []

            for new_fix in new_fixtures:
                score_str = str(new_fix.get('score', '')).strip().lower()
                is_played = re.search(r'\d+\s*-\s*\d+', score_str)
                if is_played:
                    continue

                old_date_time = old_fixtures_map.get(new_fix['id'])

                if old_date_time and old_date_time != new_fix['date_time']:
                    logger.info(f"*** CHANGE DETECTED for {new_fix['id']} ***")
                    title = f"🚨 Game Time Change: {new_fix['division']} 🚨"
                    body = (
                        f"Round {new_fix['round']} vs {new_fix['opponent']}\n\n"
                        f"WAS: {old_date_time}\n"
                        f"NOW: {new_fix['date_time']}"
                    )
                    notifications_to_send.append(f"{title}\n{body}")

                cursor.execute(
                    "INSERT OR REPLACE INTO fixtures (id, division, date_time, opponent, location, score, round) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (new_fix.get('id'), new_fix.get('division'), new_fix.get('date_time'), new_fix.get('opponent'),
                     new_fix.get('location'), new_fix.get('score'), new_fix.get('round'))
                )

            conn.commit()

            if notifications_to_send:
                all_subs = cursor.execute("SELECT chat_id FROM subscriptions").fetchall()
                full_message = "\n\n".join(notifications_to_send)
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                for (chat_id,) in all_subs:
                    loop.run_until_complete(send_telegram_message(bot_instance, chat_id, full_message))
                loop.close()

        logger.info("--- [SCHEDULER]: Job finished. ---")

    except Exception as e:
        logger.error(f"!!! [SCHEDULER]: CRITICAL ERROR during job: {e}", exc_info=True)


# --- Flask API Server (Serves data to the React App) ---
app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app, resources={r"/api/*": {"origins": "*"}})


# --- API Routes ---
@app.route('/')
def serve_mini_app():
    return send_from_directory('public', 'index.html')


@app.route('/api/stats')
def get_all_stats():
    all_fixtures = LIVE_CACHE.get('fixtures', [])
    upcoming_fixtures = []
    for f in all_fixtures:
        score_str = str(f.get('score', '')).strip().lower()
        is_played = re.search(r'\d+\s*-\s*\d+', score_str)
        if not is_played:
            upcoming_fixtures.append(f)

    response = {
        "players": LIVE_CACHE.get('players', []),
        "fixtures": upcoming_fixtures,
        "msl_ladder": LIVE_CACHE.get('msl_ladder', []),
        "mslb_ladder": LIVE_CACHE.get('mslb_ladder', []),
        "last_updated": LIVE_CACHE.get('last_updated')
    }
    return jsonify(response)


@app.route('/health')
def health_check():
    return jsonify({
        "status": "ok",
        "last_updated": LIVE_CACHE.get('last_updated'),
        "players_cached": len(LIVE_CACHE.get('players', [])),
        "fixtures_cached": len(LIVE_CACHE.get('fixtures', []))
    })


# --- Telegram Bot Logic ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
        conn.cursor().execute("INSERT OR IGNORE INTO subscriptions (chat_id) VALUES (?)", (chat_id,))
        conn.commit()

    app_url = os.environ.get("MINI_APP_URL", "")
    if not app_url:
        logger.error("Cannot show button, MINI_APP_URL is not set.")
        await update.message.reply_text("Bot is not fully configured. Please contact admin.")
        return

    await update.message.reply_text(
        "Welcome to the Perth United Bot! ⚽️\n\n"
        "You are subscribed for game time updates.\n\n"
        "Click the 'Open App' button below to see stats, ladders, and the calendar.",
        reply_markup={
            "inline_keyboard": [
                [{"text": "🚀 Open Stats App", "web_app": {"url": app_url}}]
            ]
        }
    )


async def start_bot_and_scheduler():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    app_url = os.environ.get("MINI_APP_URL")

    if not token:
        logger.error("!!! ERROR: TELEGRAM_BOT_TOKEN environment variable is not set. Bot thread cannot start.")
        return
    if not app_url:
        logger.error("!!! ERROR: MINI_APP_URL environment variable is not set. Bot thread cannot start.")
        return

    application = Application.builder().token(token).build()

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        scheduled_scraper_job,
        'interval',
        minutes=SCRAPE_INTERVAL_MINUTES,
        args=[application]
    )
    scheduler.start()

    # Run the first scrape immediately in a non-blocking way
    application.job_queue.run_once(lambda ctx: asyncio.create_task(scheduled_scraper_job(application)), 5)

    atexit.register(lambda: scheduler.shutdown())
    application.add_handler(CommandHandler("start", start_command))

    logger.info("Bot is polling...")
    # --- THIS IS THE FIX ---
    # Tell the bot not to listen for signals, since it's in a thread
    await application.run_polling(stop_signals=None)


def run_bot_in_thread():
    """Sets up a new event loop for the bot in its own thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_bot_and_scheduler())


# --- Main Startup Function ---
if __name__ == '__main__':
    init_db()

    bot_thread = threading.Thread(target=run_bot_in_thread, daemon=True)
    bot_thread.start()

    logger.info("Starting Flask API server...")
    # Render's port is set by the PORT env var, default to 10000
    port = int(os.environ.get('PORT', 10000))
    app.run(port=port, host='0.0.0.0', debug=False)
