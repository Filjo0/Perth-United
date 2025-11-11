# app.py
#
# FINAL STABLE VERSION (v5)
# This version fixes all threading and asyncio errors.
# - Runs Flask (via Uvicorn) and the Telegram Bot in the SAME event loop.
# - Uses the bot's built-in JobQueue instead of APScheduler.
# - Fixes 'lxml' and 'job-queue' dependencies.

import asyncio
import logging
import os
import re
import sqlite3

import pandas as pd
import uvicorn  # For running Flask asynchronously
from flask import Flask, jsonify, send_from_directory
from flask_cors import CORS
from telegram import Update
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
    'players': [], 'fixtures': [], 'msl_ladder': [], 'mslb_ladder': [], 'last_updated': None
}

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# --- Database Setup (SQLite) ---
def init_db():
    # check_same_thread=False is crucial for this environment
    with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS subscriptions (chat_id INTEGER PRIMARY KEY)''')
        cursor.execute('''CREATE TABLE IF NOT EXISTS fixtures (
            id TEXT PRIMARY KEY, division TEXT, date_time TEXT,
            opponent TEXT, location TEXT, score TEXT, round TEXT
        )''')
        conn.commit()


# --- Push Notification Logic ---
async def send_telegram_message(bot, chat_id, message_text):
    """Utility function to send a message to a specific user."""
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
async def scheduled_scraper_job(context: ContextTypes.DEFAULT_TYPE):
    """
    This is the core job, run by the bot's JobQueue.
    It runs the scraper, compares data, and sends notifications.
    """
    logger.info("--- [SCHEDULER]: Running scheduled scraper job... ---")

    application_instance = context.application

    try:
        msl_data = await get_division_data('MSL')
        mslb_data = await get_division_data('MSLB')

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
                    continue  # Skip played games

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
                for (chat_id,) in all_subs:
                    # We are already in an async job, so just await
                    await send_telegram_message(application_instance.bot, chat_id[0], full_message)

        logger.info("--- [SCHEDULER]: Job finished. ---")

    except Exception as e:
        logger.error(f"!!! [SCHEDULER]: CRITICAL ERROR during job: {e}", exc_info=True)


# --- Flask API Server ---
app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app, resources={r"/api/*": {"origins": "*"}})


@app.route('/')
def serve_mini_app():
    return send_from_directory('public', 'index.html')


@app.route('/api/stats')
def get_all_stats():
    all_fixtures = LIVE_CACHE.get('fixtures', [])
    upcoming_fixtures = [f for f in all_fixtures if not re.search(r'\d+\s*-\s*\d+', str(f.get('score', '')).strip())]

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
    """A simple health check endpoint for Render."""
    return jsonify({
        "status": "ok",
        "last_updated": LIVE_CACHE.get('last_updated'),
        "players_cached": len(LIVE_CACHE.get('players', [])),
        "fixtures_cached": len(LIVE_CACHE.get('fixtures', []))
    })


# --- Telegram Bot Logic ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
        conn.execute("INSERT OR IGNORE INTO subscriptions (chat_id) VALUES (?)", (chat_id,))
        conn.commit()

    await update.message.reply_text(
        "Welcome to Perth United Stats! ⚽️",
        reply_markup={
            "inline_keyboard": [[{
                "text": "🚀 Open App",
                "web_app": {"url": MINI_APP_URL}
            }]]
        }
    )


# --- Main Startup Function ---
async def main():
    """Starts the Flask server and the Telegram bot."""

    if not TELEGRAM_BOT_TOKEN:
        logger.error("!!! ERROR: TELEGRAM_BOT_TOKEN environment variable is not set.")
        return
    if not MINI_APP_URL or "your-frontend-app-url.com" in MINI_APP_URL:
        logger.error("!!! ERROR: MINI_APP_URL environment variable is not set.")
        return

    # Initialize the DB
    init_db()

    # Create the Telegram Application
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # Add the /start command handler
    application.add_handler(CommandHandler("start", start))

    # --- Schedule the scraper job using the bot's job queue ---
    job_queue = application.job_queue
    if job_queue:
        job_queue.run_once(scheduled_scraper_job, 5)  # Run 5 seconds after startup
        job_queue.run_repeating(scheduled_scraper_job, interval=SCRAPE_INTERVAL_MINUTES * 60, first=5)
    else:
        logger.error("JobQueue is not available. Check requirements.txt for [job-queue]")

    # --- Start the Flask server as an async task ---
    # Render's PORT env var is 10000 by default
    port = int(os.environ.get('PORT', 10000))

    # Use Uvicorn to run Flask (app) as an async-compatible server
    # We must wrap the Flask app (WSGI) in an ASGI middleware
    asgi_app = WSGIMiddleware(app)
    config = uvicorn.Config(asgi_app, host="0.0.0.0", port=port, log_level="info")
    server = uvicorn.Server(config)

    logger.info(f"Starting Flask/Uvicorn server on port {port}...")

    # --- This is the fix for the event loop crash ---

    # 1. Initialize the bot (this creates the job_queue)
    await application.initialize()

    # 2. Start the job queue (which runs the scheduler)
    if application.job_queue:
        await application.job_queue.start()

    # 3. Start the Flask/Uvicorn server task
    server_task = asyncio.create_task(server.serve())

    # 4. Start the bot polling task (non-blocking)
    await application.updater.start_polling(drop_pending_updates=True)

    # Wait for both tasks to run
    await asyncio.gather(server_task)


if __name__ == '__main__':
    asyncio.run(main())
