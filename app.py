import asyncio
import atexit
import logging
import os  # <-- Reads Environment Variables
import sqlite3
import threading

import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS  # <-- Allows frontend to call API
from telegram import Update, WebAppInfo
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# Import the main function from your scraper script
try:
    from scraper import get_division_data
except ImportError:
    print("Error: scraper.py not found. Make sure it's in the same directory.")
    exit(1)

# --- Configuration ---
# Reads the secret keys and URL from Render's Environment Variables
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
MINI_APP_URL = os.environ.get("MINI_APP_URL")

SCRAPE_INTERVAL_MINUTES = 15
DB_FILE = "futsal_data.db"

# In-memory cache for fast API responses
LIVE_CACHE = {
    'players': [],
    'fixtures': [],
    'msl_ladder': [],
    'mslb_ladder': [],
    'last_updated': None
}

# Setup logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# --- Database Setup (SQLite) ---
def init_db():
    """Initializes the SQLite database tables."""
    with sqlite3.connect(DB_FILE) as conn:
        cursor = conn.cursor()
        # Stores user chat IDs for notifications
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS subscriptions (
            chat_id INTEGER PRIMARY KEY
        )
        ''')
        # Stores last known fixtures to check for changes
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS fixtures (
            id TEXT PRIMARY KEY, division TEXT, date_time TEXT,
            opponent TEXT, location TEXT, score TEXT, round TEXT
        )
        ''')
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
            # User blocked the bot, remove them from subscriptions
            with sqlite3.connect(DB_FILE) as conn:
                conn.cursor().execute("DELETE FROM subscriptions WHERE chat_id = ?", (chat_id,))
                conn.commit()
                logger.info(f"Removed unsubscribed user: {chat_id}")


# --- Background Scraper & Notifier Job ---
def scheduled_scraper_job(bot_instance):
    """
    This is the core job. It runs the scraper, compares data,
    and sends notifications to all subscribed users.
    """
    logger.info("--- [SCHEDULER]: Running scheduled scraper job... ---")

    try:
        # 1. Run the async scraper from scraper.py
        msl_data = asyncio.run(get_division_data('MSL'))
        mslb_data = asyncio.run(get_division_data('MSLB'))

        if not msl_data or not mslb_data:
            logger.warning("[SCHEDULER]: Scraping failed. Skipping update.")
            return

        # 2. Combine and update the live cache for API commands
        global LIVE_CACHE
        new_fixtures = msl_data.get('fixtures', []) + mslb_data.get('fixtures', [])
        LIVE_CACHE['players'] = msl_data.get('players', []) + mslb_data.get('players', [])
        LIVE_CACHE['fixtures'] = new_fixtures
        LIVE_CACHE['msl_ladder'] = msl_data.get('ladder', [])
        LIVE_CACHE['mslb_ladder'] = mslb_data.get('ladder', [])
        LIVE_CACHE['last_updated'] = pd.Timestamp.now().isoformat()

        logger.info(f"[SCHEDULER]: Live cache updated. Found {len(new_fixtures)} total fixtures.")

        # 3. Check for changed game times
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            old_fixtures_q = cursor.execute("SELECT id, date_time FROM fixtures").fetchall()
            old_fixtures_map = {row[0]: row[1] for row in old_fixtures_q}

            notifications_to_send = []

            for new_fix in new_fixtures:
                old_date_time = old_fixtures_map.get(new_fix['id'])

                if old_date_time and old_date_time != new_fix['date_time']:
                    # CHANGE DETECTED!
                    logger.info(f"*** CHANGE DETECTED for {new_fix['id']} ***")
                    title = f"🚨 Game Time Change: {new_fix['division']} 🚨"
                    body = (
                        f"Round {new_fix['round']} vs {new_fix['opponent']}\n\n"
                        f"WAS: {old_date_time}\n"
                        f"NOW: {new_fix['date_time']}"
                    )
                    notifications_to_send.append(f"{title}\n{body}")

                # Update or Insert the new fixture data
                cursor.execute(
                    "INSERT OR REPLACE INTO fixtures (id, division, date_time, opponent, location, score, round) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (new_fix.get('id'), new_fix.get('division'), new_fix.get('date_time'), new_fix.get('opponent'),
                     new_fix.get('location'), new_fix.get('score'), new_fix.get('round'))
                )

            conn.commit()

            # 4. Send all notifications
            if notifications_to_send:
                all_subs = cursor.execute("SELECT chat_id FROM subscriptions").fetchall()
                full_message = "\n\n".join(notifications_to_send)
                for (chat_id,) in all_subs:
                    # We must run the async send function in a new event loop
                    # because APScheduler runs in a separate thread.
                    asyncio.run(send_telegram_message(bot_instance, chat_id, full_message))

        logger.info("--- [SCHEDULER]: Job finished. ---")

    except Exception as e:
        logger.error(f"!!! [SCHEDULER]: CRITICAL ERROR during job: {e}")


# --- Flask API Server (Serves data to the React App) ---
app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app, resources={r"/api/*": {"origins": "*"}})  # Allow web app to call API


# --- API Routes ---
@app.route('/')
def serve_mini_app():
    """Serves the main index.html file for the Telegram Mini App."""
    return send_from_directory('public', 'index.html')


@app.route('/api/stats')
def get_all_stats():
    """Returns all data in one big JSON blob."""
    return jsonify(LIVE_CACHE)


@app.route('/api/players')
def get_players():
    """Returns player data, with optional sorting."""
    sort_by = request.args.get('sort', 'goals')
    order = request.args.get('order', 'desc')
    players = LIVE_CACHE.get('players', [])

    if not players:
        return jsonify({"error": "No player data available. Cache might be building."}), 503

    try:
        is_reverse = (order == 'desc')
        # Sort by the primary key (e.g., goals), and then by 'appearances' as a tie-breaker
        sorted_players = sorted(
            players,
            key=lambda p: (p.get(sort_by, 0), p.get('appearances', 0)),
            reverse=is_reverse
        )
        return jsonify(sorted_players)
    except Exception as e:
        return jsonify({"error": f"Invalid sort key: {e}"}), 400


@app.route('/api/fixtures')
def get_fixtures():
    """Returns all fixtures."""
    return jsonify(LIVE_CACHE.get('fixtures', []))


# --- Telegram Bot Logic ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command. Subscribes user and shows 'Open App' button."""
    chat_id = update.effective_chat.id

    # Save user to database
    with sqlite3.connect(DB_FILE) as conn:
        conn.cursor().execute("INSERT OR IGNORE INTO subscriptions (chat_id) VALUES (?)", (chat_id,))
        conn.commit()

    await update.message.reply_text(
        "Welcome to the Perth United Bot! ⚽️\n\n"
        "You are subscribed for game time updates.\n\n"
        "Click the 'Open App' button below to see stats, ladders, and the calendar.",
        reply_markup={
            "inline_keyboard": [
                [{"text": "🚀 Open Stats App", "web_app": {"url": MINI_APP_URL}}]
            ]
        }
    )


def start_bot_and_scheduler():
    """Initializes and starts the Telegram bot and the background scheduler."""

    if not TELEGRAM_BOT_TOKEN:
        logger.error("!!! ERROR: TELEGRAM_BOT_TOKEN environment variable is not set.")
        return
    if not MINI_APP_URL:
        logger.error("!!! ERROR: MINI_APP_URL environment variable is not set.")
        return

    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(
        scheduled_scraper_job,
        'interval',
        minutes=SCRAPE_INTERVAL_MINUTES,
        args=[application.bot]
    )
    scheduler.start()

    # Run the first scrape immediately in a background thread
    threading.Thread(target=scheduled_scraper_job, args=(application.bot,), daemon=True).start()
    atexit.register(lambda: scheduler.shutdown())

    application.add_handler(CommandHandler("start", start_command))

    print("Bot is polling...")
    application.run_polling()


# --- Main Startup Function ---
if __name__ == '__main__':
    init_db()

    # Run the Bot in a separate thread
    bot_thread = threading.Thread(target=asyncio.run, args=(start_bot_and_scheduler(),), daemon=True)
    bot_thread.start()

    print("Starting Flask API server on http://0.0.0.0:5000...")
    # Run Flask in the main thread (Render expects this)
    # Render's port is set by the PORT env var, default to 5000 for local
    port = int(os.environ.get('PORT', 5000))
    app.run(port=port, host='0.0.0.0', debug=False)
