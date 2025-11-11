# scraper.py
#
# This script contains all the logic to scrape and clean data
# for both the MSL and MSLB divisions for Perth United.

import asyncio
import logging
from io import StringIO  # Used to fix a Pandas FutureWarning

import pandas as pd
from playwright.async_api import async_playwright, Page, FrameLocator

# --- Configuration ---
BASE_URL = 'https://www.futsalsupaliga.com.au'
GLOBAL_TIMEOUT_MS = 90000  # 90 seconds for page operations
CONTENT_WAIT_TIMEOUT_MS = 60000  # 60 seconds for internal iframe content
FORCE_WAIT_MS = 20000  # Hard 20-second wait for page to settle

TABLE_INDICES = {
    'ladder': 0,
    'fixtures': 1,
    'player_stats': 2
}

# Column name maps based on your debug output
PLAYER_COL_MAP = {
    'Name': 'player_name',
    'Club': 'team_name',
    'Team': 'team_name',
    'Goals': 'goals',
    'Assists': 'assists',
    'Apps': 'appearances',
    'Appearances': 'appearances'
}

FIXTURE_COL_MAP = {
    'Round': 'round',
    'Date': 'date',
    'Time': 'time',
    'Home': 'home_team',
    'Away': 'away_team',
    'Score': 'score',
    'Result': 'score',
    'Location': 'location',
    'Venue': 'location'
}

# Standard browser user-agent to avoid being blocked
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"

# Get a logger instance
logger = logging.getLogger(__name__)


async def scrape_table_to_dataframe(page: Page, iframe_index: int) -> pd.DataFrame:
    """
    Finds a specific iframe by its index, waits for the table,
    and returns its content as a Pandas DataFrame.
    """

    frame_locator: FrameLocator = page.frame_locator(f".OQ8Tzd >> nth={iframe_index} >> iframe")
    table_selector = '#theTable'

    await frame_locator.locator(table_selector).wait_for(
        state='attached',
        timeout=CONTENT_WAIT_TIMEOUT_MS
    )

    await frame_locator.locator(f'{table_selector} tr').nth(1).wait_for(
        state='attached',
        timeout=15000
    )

    table_html = await frame_locator.locator(table_selector).inner_html()

    # Wrap the HTML string in a StringIO object
    df_list = pd.read_html(StringIO(f"<table>{table_html}</table>"))

    if not df_list:
        raise Exception("Pandas could not parse any tables from the iframe HTML.")

    return df_list[0]


async def get_division_data(division: str):
    """
    Main scraping function for one division (MSL or MSLB).
    It fetches and processes all three tables.
    """
    division_path = f"/{division.lower()}"
    player_team_filter = 'Perth United'
    fixture_team_name = 'Perth United' if division == 'MSL' else 'Perth United B'
    division_upper = division.upper()

    logger.info(f"--- Starting scrape for {division_upper} ---")

    browser = None
    async with async_playwright() as p:
        try:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(user_agent=USER_AGENT)
            page = await context.new_page()
            page.set_default_timeout(GLOBAL_TIMEOUT_MS)

            url = f'{BASE_URL}{division_path}'
            logger.info(f"Navigating to: {url}")
            await page.goto(url, wait_until='domcontentloaded')

            # --- THIS IS THE CRITICAL 20-SECOND WAIT ---
            logger.info(f"[{division_upper}] Page loaded. Waiting 20 seconds for dynamic content...")
            await page.wait_for_timeout(FORCE_WAIT_MS)
            logger.info(f"[{division_upper}] Wait complete. Starting iframe search.")
            # --- END OF WAIT ---

            tasks = [
                scrape_table_to_dataframe(page, TABLE_INDICES['ladder']),
                scrape_table_to_dataframe(page, TABLE_INDICES['fixtures']),
                scrape_table_to_dataframe(page, TABLE_INDICES['player_stats'])
            ]
            results_df = await asyncio.gather(*tasks)

            ladder_df_raw, fixtures_df_raw, players_df_raw = results_df

            logger.info(f"\n[{division_upper}] DEBUG: Ladder Headers Found: {list(ladder_df_raw.columns)}")
            logger.info(f"[{division_upper}] DEBUG: Fixtures Headers Found: {list(fixtures_df_raw.columns)}")
            logger.info(f"[{division_upper}] DEBUG: Player Stats Headers Found: {list(players_df_raw.columns)}\n")

            ladder_df = ladder_df_raw.copy()
            fixtures_df = fixtures_df_raw.copy()
            players_df = players_df_raw.copy()

            # --- 2. Clean Player Stats ---
            players_df.rename(columns=PLAYER_COL_MAP, inplace=True)

            if 'team_name' not in players_df.columns:
                raise KeyError(f"'team_name' key not found. Available: {list(players_df.columns)}")

            players_df = players_df[
                players_df['team_name'].str.contains(player_team_filter, case=False, na=False)].copy()

            for col in ['goals', 'assists', 'appearances', 'yellow_cards', 'red_cards']:
                if col in players_df.columns:
                    players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0).astype(int)
                else:
                    players_df[col] = 0
            players_df['division'] = division_upper

            # --- 3. Clean Fixtures ---
            fixtures_df.rename(columns=FIXTURE_COL_MAP, inplace=True)

            if 'home_team' not in fixtures_df.columns or 'away_team' not in fixtures_df.columns:
                raise KeyError(f"'home_team' or 'away_team' key not found. Available: {list(fixtures_df.columns)}")

            fixtures_df = fixtures_df[
                fixtures_df['home_team'].str.contains(fixture_team_name, case=False, na=False) |
                fixtures_df['away_team'].str.contains(fixture_team_name, case=False, na=False)
                ].copy()

            for col in ['date', 'time', 'round', 'location', 'score']:
                if col not in fixtures_df.columns: fixtures_df[col] = ''

            fixtures_df['date_time'] = fixtures_df['date'].fillna('') + ' ' + fixtures_df['time'].fillna('')
            fixtures_df['id'] = (
                    division_upper + '-' +
                    fixtures_df['home_team'].str.replace(' ', '', regex=False).fillna('') + '-' +
                    fixtures_df['away_team'].str.replace(' ', '', regex=False).fillna('') + '-' +
                    fixtures_df['date'].fillna('')
            )

            is_home_game = fixtures_df['home_team'].str.startswith(fixture_team_name)
            fixtures_df['opponent'] = fixtures_df['away_team']
            fixtures_df.loc[~is_home_game, 'opponent'] = fixtures_df['home_team']
            fixtures_df['division'] = division_upper

            logger.info(f"--- Successfully finished {division_upper} ---")

            return {
                'ladder': ladder_df.to_dict('records'),
                'fixtures': fixtures_df.to_dict('records'),
                'players': players_df.to_dict('records')
            }

        except Exception as e:
            logger.error(f"!!! CRITICAL ERROR scraping {division_path}: {e}", exc_info=True)
            return None
        finally:
            if browser:
                await browser.close()
