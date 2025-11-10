# scraper.py
#
# This script contains all the logic to scrape and clean data
# for both the MSL and MSLB divisions for Perth United.

import asyncio
from io import StringIO  # Used to fix a Pandas FutureWarning

import pandas as pd
from playwright.async_api import async_playwright

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


async def scrape_table_to_dataframe(page, iframe_index: int) -> pd.DataFrame:
    """
    Finds a specific iframe by its index, waits for the table,
    and returns its content as a Pandas DataFrame.
    """

    frame_locator = page.frame_locator(f".OQ8Tzd >> nth={iframe_index} >> iframe")
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

    # --- THIS IS THE FIX ---
    # Both player stats and fixtures seem to use "Perth United" for both teams.
    # We will filter by this common name and use the 'division' field to tell them apart.
    team_name_filter = 'Perth United'
    # --- End of Fix ---

    division_upper = division.upper()

    print(f"--- Starting scrape for {division_upper} ---")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        page.set_default_timeout(GLOBAL_TIMEOUT_MS)

        url = f'{BASE_URL}{division_path}'
        print(f"Navigating to: {url}")
        await page.goto(url, wait_until='domcontentloaded')

        print(f"[{division_upper}] Page loaded. Waiting 20 seconds for dynamic content...")
        await page.wait_for_timeout(FORCE_WAIT_MS)
        print(f"[{division_upper}] Wait complete. Starting iframe search.")

        try:
            tasks = [
                scrape_table_to_dataframe(page, TABLE_INDICES['ladder']),
                scrape_table_to_dataframe(page, TABLE_INDICES['fixtures']),
                scrape_table_to_dataframe(page, TABLE_INDICES['player_stats'])
            ]
            results_df = await asyncio.gather(*tasks)

            ladder_df_raw = results_df[0]
            fixtures_df_raw = results_df[1]
            players_df_raw = results_df[2]

            print(f"\n[{division_upper}] DEBUG: Ladder Headers Found: {list(ladder_df_raw.columns)}")
            print(f"[{division_upper}] DEBUG: Fixtures Headers Found: {list(fixtures_df_raw.columns)}")
            print(f"[{division_upper}] DEBUG: Player Stats Headers Found: {list(players_df_raw.columns)}\n")

            ladder_df = ladder_df_raw.copy()
            fixtures_df = fixtures_df_raw.copy()
            players_df = players_df_raw.copy()

            # --- 2. Clean Player Stats ---
            try:
                players_df.rename(columns=PLAYER_COL_MAP, inplace=True)

                if 'team_name' not in players_df.columns:
                    raise KeyError(
                        f"'team_name' key not found after rename. Check PLAYER_COL_MAP. Available columns: {list(players_df.columns)}")

                players_df = players_df[
                    players_df['team_name'].str.contains(team_name_filter, case=False, na=False)].copy()

                for col in ['goals', 'assists', 'appearances', 'yellow_cards', 'red_cards']:
                    if col in players_df.columns:
                        players_df[col] = pd.to_numeric(players_df[col], errors='coerce').fillna(0).astype(int)
                    else:
                        players_df[col] = 0

                players_df['division'] = division_upper
            except KeyError as e:
                print(f"!!! KEY ERROR processing Player Stats for {division_upper}: {e}")
                return None

                # --- 3. Clean Fixtures ---
            try:
                fixtures_df.rename(columns=FIXTURE_COL_MAP, inplace=True)

                if 'home_team' not in fixtures_df.columns or 'away_team' not in fixtures_df.columns:
                    raise KeyError(
                        f"'home_team' or 'away_team' key not found after rename. Check FIXTURE_COL_MAP. Available columns: {list(fixtures_df.columns)}")

                fixtures_df = fixtures_df[
                    fixtures_df['home_team'].str.contains(team_name_filter, case=False, na=False) |
                    fixtures_df['away_team'].str.contains(team_name_filter, case=False, na=False)
                    ].copy()

                if 'date' not in fixtures_df.columns: fixtures_df['date'] = ''
                if 'time' not in fixtures_df.columns: fixtures_df['time'] = ''
                if 'round' not in fixtures_df.columns: fixtures_df['round'] = ''

                fixtures_df['date_time'] = fixtures_df['date'].fillna('') + ' ' + fixtures_df['time'].fillna('')
                fixtures_df['id'] = (
                        division_upper + '-' +
                        fixtures_df['home_team'].str.replace(' ', '', regex=False) + '-' +
                        fixtures_df['away_team'].str.replace(' ', '', regex=False) + '-' +
                        fixtures_df['date']
                )

                is_home_game = fixtures_df['home_team'].str.startswith(team_name_filter)
                fixtures_df['opponent'] = fixtures_df['away_team']
                fixtures_df.loc[~is_home_game, 'opponent'] = fixtures_df['home_team']

                fixtures_df['division'] = division_upper

            except KeyError as e:
                print(f"!!! KEY ERROR processing Fixtures for {division_upper}: {e}")
                return None

            await browser.close()
            print(f"--- Successfully finished {division_upper} ---")

            return {
                'ladder': ladder_df.to_dict('records'),
                'fixtures': fixtures_df.to_dict('records'),
                'players': players_df.to_dict('records')
            }

        except Exception as e:
            print(f"!!! CRITICAL ERROR scraping {division_path}: {e}")
            await browser.close()
            return None


async def main():
    """
    Main function to orchestrate both division scrapes and print the result.
    This will be adapted into our API server.
    """
    print("Starting Futsal Scraper...")

    msl_task = asyncio.create_task(get_division_data('MSL'))
    mslb_task = asyncio.create_task(get_division_data('MSLB'))

    msl_data = await msl_task
    mslb_data = await mslb_task

    if not msl_data:
        print("Scraping failed for MSL. Exiting.")
    if not mslb_data:
        print("Scraping failed for MSLB. Exiting.")
    if not msl_data or not mslb_data:
        return

    all_players = msl_data.get('players', []) + mslb_data.get('players', [])
    all_fixtures = msl_data.get('fixtures', []) + mslb_data.get('fixtures', [])

    all_players_sorted = sorted(all_players, key=lambda p: p.get('goals', 0), reverse=True)

    print("\n\n--- PERTH UNITED - PLAYER STATS (Sorted by Goals) ---")
    if not all_players_sorted:
        print("  No player data found.")
    for player in all_players_sorted:
        print(
            f"  [{player['division']}] {player.get('player_name', 'N/A')}: {player.get('goals', 0)} Goals, {player.get('assists', 0)} Assists, {player.get('appearances', 0)} Apps")

    print("\n\n--- PERTH UNITED - UPCOMING FIXTURES ---")

    upcoming_fixtures = []
    for f in all_fixtures:
        score_str = str(f.get('score', '')).strip().lower()

        import re
        # A played game has a score like "5 - 2"
        is_played = re.search(r'\d+\s*-\s*\d+', score_str)

        if not is_played:
            upcoming_fixtures.append(f)

    if not upcoming_fixtures:
        print("  No upcoming fixtures found.")
    for fixture in upcoming_fixtures:
        print(
            f"  [{fixture['division']}] Round {fixture.get('round', '?')}: vs {fixture.get('opponent', 'N/A')} on {fixture.get('date_time', 'N/A')} at {fixture.get('location', 'N/A')}")


# This allows the script to be run directly
if __name__ == "__main__":
    asyncio.run(main())
