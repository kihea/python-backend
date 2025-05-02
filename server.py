import math
from typing import Dict, Any, List
import pandas as pd
import time
import threading
import queue
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import requests
load_dotenv()  # Load environment variables from .env file
import os
from datetime import datetime
from supabase import create_client, Client
from nba_api.stats.static import players, teams
from nba_api.live.nba.endpoints import scoreboard
from nba_api.stats.endpoints import (shotchartdetail,commonplayerinfo, leaguegamelog)
from apscheduler.schedulers.background import BackgroundScheduler
url = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
key = os.getenv("SUPABASE_SERVICE_ROLE")
supabase: Client = create_client(url, key)


class DontePicks:
    job_queue = queue.Queue()
    num_worker_threads = 5  # Adjust based on your system's capabilities
    base_url = f"https://api-{os.getenv("RELEVANCE_REGION")}.stack.tryrelevance.com/latest"
    tool_id = os.getenv("RELEVANCE_TOOL_ID")
    headers={
        "Authorization": os.getenv("RELEVANCE_PROJECT_ID") + ":" + os.getenv("RELEVANCE_API_KEY"),
        "Content-Type": "application/json",
    }
    def __init__(self, payload):
        self.payload = payload
        self.job_id = None
        DontePicks.job_queue.put(self)

    def do(self):
        if self.job_id is not None:
            return self.job_id
        response = requests.post(
            DontePicks.base_url + f"/studios/{DontePicks.tool_id}/trigger_async",
            headers=DontePicks.headers,
            json={
                "params": self.payload,
                "project": os.getenv("RELEVANCE_PROJECT_ID"),
            },
        )
        if response.status_code != 200:
            print(f"Error: {response.status_code} - {response.text}")
            return None
        job = response.json()
        self.job_id = job.get("job_id")
        return self.job_id

    def poll(self):
        if self.job_id is None:
            return None
        while True:
            response = requests.get(
                DontePicks.base_url + f"/studios/{DontePicks.tool_id}/async_poll/{self.job_id}?ending_update_only=true",
                headers=DontePicks.headers
            )
            if response.status_code != 200:
                print(f"Error with player analysis: {response.status_code} - {response.text}")
                time.sleep(5)
                continue
            job = response.json()
            
            if job["type"] == "complete":
                for step in job["updates"]:
                    if step["type"] == "chain-success":
                        job = step['output']['output']['output']
                        picks = job["picks"]
                        picks = [
                            {
                                "prop_id": pick["prop_id"],
                                "donte_projection": pick["estimated_projection"],
                                "donte_confidence": pick["confidence"],
                                "donte_analysis": pick["analysis"],
                                "donte_considerations": pick["considerations"],
                                "donte_decision": pick["decision"],
                            } for pick in picks
                        ]
                        # Remove any duplicates
                        seen_ids = set()
                        unique_picks = []
                        for pick in picks:
                            if pick["prop_id"] not in seen_ids:
                                seen_ids.add(pick["prop_id"])
                                unique_picks.append(pick)

                        picks = unique_picks
                        supabase.table("historical_odds").upsert(picks).execute()
                        break
                return job 
            elif job["type"] == "failed":
                print(f"Job failed: {job['job_id']}")
                return None
            else:
                time.sleep(3)

    @classmethod
    def worker(cls):
        while True:
            donte_pick = cls.job_queue.get()
            try:
                donte_pick.do()
                donte_pick.poll()
            finally:
                cls.job_queue.task_done()

    @classmethod
    def start_workers(cls):
        for _ in range(cls.num_worker_threads):
            t = threading.Thread(target=cls.worker)
            t.daemon = True
            t.start()

min_sleep = 0.1  # Minimum sleep time in seconds
sleep_time = min_sleep
def safe_api_call(api_function, max_retries=2, max_sleep=12, tracking=None, dataFrame=True,**kwargs) -> list | dict:
    global sleep_time, min_sleep
    custom_headers = {
        'Host': 'stats.nba.com',
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nba.com/',
        'Origin': 'https://www.nba.com',
    }

    for i in range(max_retries):
        try:
            response = api_function(**kwargs, headers=custom_headers)
            # Gradually decrease sleep time on success, but don't go below min_sleep
            sleep_time = max(min(sleep_time / 1.2, max_sleep), min_sleep)
            time.sleep(sleep_time)  # Adaptive sleep
            return response.get_data_frames() if dataFrame else response
        except Exception as e:
            sleep_time = min(max_sleep, sleep_time * 3)  # Increase sleep time on failure
            print(f"Error calling API (ID: {tracking if tracking else api_function.__name__}): {e}")
            
            print(f"Retrying in {max_sleep} seconds...")
            time.sleep(sleep_time)
            
    return []  # Return an empty DataFrame if all retries fail

def get_game_logs(season='2024-25', id_filter=None):
    game_logs = safe_api_call(leaguegamelog.LeagueGameLog,
                                season=season,
                                direction='DESC',
                                league_id='00',
                                season_type_all_star='Regular Season',
                                player_or_team_abbreviation='P')
    playoff_game_logs = safe_api_call(leaguegamelog.LeagueGameLog,
                                season=season,
                                direction='DESC',
                                league_id='00',
                                season_type_all_star='Playoffs',
                                player_or_team_abbreviation='P')
    if not game_logs or not playoff_game_logs:
        print("No game logs found")
        return pd.DataFrame()
    all_game_logs = pd.concat([game_logs[0], playoff_game_logs[0]], ignore_index=True)
    columns_to_drop = ['VIDEO_AVAILABLE', 'SEASON_ID', 'FANTASY_PTS']
    all_game_logs = all_game_logs.drop(columns=[col for col in columns_to_drop if (col in all_game_logs.columns)])
    all_game_logs.columns = [col.lower() for col in all_game_logs.columns]
    all_game_logs = all_game_logs.fillna(0)
    if id_filter is not None:
        # Should be a list of game_ids to filter out
        print(f"Filtering out {len(id_filter)} game logs of {len(all_game_logs)}...")
        all_game_logs = all_game_logs[~all_game_logs['game_id'].isin(id_filter)]
        print(f"Remaining game logs: {len(all_game_logs)}")
    return all_game_logs    

def get_game_data(season='2024-25'):
    print(f"Fetching game data for {season} season...")
    # Get live data first
    
    # Get games for today
    gamestoday = scoreboard.ScoreBoard().games.get_dict()
    for game in gamestoday:
        home_team = game["homeTeam"]["teamId"]
        away_team = game["awayTeam"]["teamId"]
        # See if game is in games table
        response = (
            supabase
            .table('games')
            .select('game_id')
            .eq('home_team_id', home_team)
            .eq('away_team_id', away_team)
            .maybe_single()
            .execute()
        )
        if response.data:
            # Add game_id to games table as nba_api_game_id
            game_id = response.data['game_id']
            supabase.table('games').update({'nba_api_game_id': safe_int_cast(game['gameId']),'series_game_number': game['seriesGameNumber'], 'series_text': game['seriesText'] ,'period': game['period'], 'game_clock': game['gameClock'], 'status': game['gameStatusText'], 'home_score': game['homeTeam']['score'], 'away_score': game['awayTeam']['score']}).eq('game_id', game_id).execute()
            pass
        else:
            continue
    batch_size = 4000
    offset = 0
    game_ids = []
    while True:
        response = (
            supabase
            .table('historical_performances')
            .select('game_id')
            .range(offset, offset + batch_size - 1)
            .execute()
        )
        if not response.data:
            break
        game_ids.extend(row['game_id'] for row in response.data)
        offset += batch_size
    twenty_four_hours_ago = (datetime.now().astimezone() - timedelta(days=3)).isoformat()
    needToRerun = (
        supabase
        .table('historical_performances')
        .select('game_id')
        .or_(
            "wl.eq.0,has_shot_data.eq.false,game_date.gte." + twenty_four_hours_ago
        )
        .execute()
    )

    if needToRerun.data: # Remove any game_ids from game_ids that are in needToRerun
        game_ids = [game_id for game_id in game_ids if (game_id not in [row['game_id'] for row in needToRerun.data])]
    logs = get_game_logs(season=season, id_filter=game_ids)

    all_processed_data = []  # Store all rows here
    bad_data_lines = []
    for _, row in logs.iterrows():
        data = row.to_dict()

        game_id = data['game_id']
        player_id = data['player_id']
        team_id = data['team_id']
        expected_shot_zones = [
            'abovethebreak3',
            'inthepaintnonra',
            'restrictedarea',
            'leftcorner3',
            'rightcorner3',
            'midrange',
            'backcourt'
        ]

        expected_action_types = [
            'cuttingdunkshot', 'cuttinglayupshot', 'drivingfloatingjumpshot', 'jumpshot',
            'drivinglayupshot', 'pullupjumpshot', 'runningfingerrolllayupshot',
            'runningjumpshot', 'fadeawayjumpshot', 'floatingjumpshot', 'tiplayupshot',
            'turnaroundhookshot', 'runninglayupshot', 'drivingfloatingbankjumpshot',
            'putbacklayupshot', 'turnaroundbankshot', 'runningpullupjumpshot',
            'stepbackjumpshot', 'turnaroundjumpshot', 'runningdunkshot', 'drivingdunkshot',
            'drivingfingerrolllayupshot', 'jumpbankshot', 'alleyoopdunkshot', 'drivinghookshot',
            'layupshot', 'tipdunkshot', 'dunkshot', 'turnaroundfadeawayshot',
            'runningalleyoopdunkshot', 'runningreverselayupshot', 'drivingbankhookshot',
            'reverselayupshot', 'alleyooplayupshot', 'putbackdunkshot', 'runningreversedunkshot',
            'drivingreverselayupshot', 'cuttingfingerrolllayupshot', 'fingerrolllayupshot',
            'turnaroundbankhookshot', 'runningalleyooplayupshot', 'fadeawaybankshot',
            'hookshot', 'stepbackbankjumpshot', 'reversedunkshot',
            'turnaroundfadeawaybankjumpshot', 'hookbankshot', 'drivingreversedunkshot'
        ]

        expected_shot_ranges = ['lt8ft', '8_16ft', '16_24ft', 'gt24ft']
        shot_data = safe_api_call(shotchartdetail.ShotChartDetail,
                                  team_id=team_id,
                                  player_id=player_id,
                                  game_id_nullable=game_id,
                                  context_measure_simple='FGA')
        for zone in expected_shot_zones:
            data[f'{zone}fga'] = 0
            data[f'{zone}fgm'] = 0

        for action in expected_action_types:
            data[f'{action}_attempted'] = 0
            data[f'{action}_made'] = 0

        for dist in expected_shot_ranges:
            data[f'fga_{dist}'] = 0
            data[f'fgm_{dist}'] = 0
        if shot_data and len(shot_data) > 0:
            data['has_shot_data'] = True
            shots_df = shot_data[0]

            try:
                # Shot Zone Aggregations
                shot_zone_agg = shots_df.groupby('SHOT_ZONE_BASIC')['SHOT_MADE_FLAG'].agg(['count', 'sum'])
                shot_distance_agg = shots_df.groupby('SHOT_ZONE_RANGE')['SHOT_MADE_FLAG'].agg(['count', 'sum'])
                action_type_agg = shots_df.groupby('ACTION_TYPE')['SHOT_MADE_FLAG'].agg(['count', 'sum'])

                for zone_name, stats in shot_zone_agg.iterrows():
                    zone_key = zone_name.replace(' ', '').replace('(', '').replace(')', '').replace('-', '').lower()
                    data[f'{zone_key}fga'] = int(stats['count'])
                    data[f'{zone_key}fgm'] = int(stats['sum'])

                for action_name, stats in action_type_agg.iterrows():
                    action_key = action_name.replace(' ', '').replace('(', '').replace(')', '').replace('-', '').lower()
                    data[f'{action_key}_attempted'] = int(stats['count'])
                    data[f'{action_key}_made'] = int(stats['sum'])

                range_mapping = {
                    "Less Than 8 ft.": "lt8ft",
                    "8-16 ft.": "8_16ft",
                    "16-24 ft.": "16_24ft",
                    "24+ ft.": "gt24ft"
                }

                for range_val, stats in shot_distance_agg.iterrows():
                    key = range_mapping.get(range_val)
                    if key:
                        data[f'fga_{key}'] = int(stats['count'])
                        data[f'fgm_{key}'] = int(stats['sum'])
            except Exception as e:
                continue
        else:
            data['has_shot_data'] = False
        try:
            supabase.table('historical_performances').upsert(data).execute()
        except Exception as e:
            bad_data_lines.append(data)
            continue
    
    # Go back to any rows in the database where wl == "0" or has_shot_data == False and retry data retrieval


    return all_processed_data, bad_data_lines
def clean_row(row):
    """Replace any NaN or None in the row with 0"""
    return {k: (0 if (v is None or (isinstance(v, float) and math.isnan(v))) else v) for k, v in row.items()}
def upload_logs_to_supabase_bulk(data_list, table_name="historical_performances", batch_size=500):
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i+batch_size]

        # Clean each row before upload
        clean_batch = [clean_row(row) for row in batch]

        supabase.table(table_name).upsert(clean_batch).execute()

bad_lines = []
def now() -> str:
    """
    Returns the current time in ISO format.
    """
    return datetime.now().astimezone().isoformat()
def transform_to_odds(data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Transforms the raw odds payload into the shape:
    {
      "odds": [{
          "player_name": str,
        "props": [ { ... }, … ],
          "game_id": game_id,
          "last_update": <timestamp>
            }, …]}
    """
    player_map: Dict[str, List[Dict[str, Any]]] = {}
    game_id = data.get("id")
    for bookmaker in data.get("bookmakers", []):
        bm_key = bookmaker["key"]
        for market in bookmaker.get("markets", []):
            prop_key    = market["key"]
            last_update = market.get("last_update")
            for outcome in market.get("outcomes", []):
                name = outcome["description"]
                # init nested dicts
                player_map.setdefault(name, {})
                entry = player_map[name].setdefault(prop_key + bm_key, {
                    "point":       outcome["point"] if "point" in outcome else 0.5, # For props such as triple-doubles
                    "prop":        prop_key,
                    "bookmaker":   bm_key,
                    "last_update": last_update,
                    "game_id":     game_id,
                    "overDecision": None,
                    "underDecision": None,
                })

                # assign to over or under
                if outcome["name"].lower() == "over" or outcome["name"].lower() == "yes":
                    entry["overDecision"] = outcome["price"]
                elif outcome["name"].lower() == "under" or outcome["name"].lower() == "no":
                    entry["underDecision"] = outcome["price"]
                else:
                    # Handle unexpected outcome names
                    print(f"Unexpected outcome name: {outcome['name']}")
                    bad_lines.append(outcome)
                    continue


    # build final structure
    odds = []
    for player_name, props in player_map.items():
        # Flatten props into a list
        props = list(props.values())
        player_id = players.find_players_by_full_name(player_name.replace('.', ' '))
        if not player_id:
            bad_lines.append(player_name)
            continue
        player_id = player_id[0]["id"]
        odds.append({
            "player_name": player_name,
            "player_id": player_id,
            "props":       props,
            "game_id":     game_id,
            "last_update": now(),
        })

    return {"odds": odds}
def getHeadshot(player_id):
    base = "https://ak-static.cms.nba.com/wp-content/uploads/headshots/nba/latest/260x190/"
    if player_id is None:
        return None
    if player_id == 0:
        return None
    try:
        requests.get(f"{base}{player_id}.png").raise_for_status()
        return f"{base}{player_id}.png"
    except:
        # If the request fails, return a default image URL or None
        return None
bookmakerDomainMap = {
    "underdog": "https://img.logo.dev/underdogfantasy.com",
    "prizepicks": "https://img.logo.dev/prizepicks.com",
    "betmgm": "https://img.logo.dev/betmgminc.com",
    "fanduel": "https://img.logo.dev/fanduel.com",
    "draftkings": "https://img.logo.dev/draftkings.com",
    "nba": "https://img.logo.dev/nba.com",
}
nbateams_abbreviations = {
    "Atlanta Hawks": "ATL",
    "Boston Celtics": "BOS",
    "Brooklyn Nets": "BKN",
    "Charlotte Hornets": "CHA",
    "Chicago Bulls": "CHI",
    "Cleveland Cavaliers": "CLE",
    "Dallas Mavericks": "DAL",
    "Denver Nuggets": "DEN",
    "Detroit Pistons": "DET",
    "Golden State Warriors": "GSW",
    "Houston Rockets": "HOU",
    "Indiana Pacers": "IND",
    "Los Angeles Clippers": "LAC",
    "Los Angeles Lakers": "LAL",
    "Memphis Grizzlies": "MEM",
    "Miami Heat": "MIA",
    "Milwaukee Bucks": "MIL",
    "Minnesota Timberwolves": "MIN",
    "New Orleans Pelicans": "NOP",
    "New York Knicks": "NYK",
    "Oklahoma City Thunder": "OKC",
    "Orlando Magic": "ORL",
    "Philadelphia 76ers": "PHI",
    "Phoenix Suns": "PHX",
    "Portland Trail Blazers": "POR",
    "Sacramento Kings": "SAC",
    "San Antonio Spurs": "SAS",
    "Toronto Raptors": "TOR",
    "Utah Jazz": "UTA",
    "Washington Wizards": "WAS",
}

def getBookmakerLogo(bookmaker: str) -> str:
    """
    Returns the logo URL for a given bookmaker.
    """
    if bookmaker in bookmakerDomainMap:
        return f"{bookmakerDomainMap[bookmaker]}?token={os.getenv('LOGO_TOKEN')}"
    else:
        return f"{bookmakerDomainMap[bookmaker]}?token={os.getenv("LOGO_TOKEN")}"  # Default logo URL

def upload_headshot_from_url(headshot_url, bucket_name, file_name):
    # 1. Download the image
    resp = requests.get(headshot_url)
    resp.raise_for_status()

    # 2. Upload to Supabase
    image_data = resp.content
    res = supabase.storage \
        .from_(bucket_name) \
        .upload(
            file_name,
            image_data,
            {"cache-control": "3600", "upsert": "true", "content-type": "image/png"}
        )
    if res.full_path:
        return True
    else:
        return False
def safe_int_cast(value, default=0):
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def safe_str_int_cast(value):
    try:
        return str(int(value)).zfill(2)
    except (ValueError, TypeError):
        return None
def sync_player_data_to_supabase():
    all_players = players.get_active_players()
    
    # Calculate timestamp for 24 hours ago
    twenty_four_hours_ago = (datetime.now().astimezone() - timedelta(days=7)).isoformat()
    # Get players updated in the last 24 hours
    existing_players = supabase.table('players').select('*').gte('updated_at', twenty_four_hours_ago).execute()
    existing_player_ids = [player['player_id'] for player in existing_players.data]
    all_players = [player for player in all_players if (player and player['id'] not in existing_player_ids)]
    for player in all_players:
        player_id = player['id']
        if not player_id:
            continue
        player_name = player['full_name']
        player_info = safe_api_call(commonplayerinfo.CommonPlayerInfo, player_id=player_id, league_id_nullable='00')
        if not player_info or len(player_info) == 0:
            continue
        player_info = player_info[0]
        team_id = player_info.at[0, "TEAM_ID"].item()
        if team_id is None or team_id == 0:
            continue
        
        teamName =  player_info.at[0, "TEAM_CITY"] + " " + player_info.at[0, "TEAM_NAME"]
        # Get the headshot URL
        headshot_url = getHeadshot(player_id)
        if headshot_url is None:
            headshot_url = "https://img.logo.dev/nba.com?token=" + os.getenv("LOGO_TOKEN")  # Default image URL
            continue
        else:
            # Upload the image to Supabase storage bucket
            bucket_name = "headshots"
            file_name = f"{player_id}.png"
            if upload_headshot_from_url(headshot_url, bucket_name, file_name):
            # Get the public URL of the uploaded image
                headshot_url = supabase.storage.from_(bucket_name).get_public_url("headshots/" + file_name)
        draft_year = player_info.at[0, "DRAFT_YEAR"]
        result_dy = safe_int_cast(draft_year)

        draft_round = player_info.at[0, "DRAFT_ROUND"]
        result_dr = safe_int_cast(draft_round)

        draft_number = player_info.at[0, "DRAFT_NUMBER"]
        result_dn = safe_int_cast(draft_number)

        # Jersey number as 2-digit string (e.g., "07")
        jersey_number = player_info.at[0, "JERSEY"]
        result_jn = safe_str_int_cast(jersey_number)

        supabase.table('players').upsert({
            'player_id': player_id,
            'player_name': player_name,
            'team_id': team_id,
            'team_name': teamName,
            'team_abbreviation': player_info.at[0, "TEAM_ABBREVIATION"],
            'updated_at': now(),
            'headshot_url': headshot_url,

            'birthdate': player_info.at[0, "BIRTHDATE"],
            'height': player_info.at[0, "HEIGHT"],
            'weight': safe_int_cast(player_info.at[0, "WEIGHT"]),
            'position': player_info.at[0, "POSITION"],
            'jersey_number': result_jn,
            'country': player_info.at[0, "COUNTRY"],
            'college': player_info.at[0, "SCHOOL"],
            # If undrafted, set to 0
            'draft_year': result_dy,
            'draft_round': result_dr,
            'draft_number': result_dn,
        }).execute()
def fetch_upcoming_event_count():
    # Get the list of upcoming events
    endpointEvents = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
    
    events = requests.get(endpointEvents, params={"apiKey": os.getenv("THE_ODDS_API_KEY")}).json()
    # Cut out extra data turn each entry into {'game_id': game_id, 'game_date': commence_time, 'home_team': home_team, 'away_team': away_team}
    events = [{'game_id': event['id']} for event in events]
    return len(events)
def sync_active_odds_to_supabase():
    # Get the list of active games
    
    endpointEvents = "https://api.the-odds-api.com/v4/sports/basketball_nba/events"
    response = requests.get(endpointEvents, params={"apiKey": os.getenv("THE_ODDS_API_KEY")})

    if response.status_code == 200:
        games_with_odds_available = response.json()

        games_with_odds_available = [
            {
                'game_id': game['id'],
                'game_date': game['commence_time'],
                'home_team': game['home_team'],
                'away_team': game['away_team']
            }
            for game in games_with_odds_available
        ]
    else:
        print(f"Error fetching odds: {response.status_code}")
        games_with_odds_available = []

    # 2. Fetch games updated in the last hour
    one_hour_ago = (datetime.now().astimezone() - timedelta(hours=0)).isoformat()

    existing_odds = supabase.table('games').select('game_id').gte('updated_at', one_hour_ago).execute()

    existing_game_ids = [game['game_id'] for game in existing_odds.data] if existing_odds.data else []

    # 3. Filter out already updated games
    games_with_odds_available = [
        game for game in games_with_odds_available
        if game['game_id'] not in existing_game_ids
    ]
    bookmakers = [
      "draftkings",
      "fanduel",
      "prizepicks",
      "underdog",
      "betmgm",
    ];
    markets = [
      "player_points",
      "player_points_q1",
      "player_rebounds",
      "player_rebounds_q1",
      "player_assists",
      "player_assists_q1",
      "player_threes",
      "player_blocks",
      "player_steals",
      "player_blocks_steals",
      "player_turnovers",
      "player_points_rebounds_assists",
      "player_points_rebounds",
      "player_points_assists",
      "player_rebounds_assists",
      "player_field_goals",
      "player_frees_made",
      "player_frees_attempts",
      "player_double_double",
      "player_triple_double",
    ];
    regions = ["us", "us_dfs"];
    for game in games_with_odds_available:
        # Call the API to get the odds data 
        
        endpoint = f"{endpointEvents}/{game['game_id']}/odds"
        # 2) Prepare your query-string params exactly as the API defines them
        params = {
            "apiKey":      os.getenv("THE_ODDS_API_KEY"),
            "regions":     ",".join(regions),
            "bookmakers":  ",".join(bookmakers),
            "markets":     ",".join(markets),
            "oddsFormat":  "american",
            "dateFormat":  "iso",
        }
    # 3) Fire the request
        response = requests.get(endpoint, params=params)
        response.raise_for_status()
        data = transform_to_odds(response.json())
        supabase.table('games').upsert({
            'game_id': game['game_id'],
            'game_date': game['game_date'],
            'home_team': game['home_team'],
            'home_team_id': teams.find_teams_by_full_name(game['home_team'])[0]["id"],
            'away_team_id': teams.find_teams_by_full_name(game['away_team'])[0]["id"],
            'away_team': game['away_team'],
            'updated_at': now()
        }).execute()
        for oddsInstance in data["odds"]:
            plr = supabase.table('players').select('player_name,team_name,team_abbreviation').eq('player_id', oddsInstance['player_id']).single().execute()
            
            props = {
                prop['prop'] + str(oddsInstance['player_id']) + oddsInstance['game_id'] + prop['bookmaker'] :{
                    'prop_id': prop['prop'] + str(oddsInstance['player_id']) + oddsInstance['game_id'] + prop['bookmaker'],
                    'player_id': oddsInstance['player_id'],
                    'game_id': oddsInstance['game_id'],
                    'point': prop['point'],
                    'prop': prop['prop'],
                    'bookmaker': prop['bookmaker'],
                    'last_update': prop['last_update'],
                    'overDecision': prop['overDecision'],
                    'underDecision': prop['underDecision'],
                }
                for prop in oddsInstance['props']}
            game_date = datetime.fromisoformat(game['game_date'].replace('Z', '+00:00'))

            if plr.data and game_date.date() == datetime.now(timezone.utc).date():
                l10 = supabase.table('historical_performances').select('*').eq('player_id', oddsInstance['player_id']).order('game_date', desc=True).limit(10).execute()
                l10 = l10.data if l10.data else []
                opponent_abbr = nbateams_abbreviations[game['home_team']] if game['away_team'] == plr.data['team_name'] else nbateams_abbreviations[game['away_team']]
                matchup_vs = f"{plr.data['team_abbreviation']} vs. {opponent_abbr}"
                matchup_at = f"{plr.data['team_abbreviation']} @ {opponent_abbr}"
                l5matchups = (
                    supabase.table('historical_performances')
                    .select('*')
                    .eq('player_id', oddsInstance['player_id'])
                    .gt('min', 0)
                    .in_('matchup', [matchup_vs, matchup_at])  # Filter exact matchups
                    .order('game_date', desc=True)
                    .limit(5)
                    .execute()
                )
                gameinfo = (
                    supabase.table('games')
                    .select('*')
                    .eq('game_id', oddsInstance['game_id'])
                    .single()
                    .execute()
                )
                if gameinfo.data:
                    gameinfo = {
                        "game_id": gameinfo.data.get('game_id'),
                        "home_team": gameinfo.data.get('home_team'),
                        "away_team": gameinfo.data.get('away_team'),
                        "home_team_id": gameinfo.data.get('home_team_id'),
                        "away_team_id": gameinfo.data.get('away_team_id'),
                        "game_date": gameinfo.data.get('game_date'),
                        "status": gameinfo.data.get('status', "Scheduled"),
                        "series_game_number": gameinfo.data.get('series_game_number', "Unknown or Regular Season"),
                        "series_text": gameinfo.data.get('series_text', "Unknown"),
                    }
                else:
                    gameinfo = {}
                payload = {
                    "player_name": oddsInstance['player_name'],
                    "team_name": plr.data['team_name'],
                    "performances": {
                        "last_10": l10,
                        "last_5_matchups": l5matchups.data if l5matchups.data else [],
                    },
                    "props": props,
                    "game_info": gameinfo
                }
                # Send the payload to the endpoint
                DontePicks(payload)
            upload_logs_to_supabase_bulk(list(props.values()), table_name="historical_odds")
def sync_team_data_to_supabase():
    all_teams = teams.get_teams()
    
    # Calculate timestamp for 24 hours ago
    twenty_four_hours_ago = (datetime.now().astimezone() - timedelta(days=7)).isoformat()
    # Get teams updated in the last 24 hours
    existing_teams = supabase.table('teams').select('team_id').gte('updated_at', twenty_four_hours_ago).execute()
    existing_team_ids = [team['team_id'] for team in existing_teams.data]
    all_teams = [team for team in all_teams if team and (team['id'] not in existing_team_ids)]
    
    for team in all_teams:
        team_id = team['id']
        team_name = team['full_name']
        team_abbreviation = team['abbreviation']
        if not team_id or not team_name or not team_abbreviation:
            continue
        supabase.table('teams').upsert({
            'team_id': team_id,
            'team_name': team_name,
            'abbreviation': team_abbreviation,
            'updated_at': now()
        }, on_conflict="team_id").execute()
def adaptive_interval(events: int,
                      credit_limit: int = 20_000,
                      cost_per_event: int = 20,
                      days_in_month: int = 30,
                      min_interval: int = 60,
                      max_interval: int = 14400) -> float:
    """
    Calculate sleep interval (in seconds) so that monthly credit consumption stays within limits.
    Spread runs evenly over a 30-day window: interval = (seconds_in_month * events * cost_per_event) / credit_limit
    Clamped between min_interval and max_interval.
    """
    seconds_in_month = days_in_month * 24 * 3600
    interval = seconds_in_month * events * cost_per_event / credit_limit
    return max(min_interval, min(interval, max_interval))
def adaptive_odds_loop():
    """
    Continuously sync odds with a dynamic interval based on upcoming event count and monthly budget.
    """
    while True:
        try:
            # 1. Determine number of upcoming events (no credit cost)
            events = fetch_upcoming_event_count()  # implement this to count without using paid calls

            # 2. Compute next interval
            interval = adaptive_interval(events)
            print(f"[Odds Loop] {events} events, next sync in {interval:.0f}s based on monthly limit")

            # 3. Sync odds (costly call)
            sync_active_odds_to_supabase()
        except Exception as e:
            print(f"Error in odds loop: {e}")
        # 4. Sleep until next run
        time.sleep(interval)


def main():
    
    start_time = datetime.now()
    print(f"[Main] Data collection start: {start_time}")

    import concurrent.futures
    DontePicks.start_workers()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        print("[Main] Running parallel tasks...")
        futures = []
        futures.append(executor.submit(sync_player_data_to_supabase))
        futures.append(executor.submit(sync_team_data_to_supabase))
        futures.append(executor.submit(get_game_data))
        # We no longer call sync_active_odds here

        # Wait for player & team tasks to finish
        for f in futures:
            f.result()
    DontePicks.job_queue.join()
    end_time = datetime.now()
    print(f"[Main] Data collection completed in {end_time - start_time}")


if __name__ == '__main__':
    
    # -- 1) Start APScheduler for `main` --
    scheduler = BackgroundScheduler()
    # Run `main` every 6 hours (adjust as needed)
    scheduler.add_job(main, 'interval', hours=6, next_run_time=datetime.now())
    scheduler.start()
    print("Scheduler started: main() will run every 6 hours.")

    # -- 2) Start adaptive odds loop in background thread --
    odds_thread = threading.Thread(target=adaptive_odds_loop, daemon=True)
    odds_thread.start()
    print("Adaptive odds loop started in background thread.")

    # -- 3) Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        print("Shutting down scheduler.")
