from get_api_data import *
from get_data_from_data_base import *
from get_database import get_cursor, connection, close_connection
import time
cursor = get_cursor()

def insert_data_into_data_base(insert_query, data):
    cursor.executemany(insert_query, data)
    connection.commit()

def insert_seasons():

    seasons = get_seasons()

    seasons_to_insert = [(year,) for year in seasons]

    insert_season_query = 'INSERT INTO SEASONS (SEASON_YEAR) VALUES (:season_year)'
    check_season_query = 'SELECT COUNT(*) FROM SEASONS WHERE SEASON_YEAR = :season_year'

    for season in seasons_to_insert:
        cursor.execute(check_season_query, {'season_year': season[0]})
        season_exist = cursor.fetchone()[0]

        if season_exist == 0:
            insert_data_into_data_base(insert_season_query, [season])
            print(f'Season {season[0]} inserted.')
        else:
            print(f'Season {season[0]} already exists, skipping insertion.')

def insert_single_season_leagues(season_year):

    leagues = get_leagues(season_year)

    # prepare to insert league part

    insert_leagues_query = 'INSERT INTO LEAGUES (LEAGUE_ID, LEAGUE_NAME, LEAGUE_COUNTRY) VALUES (:league_id, :league_name, :league_country)'
    check_leagues_query = 'SELECT COUNT(*) FROM LEAGUES WHERE LEAGUE_ID = :league_id'

    # get season_id by input season_year and check if is in the database

    cursor.execute('SELECT SEASON_ID FROM SEASONS WHERE SEASON_YEAR = :season_year', {'season_year': season_year})
    season_id_result = cursor.fetchone()
    if season_id_result is None:
        print(f"Season id for season: {season_year} is not exist.")
        return
    season_id = season_id_result[0]

    # prepare to insert season_leagues

    insert_season_leagues = """
        INSERT INTO SEASON_LEAGUES (SEASON_ID, SEASON_START, SEASON_END, LEAGUE_ID) VALUES
        (:season_id, TO_DATE(:season_start, 'YYYY-MM-DD'), TO_DATE(:season_end, 'YYYY-MM-DD'), :league_id)
    """
    check_season_leagues = 'SELECT COUNT(*) FROM SEASON_LEAGUES WHERE SEASON_ID = :season_id AND LEAGUE_ID = :league_id'

    # insert leagues and season_leagues

    for league_entry in leagues:

        league = league_entry['league']
        country = league_entry['country']

        league_id = league['id']
        league_name = league['name']
        league_country = country['name']

        season = league_entry['seasons'][0]
        season_year = season['year']
        season_start = season['start']
        season_end = season['end']

        cursor.execute(check_leagues_query, {'league_id': league_id})
        league_exist = cursor.fetchone()[0]

        if league_exist == 0:
            data = [{'league_id': league_id, 'league_name': league_name, 'league_country': league_country}]
            insert_data_into_data_base(insert_leagues_query, data)
        else:
            print(f'League {league_name} already exists, skipping insertion.')

        cursor.execute(check_season_leagues, {'season_id': season_id, 'league_id': league_id})
        season_league_exist = cursor.fetchone()[0]

        if season_league_exist == 0:
            data = [{'season_id': season_id, 'season_start': season_start, 'season_end': season_end, 'league_id': league_id}]
            insert_data_into_data_base(insert_season_leagues, data)
        else:
            print(f'Season {season_year} and league {league_name} already exists, skipping insertion.')

def insert_single_league_teams(league_id, season_year):

    league_teams = get_teams(league_id, season_year)

    # get season_id by input season_year and check if is in the database

    cursor.execute('SELECT SEASON_ID FROM SEASONS WHERE SEASON_YEAR = :season_year', {'season_year': season_year})
    season_id_result = cursor.fetchone()
    if season_id_result is None:
        print(f"Season id for season: {season_year} is not exist.")
        return
    season_id = season_id_result[0]

    # prepare to insert team stadiums

    insert_team_stadium_query = """INSERT INTO STADIUMS (STADIUM_ID, STADIUM_NAME, STADIUM_ADDRESS, STADIUM_CITY, CAPACITY, SURFACE) 
    VALUES (:stadium_id, :stadium_name, :address, :city, :capacity, :surface)
    """
    check_stadium_query = 'SELECT COUNT(*) FROM STADIUMS WHERE STADIUM_ID = :stadium_id'

    # prepare to insert league teams

    insert_teams_query = """INSERT INTO TEAMS (TEAM_ID ,TEAM_NAME, SHORT_TEAM_NAME, TEAM_COUNTRY, FOUNDED, IS_NATIONAL, STADIUM_ID)
      VALUES (:team_id, :team_name, :short_team_name, :team_country, :founded, :is_national, :stadium_id)
      """
    check_teams_query = 'SELECT COUNT(*) FROM TEAMS WHERE TEAM_ID = :team_id'

    # prepare to insert season league teams

    insert_teams_league_season_query = """ 
    INSERT INTO TEAMS_SEASON_LEAGUE (TEAM_ID, SEASON_ID, LEAGUE_ID) VALUES (:team_id, :season_id, :league_id)
    """
    check_teams_league_season_query = """
    SELECT COUNT(*) FROM TEAMS_SEASON_LEAGUE WHERE TEAM_ID = :team_id AND SEASON_ID = :season_id AND LEAGUE_ID = :league_id
    """

    # inserts

    for teams_entry in league_teams:
        team = teams_entry['team']
        stadium = teams_entry['venue']

        team_id = team['id']
        team_name = team['name']
        short_team_name = team['code']
        team_country = team['country']
        founded = team['founded']
        is_national = 'Y' if team['national'] == 'True' else 'N'
        stadium_id = stadium['id']
        print(stadium_id)
        season_id = season_id
        league_id = league_id

        stadium_name = stadium['name']
        stadium_address = stadium['address']
        stadium_city = stadium['city']
        stadium_capacity = stadium['capacity']
        stadium_surface = stadium['surface']

        # insert team stadiums to database

        cursor.execute(check_stadium_query, {'stadium_id': stadium_id})
        team_stadium_exist = cursor.fetchone()[0]
        if team_stadium_exist == 0:
            data = [{'stadium_id': stadium_id, 'stadium_name': stadium_name, 'address': stadium_address, 'city': stadium_city, 'capacity': stadium_capacity, 'surface': stadium_surface}]
            insert_data_into_data_base(insert_team_stadium_query, data)
        else:
            print(f'Stadium {stadium_name} already exists, skipping insertion.')

        cursor.execute(check_teams_query, {'team_id': team_id})
        team_exist = cursor.fetchone()[0]
        if team_exist == 0:
            data = [{'team_id': team_id, 'team_name': team_name, 'short_team_name': short_team_name,
                     'team_country': team_country, 'founded' :founded, 'is_national': is_national,
                     'stadium_id': stadium_id}]
            insert_data_into_data_base(insert_teams_query, data)
        else:
            print(f'Team {team_name} already exists, skipping insertion.')

        # insert season league teams

        cursor.execute(check_teams_league_season_query, {'team_id': team_id, 'season_id': season_id, 'league_id': league_id})
        teams_league_season_exist = cursor.fetchone()[0]
        if teams_league_season_exist == 0:
            data = [{'team_id': team_id, 'season_id': season_id, 'league_id': league_id}]
            insert_data_into_data_base(insert_teams_league_season_query, data)
        else:
            print(f'Team {team_name} for league id {league_id} and season {season_year} already exists, skipping insertion.')

# insert_single_league_teams(140, 2022)

def insert_single_team_matches(team_id, league_id, season_year):

    team_matches = get_team_matches(team_id, league_id, season_year)

    # get season_id by input season_year and check if is in the database

    cursor.execute('SELECT SEASON_ID FROM SEASONS WHERE SEASON_YEAR = :season_year', {'season_year': season_year})
    season_id_result = cursor.fetchone()
    if season_id_result is None:
        print(f"Season id for season: {season_year} is not exist.")
        return
    season_id = season_id_result[0]

    # prepare to insert matches

    insert_team_match_query = """
    INSERT INTO MATCHES (MATCH_ID, SEASON_ID, LEAGUE_ID, HOME_TEAM_ID, AWAY_TEAM_ID, REFEREE, MATCH_DATE, STADIUM_ID)
    VALUES (:match_id, :season_id, :league_id, :home_team_id, :away_team_id, :referee, TO_TIMESTAMP_TZ(:match_date,'YYYY-MM-DD"T"HH24:MI:SS TZH:TZM'), :stadium_id)
    """
    check_team_match_query = 'SELECT COUNT(*) FROM MATCHES WHERE MATCH_ID = :match_id'

    # prepare to insert match result

    insert_match_result_query = """
    INSERT INTO MATCH_RESULTS (MATCH_ID, HOME_TEAM_FULL_TIME_GOALS, AWAY_TEAM_FULL_TIME_GOALS, HOME_TEAM_HALF_TIME_GOALS, AWAY_TEAM_HALF_TIME_GOALS)
    VALUES (:match_id, :home_team_full_time_goals, :away_team_full_time_goals, :home_team_half_time_goals, :away_team_half_time_goals)
    """
    check_match_result_query = 'SELECT COUNT(*) FROM MATCH_RESULTS WHERE MATCH_ID = :match_id'

    # inserts

    # additional query because API has leaks in stadiums

    check_stadium_query = 'SELECT COUNT(*) FROM STADIUMS WHERE STADIUM_ID = :stadium_id'

    for matches_entry in team_matches:
        match = matches_entry['fixture']
        match_id = match['id']
        match_referee = match['referee']
        match_date = match['date']
        stadium = match['venue']
        teams = matches_entry['teams']
        stadium_id = stadium.get('id')

        if stadium_id:
            cursor.execute(check_stadium_query, {'stadium_id': stadium_id})
            if cursor.fetchone()[0] == 0:
                continue
        else:
            continue

        home_team = teams['home']
        away_team = teams['away']
        home_team_id = home_team['id']
        away_team_id = away_team['id']

        goals = matches_entry['score']
        home_team_full_time_goals = goals['fulltime'].get('home')
        home_team_half_time_goals = goals['halftime'].get('home')
        away_team_full_time_goals = goals['fulltime'].get('away')
        away_team_half_time_goals = goals['halftime'].get('away')

        # insert match

        cursor.execute(check_team_match_query, {'match_id': match_id})
        match_exist = cursor.fetchone()[0]
        if match_exist == 0:
            data = [{'match_id': match_id, 'season_id': season_id, 'league_id': league_id, 'home_team_id': home_team_id,
                 'away_team_id': away_team_id, 'referee': match_referee, 'match_date': match_date, 'stadium_id': stadium_id}]
            insert_data_into_data_base(insert_team_match_query, data)
        else:
            print(f'Match {match_id} already exists, skipping insertion.')

        # insert match result (only goals)

        cursor.execute(check_match_result_query, {'match_id': match_id})
        match_result_exist = cursor.fetchone()[0]
        if match_result_exist == 0:
            data = [{'match_id': match_id, 'home_team_full_time_goals': home_team_full_time_goals, 'home_team_half_time_goals': home_team_half_time_goals,
                 'away_team_full_time_goals': away_team_full_time_goals, 'away_team_half_time_goals': away_team_half_time_goals}]
            insert_data_into_data_base(insert_match_result_query, data)
        else:
            print(f'Match {match_id} result already exists, skipping insertion.')

def insert_multiple_team_matches():
    teams_league_season = get_teams_season_league_db()
    for team_league_season in teams_league_season:
        team_id = team_league_season['TEAM_ID']
        league_id = team_league_season['LEAGUE_ID']
        season = team_league_season['SEASON_YEAR']
        time.sleep(3)
        insert_single_team_matches(team_id, league_id, season)

# insert_multiple_team_matches()


def get_detailed_match_stats(match_id, team_id):

    match_stats = detailed_match_stats(match_id, team_id)
    time.sleep(10)

    # prepare to insert match stats

    insert_match_stats_query = """INSERT INTO TEAM_MATCH_STATISTICS (MATCH_ID, TEAM_ID, BALL_POSSESSION, TOTAL_PASSES, PASSES_ACCURATE, 
        PASSES_PERCENTAGE, FOULS, CORNER_KICKS, OFFSIDES, YELLOW_CARDS, RED_CARDS, GOALKEEPER_SAVES)
            VALUES (:match_id, :team_id, :ball_possession, :total_passes, :passes_accurate, :passes_percentage, :fouls, :corner_kicks, :offsides,
        :yellow_cards, :red_cards, :goalkeeper_saves)"""
    check_match_stats_query = 'SELECT COUNT(*) FROM TEAM_MATCH_STATISTICS WHERE MATCH_ID = :match_id AND TEAM_ID = :team_id'

    # prepare to insert match shot stats

    insert_match_shot_stats_query = """INSERT INTO TEAM_SHOTS_STATISTICS (MATCH_ID, TEAM_ID, SHOTS_ON_GOAL, SHOTS_OFF_GOAL,
     TOTAL_SHOTS, BLOCKED_SHOTS, SHOTS_INSIDEBOX, SHOTS_OUTSIDEBOX) VALUES (:match_id, :team_id, :shots_on_goal, :shots_off_goal, 
     :total_shots, :blocked_shots, :shots_inside_box, :shots_outside_box)"""

    check_match_shot_stats = 'SELECT COUNT(*) FROM TEAM_SHOTS_STATISTICS WHERE MATCH_ID = :match_id AND TEAM_ID = :team_id'

    match_data = match_stats[0]['team']
    stats = {stat['type']: stat['value'] for stat in match_stats[0]['statistics']}

    # inserts

    match_stats = [{
        'match_id': match_id,
        'team_id': match_data['id'],
        'ball_possession': stats.get('Ball Possession'),
        'total_passes': stats.get('Total passes'),
        'passes_accurate': stats.get('Passes accurate'),
        'passes_percentage': stats.get('Passes %'),
        'fouls': stats.get('Fouls'),
        'corner_kicks': stats.get('Corner Kicks'),
        'offsides': stats.get('Offsides'),
        'yellow_cards': stats.get('Yellow Cards'),
        'red_cards': stats.get('Red Cards'),
        'goalkeeper_saves': stats.get('Goalkeeper Saves')
    }]

    match_shot_stats = [{
        'match_id': match_id,
        'team_id': match_data['id'],
        'shots_on_goal': stats.get('Shots on Goal'),
        'shots_off_goal': stats.get('Shots off Goal'),
        'total_shots': stats.get('Total Shots'),
        'blocked_shots': stats.get('Blocked Shots'),
        'shots_inside_box': stats.get('Shots insidebox'),
        'shots_outside_box': stats.get('Shots outsidebox')
    }]


    # insert match stats

    cursor.execute(check_match_stats_query, {'match_id': match_id, 'team_id': team_id})
    match_stats_exist = cursor.fetchone()[0]
    if match_stats_exist == 0:
        insert_data_into_data_base(insert_match_stats_query, match_stats)
    else:
        print(f'Match stats for match {match_id} and team {team_id} already exists, skipping insertion.')

    # insert match shots

    cursor.execute(check_match_shot_stats, {'match_id': match_id, 'team_id': team_id})
    check_match_shot_stats = cursor.fetchone()[0]
    if check_match_shot_stats == 0:
        insert_data_into_data_base(insert_match_shot_stats_query, match_shot_stats)
    else:
        print(f'Match shots for match {match_id} and team {team_id} already exists, skipping insertion.')


def get_multiple_detailed_match_stats():

    teams = get_teams_db()
    for team in teams:
        team_id = team['TEAM_ID']
        print(team_id)
        matches = get_team_matches_db(team_id)
        print(matches)
        counter = 0
        for match in matches:
            counter += 1
            if team_id in match and counter <= 10:
                team_id_matches = team_id
                match_id = match[0]
                print(match_id)
                get_detailed_match_stats(match_id, team_id_matches)

# next step team id > 42

get_multiple_detailed_match_stats()

close_connection()