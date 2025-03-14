from get_database import get_cursor

cursor = get_cursor()

def get_db(query, data = None):
    cursor.execute(query, data)
    columns = [col[0] for col in cursor.description]
    rows = cursor.fetchall()
    result = [dict(zip(columns, row)) for row in rows]
    return result


def get_team_matches_db(team_id):
    cursor.execute('SELECT MATCH_ID, HOME_TEAM_ID, AWAY_TEAM_ID FROM MATCHES WHERE AWAY_TEAM_ID = :team_id OR HOME_TEAM_ID = :team_id', (team_id,))
    matches = cursor.fetchall()
    return matches

def get_seasons_db():
    cursor.execute('SELECT SEASON_YEAR FROM SEASONS')
    seasons = cursor.fetchall()
    result = [season[0] for season in seasons]
    return result

def get_season_leagues_db(season_year):
    season_leagues_query = """
    SELECT LEAGUE_NAME FROM SEASON_LEAGUES
        INNER JOIN LEAGUES L on SEASON_LEAGUES.LEAGUE_ID = L.LEAGUE_ID
        INNER JOIN SEASONS S on SEASON_LEAGUES.SEASON_ID = S.SEASON_ID
    WHERE SEASON_YEAR = :season_year
    """
    leagues = cursor.execute(season_leagues_query, {'season_year': int(season_year)})
    data = [league[0] for league in leagues]
    return data


def get_season_league_teams(league_name, season_year):

    season_league_teams_query = """
    SELECT TEAM_NAME FROM TEAMS_SEASON_LEAGUE
        INNER JOIN SEASON_LEAGUES SL ON TEAMS_SEASON_LEAGUE.SEASON_ID = SL.SEASON_ID AND TEAMS_SEASON_LEAGUE.LEAGUE_ID = SL.LEAGUE_ID
        INNER JOIN TEAMS ON TEAMS_SEASON_LEAGUE.TEAM_ID = TEAMS.TEAM_ID
        INNER JOIN SEASONS S on SL.SEASON_ID = S.SEASON_ID
        INNER JOIN LEAGUES L on SL.LEAGUE_ID = L.LEAGUE_ID
    WHERE LEAGUE_NAME = :league_name AND SEASON_YEAR = :season_year
    """

    teams = cursor.execute(season_league_teams_query, {'league_name': league_name, 'season_year': int(season_year)})
    data = [team[0] for team in teams]
    return data

def get_home_team_match_stats_db(team_id):
    home_team_match_stats_query = """
    SELECT MATCHES.MATCH_ID, SEASONS.SEASON_YEAR, HOME_TEAM_ID, AWAY_TEAM_ID, MATCH_DATE, BALL_POSSESSION, TOTAL_PASSES, PASSES_ACCURATE,
        PASSES_PERCENTAGE, FOULS, CORNER_KICKS, OFFSIDES, YELLOW_CARDS, RED_CARDS,
        SHOTS_ON_GOAL, SHOTS_OFF_GOAL, TOTAL_SHOTS, BLOCKED_SHOTS, SHOTS_INSIDEBOX, SHOTS_OUTSIDEBOX
        FROM MATCHES INNER JOIN TEAM_MATCH_STATISTICS TMS on MATCHES.MATCH_ID = TMS.MATCH_ID
        INNER JOIN SEASON_LEAGUES ON MATCHES.SEASON_ID = SEASON_LEAGUES.SEASON_ID and MATCHES.LEAGUE_ID = SEASON_LEAGUES.LEAGUE_ID
        INNER JOIN SEASONS ON SEASON_LEAGUES.SEASON_ID = SEASONS.SEASON_ID
        INNER JOIN TEAM_SHOTS_STATISTICS TSS on MATCHES.MATCH_ID = TSS.MATCH_ID
    WHERE HOME_TEAM_ID = :team_id
    """

    return get_db(home_team_match_stats_query, {'team_id': team_id})


def get_away_team_match_stats_db(team_id):
    away_team_match_stats_query = """
      SELECT MATCHES.MATCH_ID, SEASONS.SEASON_YEAR, HOME_TEAM_ID, AWAY_TEAM_ID, MATCH_DATE, BALL_POSSESSION, TOTAL_PASSES, PASSES_ACCURATE,
          PASSES_PERCENTAGE, FOULS, CORNER_KICKS, OFFSIDES, YELLOW_CARDS, RED_CARDS,
          SHOTS_ON_GOAL, SHOTS_OFF_GOAL, TOTAL_SHOTS, BLOCKED_SHOTS, SHOTS_INSIDEBOX, SHOTS_OUTSIDEBOX
          FROM MATCHES INNER JOIN TEAM_MATCH_STATISTICS TMS on MATCHES.MATCH_ID = TMS.MATCH_ID
          INNER JOIN SEASON_LEAGUES ON MATCHES.SEASON_ID = SEASON_LEAGUES.SEASON_ID and MATCHES.LEAGUE_ID = SEASON_LEAGUES.LEAGUE_ID
          INNER JOIN SEASONS ON SEASON_LEAGUES.SEASON_ID = SEASONS.SEASON_ID
          INNER JOIN TEAM_SHOTS_STATISTICS TSS on MATCHES.MATCH_ID = TSS.MATCH_ID
      WHERE HOME_TEAM_ID != :team_id;
      """

    get_db(away_team_match_stats_query, {'team_id': team_id})

def get_teams_db():

    teams_id_query = """
    SELECT TEAM_ID FROM TEAMS ORDER BY TEAM_ID
    """
    return get_db(teams_id_query, {})

def get_available_seasons():
    return [season for season in get_seasons_db() if season in (2021, 2022, 2023)]


def get_teams_season_league_db():
    teams_season_league_query = """
    SELECT * FROM TEAMS_SEASON_LEAGUE INNER JOIN SEASONS ON TEAMS_SEASON_LEAGUE.SEASON_ID = SEASONS.SEASON_ID
    """
    return get_db(teams_season_league_query, {})

