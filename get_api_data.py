import os
from dotenv import load_dotenv
import requests

load_dotenv()

API_KEY = os.getenv('API_KEY')
BASE_URL = 'https://v3.football.api-sports.io'

headers = {
    'x-rapidapi-host': 'v3.football.api-sports.io',
    'x-rapidapi-key': API_KEY
}

def get_seasons():

    response = requests.get(BASE_URL + '/leagues/seasons', headers=headers)

    if response.status_code == 200:
        seasons = response.json()['response']
        return seasons
    else:
        return f"Error: {response.status_code}, {response.text}"


def get_leagues(season_year):

    response = requests.get(BASE_URL + f'/leagues?season={season_year}', headers=headers)

    if response.status_code == 200:
        leagues = response.json()['response']
        return leagues
    else:
        return f"Error: {response.status_code}, {response.text}"

def get_teams(league_id, season_year):

    response = requests.get(BASE_URL + f'/teams?league={league_id}&season={season_year}', headers=headers)

    if response.status_code == 200:
        leagues = response.json()['response']
        return leagues
    else:
        return f"Error: {response.status_code}, {response.text}"

def get_team_matches(team_id, league_id=None, season=None):

    response = requests.get(BASE_URL + f'/fixtures?team={team_id}&league={league_id}&season={season}', headers=headers)

    if response.status_code == 200:
        leagues = response.json()['response']
        return leagues
    else:
        return f"Error: {response.status_code}, {response.text}"


def detailed_match_stats(match_id, team_id):

    response = requests.get(BASE_URL + f'/fixtures/statistics?fixture={match_id}&team={team_id}', headers=headers)

    if response.status_code == 200:
        leagues = response.json()['response']
        return leagues
    else:
        return f"Error: {response.status_code}, {response.text}"
