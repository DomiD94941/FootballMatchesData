import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns
from get_database import *
import sqlalchemy

username = user
password = password
dsn = f'{host}:{port}/{sid}'

oracle_connection_string = f"oracle+cx_oracle://{username}:{password}@{dsn}"

engine = sqlalchemy.create_engine(oracle_connection_string)

teams_goals_per_season_query = """
SELECT TEAM_NAME, SEASON_YEAR, SUM(GOALS) AS TOTAL_GOALS
FROM (
    SELECT T.TEAM_NAME, MR.HOME_TEAM_FULL_TIME_GOALS AS GOALS, S.SEASON_YEAR, L.LEAGUE_NAME
    FROM MATCHES M
    INNER JOIN MATCH_RESULTS MR ON M.MATCH_ID = MR.MATCH_ID
    INNER JOIN TEAMS T ON M.HOME_TEAM_ID = T.TEAM_ID
    INNER JOIN SEASONS S ON M.SEASON_ID = S.SEASON_ID
    INNER JOIN LEAGUES L ON M.LEAGUE_ID = L.LEAGUE_ID

    UNION ALL

    SELECT T.TEAM_NAME, MR2.AWAY_TEAM_FULL_TIME_GOALS AS GOALS, S.SEASON_YEAR, L.LEAGUE_NAME
    FROM MATCHES M2
    INNER JOIN MATCH_RESULTS MR2 ON M2.MATCH_ID = MR2.MATCH_ID
    INNER JOIN TEAMS T ON M2.AWAY_TEAM_ID = T.TEAM_ID
    INNER JOIN SEASONS S ON M2.SEASON_ID = S.SEASON_ID
    INNER JOIN LEAGUES L ON M2.LEAGUE_ID = L.LEAGUE_ID

) ALL_GOALS  
WHERE LEAGUE_NAME = 'La Liga' 
GROUP BY TEAM_NAME, SEASON_YEAR  
ORDER BY TOTAL_GOALS DESC
"""

team_goals_in_la_liga_df = pd.read_sql(teams_goals_per_season_query, engine)
plt.figure(figsize=(12, 6))
sns.barplot(data=team_goals_in_la_liga_df, x="total_goals", y="team_name", hue="season_year", palette="viridis")
plt.xlabel("Goals")
plt.ylabel("Team")
plt.title("Number Of Team Goals For Particular Seasons In La Liga")
plt.legend(title="La Liga Team Goals For Particular Seasons")
plt.show()


ball_possession_query = """
SELECT 
    M.MATCH_ID,
    T.TEAM_NAME,
    TS.BALL_POSSESSION,
    MR.HOME_TEAM_FULL_TIME_GOALS + MR.AWAY_TEAM_FULL_TIME_GOALS AS TOTAL_GOALS
FROM MATCHES M
JOIN TEAM_MATCH_STATISTICS TS ON M.MATCH_ID = TS.MATCH_ID
JOIN TEAMS T ON TS.TEAM_ID = T.TEAM_ID
JOIN MATCH_RESULTS MR ON M.MATCH_ID = MR.MATCH_ID
"""

df = pd.read_sql(ball_possession_query, engine)
df["ball_possession"] = df["ball_possession"].str.replace("%", "").astype(float)

plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x="ball_possession", y="total_goals", hue="team_name", palette="husl")
plt.xlabel("Ball Possession (%)")
plt.ylabel("Total Goals in Match")
plt.title("Does Ball Possession Affect Goals Scored?")
plt.show()

team_ranking_by_number_of_cards_query = """
SELECT 
    T.TEAM_NAME,
    SUM(YELLOW_CARDS) AS TOTAL_YELLOW_CARDS,
    SUM(RED_CARDS) AS TOTAL_RED_CARDS
FROM TEAM_MATCH_STATISTICS TS
JOIN TEAMS T ON TS.TEAM_ID = T.TEAM_ID
GROUP BY T.TEAM_NAME
ORDER BY TOTAL_RED_CARDS DESC, TOTAL_YELLOW_CARDS DESC
"""

team_ranking_by_number_of_cards_df = pd.read_sql(team_ranking_by_number_of_cards_query, engine)

team_ranking_by_number_of_cards_df.plot(kind='bar', x='team_name', y=['total_yellow_cards', 'total_red_cards'], stacked=True, color=["yellow", "red"])
plt.xlabel("Team")
plt.ylabel("Number of Cards")
plt.title("Number of Yellow and Red Cards per Team")
plt.show()

most_frequency_used_stadiums_query = """
SELECT 
    S.STADIUM_NAME, 
    COUNT(M.MATCH_ID) AS MATCH_COUNT 
FROM MATCHES M
JOIN STADIUMS S ON M.STADIUM_ID = S.STADIUM_ID
GROUP BY S.STADIUM_NAME
ORDER BY MATCH_COUNT DESC
"""

most_frequency_used_stadiums_df = pd.read_sql(most_frequency_used_stadiums_query, engine)

plt.figure(figsize=(12, 6))
sns.barplot(data=most_frequency_used_stadiums_df, x="match_count", y="stadium_name", hue="stadium_name", palette="Blues_r", legend=False)
plt.xlabel("Number of Matches")
plt.ylabel("Stadium")
plt.title("Most Frequently Used Stadiums")
plt.show()

best_shots_accuracy_teams_query = """
SELECT 
    T.TEAM_NAME,
    SUM(SHOTS_ON_GOAL) AS TOTAL_SHOTS_ON_GOAL,
    SUM(TOTAL_SHOTS) AS TOTAL_SHOTS,
    ROUND(SUM(SHOTS_ON_GOAL) / NULLIF(SUM(TOTAL_SHOTS), 0) * 100, 2) AS SHOT_ACCURACY
FROM TEAM_SHOTS_STATISTICS TS
JOIN TEAMS T ON TS.TEAM_ID = T.TEAM_ID
GROUP BY T.TEAM_NAME
ORDER BY SHOT_ACCURACY DESC
"""

best_shots_accuracy_teams_df = pd.read_sql(best_shots_accuracy_teams_query, engine)
plt.figure(figsize=(12, 6))
sns.barplot(data=best_shots_accuracy_teams_df, x="shot_accuracy", y="team_name", hue="team_name", palette="Greens_r", legend=False)
plt.xlabel("Shot Accuracy (%)")
plt.ylabel("Team")
plt.title("Teams with Best Shot Accuracy")
plt.show()

