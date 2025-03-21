CREATE TABLE SEASONS (
    SEASON_ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    SEASON_YEAR INTEGER NOT NULL
);

CREATE TABLE LEAGUES (
    LEAGUE_ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    LEAGUE_NAME VARCHAR2(100) NOT NULL,
    LEAGUE_COUNTRY VARCHAR2(255)
);

CREATE TABLE SEASON_LEAGUES (
    SEASON_ID INTEGER,
    SEASON_START DATE,
    SEASON_END DATE,
    LEAGUE_ID INTEGER,
    CONSTRAINT season_leagues_season_id_fk FOREIGN KEY (SEASON_ID) REFERENCES SEASONS(SEASON_ID),
    CONSTRAINT season_leagues_league_id_fk FOREIGN KEY (LEAGUE_ID) REFERENCES LEAGUES(LEAGUE_ID),
    PRIMARY KEY (SEASON_ID, LEAGUE_ID)
);

CREATE TABLE STADIUMS (
    STADIUM_ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    STADIUM_NAME VARCHAR2(255),
    STADIUM_ADDRESS VARCHAR2(255),
    STADIUM_CITY VARCHAR2(255),
    CAPACITY INTEGER,
    SURFACE VARCHAR2(100)
);

CREATE TABLE TEAMS (
    TEAM_ID INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    TEAM_NAME VARCHAR2(100) NOT NULL,
    SHORT_TEAM_NAME VARCHAR2(5),
    TEAM_COUNTRY VARCHAR2(255),
    FOUNDED INTEGER,
    IS_NATIONAL CHAR(1) CHECK (IS_NATIONAL IN ('Y', 'N')),
    STADIUM_ID INTEGER,
    CONSTRAINT teams_stadium_id_fk FOREIGN KEY (STADIUM_ID) REFERENCES STADIUMS(STADIUM_ID)
);

CREATE TABLE TEAMS_SEASON_LEAGUE (
    TEAM_ID INTEGER,
    SEASON_ID INTEGER,
    LEAGUE_ID INTEGER,
    CONSTRAINT teams_season_league_season_id_league_id_fk FOREIGN KEY (SEASON_ID, LEAGUE_ID) REFERENCES SEASON_LEAGUES(SEASON_ID, LEAGUE_ID),
    CONSTRAINT teams_season_league_team_id_fk FOREIGN KEY (TEAM_ID) REFERENCES TEAMS(TEAM_ID),
    PRIMARY KEY (TEAM_ID, LEAGUE_ID, SEASON_ID)
);

CREATE TABLE MATCHES (
    MATCH_ID INTEGER PRIMARY KEY,
    SEASON_ID INTEGER NOT NULL,
    LEAGUE_ID INTEGER NOT NULL,
    HOME_TEAM_ID INTEGER NOT NULL,
    AWAY_TEAM_ID INTEGER NOT NULL,
    REFEREE VARCHAR2(100),
    MATCH_DATE DATE NOT NULL,
    STADIUM_ID INTEGER,
    CONSTRAINT matches_season_league_id_fk FOREIGN KEY (SEASON_ID, LEAGUE_ID) REFERENCES SEASON_LEAGUES(SEASON_ID, LEAGUE_ID),
    CONSTRAINT matches_home_team_id_fk FOREIGN KEY (HOME_TEAM_ID) REFERENCES TEAMS(TEAM_ID),
    CONSTRAINT matches_away_team_id_fk FOREIGN KEY (AWAY_TEAM_ID) REFERENCES TEAMS(TEAM_ID),
    CONSTRAINT matches_stadium_id_fk FOREIGN KEY (STADIUM_ID) REFERENCES STADIUMS(STADIUM_ID),
    CONSTRAINT matches_unique_match UNIQUE (SEASON_ID, LEAGUE_ID, HOME_TEAM_ID, AWAY_TEAM_ID, MATCH_DATE)
);

CREATE TABLE MATCH_RESULTS (
    MATCH_ID INTEGER PRIMARY KEY,
    HOME_TEAM_FULL_TIME_GOALS INTEGER NOT NULL,
    AWAY_TEAM_FULL_TIME_GOALS INTEGER NOT NULL,
    HOME_TEAM_HALF_TIME_GOALS INTEGER NOT NULL,
    AWAY_TEAM_HALF_TIME_GOALS INTEGER NOT NULL,
    CONSTRAINT match_results_match_fk FOREIGN KEY (MATCH_ID) REFERENCES MATCHES(MATCH_ID)
);

CREATE TABLE TEAM_MATCH_STATISTICS (
    MATCH_ID INTEGER NOT NULL,
    TEAM_ID INTEGER NOT NULL,
    BALL_POSSESSION VARCHAR2(10),
    TOTAL_PASSES INTEGER,
    PASSES_ACCURATE INTEGER,
    PASSES_PERCENTAGE VARCHAR2(10),
    FOULS INTEGER,
    CORNER_KICKS INTEGER,
    OFFSIDES INTEGER,
    YELLOW_CARDS INTEGER,
    RED_CARDS INTEGER,
    GOALKEEPER_SAVES INTEGER,
    CONSTRAINT team_match_statistics_match_fk FOREIGN KEY (MATCH_ID) REFERENCES MATCHES(MATCH_ID),
    CONSTRAINT team_match_statistics_team_fk FOREIGN KEY (TEAM_ID) REFERENCES TEAMS(TEAM_ID),
    PRIMARY KEY (MATCH_ID, TEAM_ID)
);

CREATE TABLE TEAM_SHOTS_STATISTICS (
    MATCH_ID INTEGER NOT NULL,
    TEAM_ID INTEGER NOT NULL,
    SHOTS_ON_GOAL INTEGER,
    SHOTS_OFF_GOAL INTEGER,
    TOTAL_SHOTS INTEGER,
    BLOCKED_SHOTS INTEGER,
    SHOTS_INSIDEBOX INTEGER,
    SHOTS_OUTSIDEBOX INTEGER,
    CONSTRAINT team_shots_statistics_match_fk FOREIGN KEY (MATCH_ID) REFERENCES MATCHES(MATCH_ID),
    CONSTRAINT team_shots_statistics_team_fk FOREIGN KEY (TEAM_ID) REFERENCES TEAMS(TEAM_ID),
    PRIMARY KEY (MATCH_ID, TEAM_ID)
);


