-- Baseball Dugout: Create Tables
-- Run after 01_setup_database.sql

USE DATABASE BASEBALL_ANALYTICS;
USE SCHEMA PLAYER_DATA;
USE WAREHOUSE BASEBALL_WAREHOUSE;

-- Master (Player biographical info)
CREATE OR REPLACE TABLE MASTER (
    player_id VARCHAR(20) PRIMARY KEY,
    birth_year INTEGER,
    birth_month INTEGER,
    birth_day INTEGER,
    birth_country VARCHAR(100),
    birth_state VARCHAR(50),
    birth_city VARCHAR(100),
    death_year INTEGER,
    death_month INTEGER,
    death_day INTEGER,
    death_country VARCHAR(100),
    death_state VARCHAR(50),
    death_city VARCHAR(100),
    name_first VARCHAR(100),
    name_last VARCHAR(100),
    name_given VARCHAR(200),
    weight INTEGER,
    height INTEGER,
    bats VARCHAR(5),
    throws VARCHAR(5),
    debut DATE,
    final_game DATE,
    retro_id VARCHAR(20),
    bbref_id VARCHAR(20)
);

-- Batting (Season batting stats)
CREATE OR REPLACE TABLE BATTING (
    player_id VARCHAR(20),
    year_id INTEGER,
    stint INTEGER,
    team_id VARCHAR(10),
    lg_id VARCHAR(5),
    g INTEGER,
    ab INTEGER,
    r INTEGER,
    h INTEGER,
    doubles INTEGER,
    triples INTEGER,
    hr INTEGER,
    rbi INTEGER,
    sb INTEGER,
    cs INTEGER,
    bb INTEGER,
    so INTEGER,
    ibb INTEGER,
    hbp INTEGER,
    sh INTEGER,
    sf INTEGER,
    gidp INTEGER,
    PRIMARY KEY (player_id, year_id, stint)
);

-- Pitching (Season pitching stats)
CREATE OR REPLACE TABLE PITCHING (
    player_id VARCHAR(20),
    year_id INTEGER,
    stint INTEGER,
    team_id VARCHAR(10),
    lg_id VARCHAR(5),
    w INTEGER,
    l INTEGER,
    g INTEGER,
    gs INTEGER,
    cg INTEGER,
    sho INTEGER,
    sv INTEGER,
    ip_outs INTEGER,
    h INTEGER,
    er INTEGER,
    hr INTEGER,
    bb INTEGER,
    so INTEGER,
    ba_opp FLOAT,
    era FLOAT,
    ibb INTEGER,
    wp INTEGER,
    hbp INTEGER,
    bk INTEGER,
    bfp INTEGER,
    gf INTEGER,
    r INTEGER,
    sh INTEGER,
    sf INTEGER,
    gidp INTEGER,
    PRIMARY KEY (player_id, year_id, stint)
);

-- Teams (Team season records)
CREATE OR REPLACE TABLE TEAMS (
    year_id INTEGER,
    lg_id VARCHAR(5),
    team_id VARCHAR(10),
    franch_id VARCHAR(10),
    div_id VARCHAR(5),
    rank INTEGER,
    g INTEGER,
    g_home INTEGER,
    w INTEGER,
    l INTEGER,
    div_win VARCHAR(5),
    wc_win VARCHAR(5),
    lg_win VARCHAR(5),
    ws_win VARCHAR(5),
    r INTEGER,
    ab INTEGER,
    h INTEGER,
    doubles INTEGER,
    triples INTEGER,
    hr INTEGER,
    bb INTEGER,
    so INTEGER,
    sb INTEGER,
    cs INTEGER,
    hbp INTEGER,
    sf INTEGER,
    ra INTEGER,
    er INTEGER,
    era FLOAT,
    cg INTEGER,
    sho INTEGER,
    sv INTEGER,
    ip_outs INTEGER,
    ha INTEGER,
    hra INTEGER,
    bba INTEGER,
    soa INTEGER,
    e INTEGER,
    dp INTEGER,
    fp FLOAT,
    name VARCHAR(100),
    park VARCHAR(200),
    attendance INTEGER,
    bpf INTEGER,
    ppf INTEGER,
    team_id_br VARCHAR(10),
    team_id_lahman45 VARCHAR(10),
    team_id_retro VARCHAR(10),
    PRIMARY KEY (year_id, team_id)
);

-- Hall of Fame
CREATE OR REPLACE TABLE HALLOFFAME (
    player_id VARCHAR(20),
    year_id INTEGER,
    voted_by VARCHAR(50),
    ballots INTEGER,
    needed INTEGER,
    votes INTEGER,
    inducted VARCHAR(5),
    category VARCHAR(50),
    needed_note VARCHAR(200),
    PRIMARY KEY (player_id, year_id, voted_by)
);

-- All-Star appearances
CREATE OR REPLACE TABLE ALLSTARFULL (
    player_id VARCHAR(20),
    year_id INTEGER,
    game_num INTEGER,
    game_id VARCHAR(20),
    team_id VARCHAR(10),
    lg_id VARCHAR(5),
    gp INTEGER,
    starting_pos INTEGER,
    PRIMARY KEY (player_id, year_id, game_num)
);

-- Salaries
CREATE OR REPLACE TABLE SALARIES (
    year_id INTEGER,
    team_id VARCHAR(10),
    lg_id VARCHAR(5),
    player_id VARCHAR(20),
    salary INTEGER,
    PRIMARY KEY (year_id, team_id, player_id)
);

-- Teams Franchises (for team name lookups)
CREATE OR REPLACE TABLE TEAMS_FRANCHISES (
    franch_id VARCHAR(10) PRIMARY KEY,
    franch_name VARCHAR(100),
    active VARCHAR(5),
    na_assoc VARCHAR(10)
);

SELECT 'All tables created!' AS status;

