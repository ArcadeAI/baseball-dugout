-- Baseball Dugout: Create File Format and Stage
-- Run after 02_create_tables.sql

USE DATABASE BASEBALL_ANALYTICS;
USE SCHEMA PLAYER_DATA;
USE WAREHOUSE BASEBALL_WAREHOUSE;

-- Create CSV file format
CREATE OR REPLACE FILE FORMAT BASEBALL_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('\\N', 'NULL', '')
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- Create internal stage for uploading CSV files
CREATE OR REPLACE STAGE BASEBALL_DATA_STAGE
    FILE_FORMAT = BASEBALL_CSV_FORMAT;

SELECT 'Stage created! Now upload CSV files using Snowsight.' AS status;

-- ============================================================
-- UPLOAD INSTRUCTIONS (Using Snowsight Web UI)
-- ============================================================
-- 
-- 1. In Snowsight, go to: Data → Databases → BASEBALL_ANALYTICS → PLAYER_DATA → Stages
-- 2. Click on BASEBALL_DATA_STAGE
-- 3. Click "+ Files" button (top right)
-- 4. Upload these CSV files from the Baseball Databank:
--    - Master.csv
--    - Batting.csv
--    - Pitching.csv
--    - Teams.csv
--    - HallOfFame.csv
--    - AllstarFull.csv
--    - Salaries.csv
--    - TeamsFranchises.csv
--
-- After upload, run 04_load_data.sql
-- ============================================================

