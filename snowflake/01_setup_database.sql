-- Baseball Dugout: Database Setup
-- Run this first to create the database structure

-- Create warehouse (compute)
CREATE WAREHOUSE IF NOT EXISTS BASEBALL_WAREHOUSE
    WITH WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

-- Create database
CREATE DATABASE IF NOT EXISTS BASEBALL_ANALYTICS;

-- Use the database
USE DATABASE BASEBALL_ANALYTICS;

-- Create schema
CREATE SCHEMA IF NOT EXISTS PLAYER_DATA;

-- Set context
USE WAREHOUSE BASEBALL_WAREHOUSE;
USE SCHEMA PLAYER_DATA;

SELECT 'Database setup complete!' AS status;

