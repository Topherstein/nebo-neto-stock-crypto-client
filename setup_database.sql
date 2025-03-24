-- Create the database if it doesn't exist
CREATE DATABASE stock_scraper;

-- Connect to the database
\c stock_scraper;

-- Create the stock_data table
CREATE TABLE IF NOT EXISTS stock_data (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name TEXT,
    sector VARCHAR(100),
    industry VARCHAR(100),
    country VARCHAR(100),
    market_cap NUMERIC,
    price NUMERIC,
    change_percent NUMERIC,
    volume NUMERIC,
    pe_ratio NUMERIC,
    eps NUMERIC,
    dividend_yield NUMERIC,
    target_price NUMERIC,
    peg_ratio NUMERIC,
    beta NUMERIC,
    data_source VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);