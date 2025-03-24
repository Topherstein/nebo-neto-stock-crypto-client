from finvizfinance.screener.overview import Overview
import psycopg2
from datetime import datetime
import logging
import traceback
import sys
import pandas as pd

# Configure logging to show on console with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def update_status_with_log(status_callback, progress, message):
    """Helper function to update status and log it"""
    if status_callback:
        status_callback(progress, message)
    logger.info(f"Progress {progress}%: {message}")

def create_table(conn):
    cur = conn.cursor()
    cur.execute("""
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
        )
    """)
    conn.commit()
    cur.close()

def convert_to_number(value):
    if not value or value == '-':
        return None
    
    try:
        # Remove any commas and '%' signs
        value = str(value).replace(',', '').replace('%', '')
        
        # Convert K/M/B/T to actual numbers
        if value.endswith('K'):
            return float(value[:-1]) * 1000
        elif value.endswith('M'):
            return float(value[:-1]) * 1000000
        elif value.endswith('B'):
            return float(value[:-1]) * 1000000000
        elif value.endswith('T'):
            return float(value[:-1]) * 1000000000000
        else:
            return float(value)
    except (ValueError, TypeError):
        return None

def scrape_stock_data(status_callback=None):
    try:
        update_status_with_log(status_callback, 5, 'Initializing database connection...')
        
        # Connect to database
        conn = psycopg2.connect(
            dbname="stock_scraper",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        
        update_status_with_log(status_callback, 10, 'Creating/verifying database table...')
        # Create table if it doesn't exist
        create_table(conn)
        
        update_status_with_log(status_callback, 20, 'Setting up Finviz screener...')
        # Set up the screener for stocks under $20
        logger.debug("Creating Finviz screener...")
        foverview = Overview()
        
        # Set up filter for stocks under $20
        filters_dict = {'Price': 'Under $20'}
        
        update_status_with_log(status_callback, 30, 'Applying price filter (Under $20)...')
        foverview.set_filter(filters_dict=filters_dict)
        
        update_status_with_log(status_callback, 40, 'Fetching stock data from Finviz...')
        try:
            # Get the stock data
            stocks_df = foverview.screener_view()
            
            # Print DataFrame info
            logger.info("DataFrame Info:")
            logger.info(stocks_df.info())
            
            # Print first few rows
            logger.info("First few rows of data:")
            logger.info(stocks_df.head())
            
            # Print column names
            logger.info("Column names:")
            logger.info(stocks_df.columns.tolist())
            
            if stocks_df.empty:
                logger.error("No data received from Finviz")
                raise Exception("No stock data received from Finviz")
                
            total_stocks = len(stocks_df)
            update_status_with_log(status_callback, 50, f'Retrieved {total_stocks} stocks from Finviz')
            
            # Clear existing Finviz data
            update_status_with_log(status_callback, 55, 'Clearing existing Finviz data from database...')
            cur = conn.cursor()
            cur.execute("DELETE FROM stock_data WHERE data_source = 'Finviz'")
            conn.commit()
            
            update_status_with_log(status_callback, 60, f'Starting to process {total_stocks} stocks...')
            
            # Process each stock
            successful_inserts = 0
            failed_inserts = 0
            
            for i, (index, stock) in enumerate(stocks_df.iterrows()):
                try:
                    # Calculate progress (from 60 to 90)
                    progress = int(60 + (i / total_stocks * 30))
                    ticker = stock.get('Ticker', 'Unknown')
                    
                    update_status_with_log(
                        status_callback, 
                        progress, 
                        f'Processing {ticker} ({i+1}/{total_stocks}) | Success: {successful_inserts} | Failed: {failed_inserts}'
                    )
                    
                    # Log detailed stock info
                    logger.debug(f"Stock data for {ticker}: {stock.to_dict()}")
                    
                    # Insert data with safe value extraction
                    cur.execute("""
                        INSERT INTO stock_data (
                            ticker, company_name, sector, industry, country,
                            market_cap, price, change_percent, volume,
                            pe_ratio, eps, dividend_yield, target_price,
                            peg_ratio, beta, data_source, last_updated
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        ticker,
                        stock.get('Company', ''),
                        stock.get('Sector', ''),
                        stock.get('Industry', ''),
                        stock.get('Country', ''),
                        convert_to_number(stock.get('Market Cap', '0')),
                        convert_to_number(stock.get('Price', '0')),
                        convert_to_number(stock.get('Change', '0')),
                        convert_to_number(stock.get('Volume', '0')),
                        convert_to_number(stock.get('P/E', '0')),
                        convert_to_number(stock.get('EPS (ttm)', '0')),
                        convert_to_number(stock.get('Dividend', '0')),
                        convert_to_number(stock.get('Target Price', '0')),
                        convert_to_number(stock.get('PEG', '0')),
                        convert_to_number(stock.get('Beta', '0')),
                        'Finviz',
                        datetime.now()
                    ))
                    successful_inserts += 1
                    
                except Exception as e:
                    failed_inserts += 1
                    logger.error(f"Error processing stock {ticker}: {str(e)}")
                    logger.error(traceback.format_exc())
                    continue
            
            # Commit the transaction
            update_status_with_log(status_callback, 90, 'Committing changes to database...')
            conn.commit()
            
            # Close database connection
            cur.close()
            conn.close()
            
            final_message = (
                f'Update completed | '
                f'Total stocks: {total_stocks} | '
                f'Successful: {successful_inserts} | '
                f'Failed: {failed_inserts}'
            )
            update_status_with_log(status_callback, 100, final_message)
            
        except Exception as e:
            error_msg = f"Error processing Finviz data: {str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            if status_callback:
                status_callback(0, error_msg)
            raise
            
    except Exception as e:
        error_msg = f"Error in scrape_stock_data: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        if status_callback:
            status_callback(0, error_msg)
        raise

if __name__ == "__main__":
    scrape_stock_data()