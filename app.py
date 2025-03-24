from flask import Flask, render_template, jsonify, request
import psycopg2
from datetime import datetime
from src.example import scrape_stock_data
import threading
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global variable to store update status
update_status = {
    'progress': 0,
    'message': '',
    'is_updating': False,
    'last_updated': None
}

def get_db_connection():
    return psycopg2.connect(
        dbname="stock_scraper",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

def status_callback(progress, message):
    global update_status
    update_status['progress'] = progress
    update_status['message'] = message
    if progress == 100:
        update_status['is_updating'] = False
        update_status['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    elif progress == 0:
        update_status['is_updating'] = False
        update_status['message'] = f'Error: {message}'

def update_data():
    try:
        scrape_stock_data(status_callback)
    except Exception as e:
        logger.error(f"Error in update_data: {str(e)}")
        status_callback(0, f"Error: {str(e)}")

@app.route('/')
def index():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get unique values for filters
        cur.execute("SELECT DISTINCT sector FROM stock_data WHERE sector IS NOT NULL AND sector != '' ORDER BY sector")
        sectors = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT DISTINCT industry FROM stock_data WHERE industry IS NOT NULL AND industry != '' ORDER BY industry")
        industries = [row[0] for row in cur.fetchall()]
        
        cur.execute("SELECT DISTINCT country FROM stock_data WHERE country IS NOT NULL AND country != '' ORDER BY country")
        countries = [row[0] for row in cur.fetchall()]
        
        cur.close()
        conn.close()
        
        return render_template('index.html', 
                           sectors=sectors,
                           industries=industries,
                           countries=countries)
    except Exception as e:
        logger.error(f"Error in index route: {str(e)}")
        # Return empty lists for filters in case of error
        return render_template('index.html',
                           sectors=[],
                           industries=[],
                           countries=[])

@app.route('/get_stocks')
def get_stocks():
    try:
        # Get query parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        sort_column = request.args.get('sort_column', 'ticker')
        sort_direction = request.args.get('sort_direction', 'asc')
        search = request.args.get('search', '')
        sector_filter = request.args.get('sector', '')
        industry_filter = request.args.get('industry', '')
        country_filter = request.args.get('country', '')
        
        # Log the parameters
        logger.info(f"Fetching stocks with parameters: page={page}, per_page={per_page}, sort_column={sort_column}, " 
                   f"sort_direction={sort_direction}, search={search}, sector={sector_filter}, " 
                   f"industry={industry_filter}, country={country_filter}")
        
        # Calculate offset
        offset = (page - 1) * per_page
        
        # Build the WHERE clause
        where_clauses = []
        params = []
        
        if search:
            where_clauses.append("(ticker ILIKE %s OR company_name ILIKE %s)")
            search_pattern = f'%{search}%'
            params.extend([search_pattern, search_pattern])
        
        if sector_filter:
            where_clauses.append("sector = %s")
            params.append(sector_filter)
            
        if industry_filter:
            where_clauses.append("industry = %s")
            params.append(industry_filter)
            
        if country_filter:
            where_clauses.append("country = %s")
            params.append(country_filter)
            
        where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        # Connect to database
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Get total count
        count_query = f"SELECT COUNT(*) FROM stock_data WHERE {where_clause}"
        cur.execute(count_query, params)
        total_count = cur.fetchone()[0]
        
        # Get data
        query = f"""
            SELECT ticker, company_name, sector, industry, country,
                   market_cap, price, change_percent, volume,
                   pe_ratio, eps, dividend_yield, target_price,
                   peg_ratio, beta, data_source, last_updated
            FROM stock_data
            WHERE {where_clause}
            ORDER BY {sort_column} {sort_direction}
            LIMIT %s OFFSET %s
        """
        
        # Log the queries
        logger.info(f"Count query: {count_query}")
        logger.info(f"Data query: {query}")
        
        # Execute data query with all parameters
        params.extend([per_page, offset])
        cur.execute(query, params)
        
        # Fetch results
        results = cur.fetchall()
        
        # Convert to list of dicts
        columns = ['ticker', 'company_name', 'sector', 'industry', 'country',
                  'market_cap', 'price', 'change_percent', 'volume',
                  'pe_ratio', 'eps', 'dividend_yield', 'target_price',
                  'peg_ratio', 'beta', 'data_source', 'last_updated']
        
        stocks = []
        for row in results:
            stock = {}
            for i, value in enumerate(row):
                if isinstance(value, datetime):
                    stock[columns[i]] = value.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    stock[columns[i]] = value
            stocks.append(stock)
        
        # Close database connection
        cur.close()
        conn.close()
        
        return jsonify({
            'stocks': stocks,
            'total': total_count
        })
        
    except Exception as e:
        logger.error(f"Error in get_stocks: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/data-sources')
def data_sources():
    return render_template('data_sources.html')

@app.route('/get_sources_status')
def get_sources_status():
    global update_status
    return jsonify(update_status)

@app.route('/update_source', methods=['POST'])
def update_source():
    global update_status
    try:
        if update_status['is_updating']:
            return jsonify({'error': 'Update already in progress'}), 400
            
        source = request.json.get('source')
        if source != 'Finviz':
            return jsonify({'error': 'Invalid source'}), 400
            
        update_status['is_updating'] = True
        update_status['progress'] = 0
        update_status['message'] = 'Starting update...'
        
        # Start update in background thread
        thread = threading.Thread(target=update_data)
        thread.start()
        
        return jsonify({'message': 'Update started'})
        
    except Exception as e:
        logger.error(f"Error in update_source: {str(e)}")
        update_status['is_updating'] = False
        return jsonify({'error': str(e)}), 500

@app.route('/add_source', methods=['POST'])
def add_source():
    try:
        source_name = request.json.get('name')
        source_type = request.json.get('type')
        
        # Here you would typically validate and save the new source to your database
        # For now, we'll just return success
        
        return jsonify({'message': f'Added new {source_type} source: {source_name}'})
        
    except Exception as e:
        logger.error(f"Error in add_source: {str(e)}")
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)