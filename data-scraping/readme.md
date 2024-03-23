**Data Collection.py**

# Energy Data Collection Scripts

This repository contains a collection of Python scripts designed to retrieve various energy-related data from different APIs and websites. Each script fetches specific types of data, processes it, and saves it to a PostgreSQL database for further analysis.

## Dependencies

- `pandas`: A powerful data manipulation library in Python.
- `requests`: A library for making HTTP requests to APIs and websites.
- `sqlalchemy`: Provides a way to interact with SQL databases using Python.
- `bs4` (Beautiful Soup): A library for web scraping.
- `json`: Used for JSON file operations.

## Scripts and Functions
    - `get_default_datetime(now=None)`: Returns the current datetime object or a provided one.
    - `get_yesterday_date_range(now=None)`: Returns the start and end datetime strings for yesterday.
    - `log(message, now=None)`: Appends a log message to a log file with the current timestamp.
    - `mibgas_dayahead(now=None)`: Downloads the MIBGAS day-ahead electricity market report, saves it as a CSV file, reads the CSV file, converts it to a DataFrame, and saves it to a PostgreSQL database.
    
### Energy Charts API and ENTSO-E Website
    - `price_data(now=None)`: Fetches price data from the Energy Charts API for various countries, converts it to a DataFrame, and saves it to a PostgreSQL database.
    - `price_spot_market(now=None)`: Fetches spot market price data from the Energy Charts API.
    - `save_to_json(cookies={'emfip-welcome': 'true'}, now=None)`: Scrapes data from the ENTSO-E website, processes it, and saves it as a JSON file.

### International Energy Agency (IEA) API
    - `dayAheadPrices(now=None)`: Fetches day-ahead price data from the ENTSO-E website for different countries and saves it to a PostgreSQL database.
    - `prices(now=None)`: Fetches energy price data from the IEA API for different countries and saves it to a PostgreSQL database.
    - `trade(now=None)`: Fetches trade data from the IEA API for different countries and saves it to a PostgreSQL database.
    - `trade_Net(now=None)`: Fetches trade net data from the IEA API and saves it to a PostgreSQL database.
    - `trade_netflows(now=None)`: Fetches trade netflows data from the IEA API and saves it to a PostgreSQL database.

### Nord Pool API
    - `update_symbol_volume_peak(row, new_data, symbol)`: Updates the peak volume data for a symbol.
    - `update_symbol_volume(row, new_data, symbol)`: Updates the volume data for a symbol.
    - `fetch_and_update_volumes(all_symbols,now=None)`: Fetches volume data for all symbols, updates the volume and peak volume information, and saves it to the PostgreSQL database.
    - `get_price(now=None)`: Fetches price data from the Nord Pool API and saves it to the PostgreSQL database.
    - `get_capacity(symbols,now=None)`: Fetches capacity data for the specified symbols, adds additional information, and saves it to the PostgreSQL database.
    - `get_flow(symbols, now=None)`: Fetches flow data for the specified symbols and saves it to the PostgreSQL database.
    - `get_intraday(symbols, now=None)`: Fetches intraday market statistics for the specified symbols and saves it to the PostgreSQL database.
    - `get_consumption(now=None)`: Fetches consumption data from the Nord Pool API and saves it to the PostgreSQL database.
    - `get_production(symbols, now=None)`: Fetches production data for the specified symbols and saves it to the PostgreSQL database.
    - `get_balance_data(now=None)`: Fetches balance data from the Nord Pool API and saves it to the PostgreSQL database.

## Usage

1. Ensure you have all the dependencies installed (`pandas`, `requests`, `sqlalchemy`, `bs4`, `json`).
2. Configure your PostgreSQL database connection details (`create_engine` function calls).
3. Run the desired script(s) to start fetching and saving energy-related data to the PostgreSQL database.



----
**save_to_postgresql_dag.py**

**Functionality:**

1. **Data Retrieval:**
   - Retrieves energy-related data from multiple sources.
   - Data types include price data, spot market data, day-ahead prices, trade data, and trade netflows...

2. **Data Processing:**
   - Processes the retrieved data, which may involve parsing HTML content, converting JSON responses to pandas DataFrames, and manipulating DataFrame structures.

3. **Database Interaction:**
   - Stores the processed data into a PostgreSQL database.
   - Utilizes SQLAlchemy for database interaction within a context manager to ensure proper handling of database connections.

4. **Email Notification:**
   - Sends an email notification upon successful execution of all tasks within the DAG.

**Dependencies:**

1. **Python Libraries:**
   - pandas: Data manipulation and processing.
   - requests: HTTP requests for fetching data from APIs and web sources.
   - BeautifulSoup: HTML parsing for web scraping purposes.
   - json: Handling JSON data.
   - airflow: Workflow orchestration and task scheduling.
   - psycopg2: PostgreSQL database adapter (implicitly required by airflow).
