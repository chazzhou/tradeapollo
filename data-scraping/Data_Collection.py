import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from collections import defaultdict
import json

from io import StringIO

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

country_codes = [
    'AT', 'BE', 'BG', 'CH', 'CZ', 'DK1', 'DK2', 'EE', 'ES', 'FI', 'FR', 'GR', 'HR',
    'HU', 'IT-Calabria', 'IT-Centre-North', 'IT-Centre-South', 'IT-North', 'IT-SACOAC',
    'IT-SACODC', 'IT-Sardinia', 'IT-Sicily', 'IT-South', 'LT', 'LV','NL',
    'NO1', 'NO2', 'NO2NSL', 'NO3', 'NO4', 'NO5', 'PL', 'PT', 'RO', 'RS', 'SE1', 'SE2',
    'SE3', 'SE4', 'SI', 'SK'
    ]


def get_default_datetime(now=None):
    return now or datetime.now()

def get_yesterday_date_range(now=None):
    today = get_default_datetime(now)
    yesterday = today - timedelta(days=1)
    start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + '+01:00'
    end_date = today.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + '+01:00'
    return start_date, end_date

def log(message, now=None):
    current_date = get_default_datetime(now).strftime("%Y-%m-%d")
    log_file_path = f"log_{current_date}.txt"

    with open(log_file_path, "a") as log_file:
        log_file.write(f"{get_default_datetime(now).strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")

def price_data(now=None):
    # API endpoint and parameters
    api_url = 'https://api.energy-charts.info/price'
    start_date, end_date = get_yesterday_date_range(now)

    for country in country_codes:
        # Define the parameters as a dictionary
        api_params = {
            'bzn': country,
            'start': start_date,
            'end': end_date
        }

        # Make the API request
        response = requests.get(api_url, params=api_params)

        # Check if the request was successful
        if response.status_code == 200:
            # Convert the JSON response to a DataFrame
            data = response.json()
            df = pd.DataFrame(data)

            df['country bidding zones']=country
            df['start']=start_date
            df['end']=end_date
            df['unix_seconds'] = pd.to_datetime(df['unix_seconds'], unit='s')

            # Save DataFrame to PostgreSQL
            engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
            table_name = 'price_data'

            # Try to append data to the existing table, create if not exists
            try:
                df.to_sql(table_name, engine, if_exists='append', index=False)
                log(f"Data appended to PostgreSQL table '{table_name}' | country: '{country}' successfully.")
            except Exception as e:
                log(f"Error: Unable to append data to table '{table_name}' | country: '{country}'. Error message: {str(e)}")

        else:
            # Log the error if the API request fails
            log(f"Error: Unable to fetch data from API. Status code: {response.status_code}")

def price_spot_market(now=None):
    api_url = 'https://api.energy-charts.info/price_spot_market'
    start_date, end_date = get_yesterday_date_range(now)

    for country in country_codes:
        api_params = {
            'bzn': country,
            'start': start_date,
            'end': end_date
        }

        response = requests.get(api_url, params=api_params)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data)
            df.columns=['xAxisValues (Unix timestamp)','Day Ahead Auction']

            df['start']=start_date
            df['end']=end_date
            df['xAxisValues (Unix timestamp)'] = pd.to_datetime(df['xAxisValues (Unix timestamp)'], unit='s')
            df['country bidding zones'] = country

            
            engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
            table_name = 'price_spot_market'

            try:
                df.to_sql(table_name, engine, if_exists='append', index=False)
                log(f"Data appended to PostgreSQL table '{table_name}' | country: '{country}' successfully.")
            except Exception as e:
                log(f"Error: Unable to append data to table '{table_name}' | country: '{country}'. Error message: {str(e)}")

        else:
            log(f"Error: Unable to fetch data from API. Status code: {response.status_code}")


def save_to_json(cookies={'emfip-welcome': 'true'}, now=None):
    yesterday = get_default_datetime(now) - timedelta(days=1)
    yesterday_date = yesterday.strftime("%d.%m.%Y")
    url="https://transparency.entsoe.eu/transmission-domain/r2/dayAheadPrices/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime={}|CET|DAY&biddingZone.values=CTY|10YES-REE------0!BZN|10YES-REE------0&resolution.values=PT60M&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)".format(yesterday_date)
    my_defaultdict = defaultdict(list)

    # Make the request with cookies
    response = requests.get(url, cookies=cookies)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the HTML content
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find div tags with class="all dv-filter-hierarchic-wrapper border"
        div_tags = soup.find_all("div", class_="dv-filter-hierarchic-wrapper")
        # Iterate through each div tag
        for div_tag in div_tags:
            # Extract label
            label = div_tag.find("label").text.strip()

            # Extract input tags with name="biddingZone.values"
            bidding_zone_values_tags = div_tag.find_all(lambda tag: tag.name == "input" and tag.get("name") == "biddingZone.values")

            # Extract values and store them in a list
            values_list = [tag.get("value") for tag in bidding_zone_values_tags]
            id_list = [tag.get("id") for tag in bidding_zone_values_tags]

            region = [soup.find("label", {"for": ids}).text for ids in id_list]

            result_list = list(zip(values_list, region))
            my_defaultdict[label] = result_list

    # Save the result as a JSON file
    with open("result.json", "w") as json_file:
        json.dump(my_defaultdict, json_file)



def dayAheadPrices(now=None):
    # Assuming my_defaultdict is defined elsewhere in your code
    # Replace it with your actual data structure
    with open("/kaggle/working/result.json", "r") as json_file:
        data = json.load(json_file)

    my_defaultdict=defaultdict(list, data)

    yesterday = get_default_datetime(now)
    yesterday_date = yesterday.strftime("%d.%m.%Y")

    for i in my_defaultdict:
        for j in my_defaultdict[i]:
            url = "https://transparency.entsoe.eu/transmission-domain/r2/dayAheadPrices/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime={}+00:00|CET|DAY&biddingZone.values={}&resolution.values=PT60M&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)".format(yesterday_date, j[0])

            # Define your cookies
            cookies = {
                'emfip-welcome': 'true'
            }

            # Make the request with cookies
            response = requests.get(url, cookies=cookies)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the HTML content
                soup = BeautifulSoup(response.content, 'html.parser')

                # Now you can proceed with the rest of your code
                tables = pd.read_html(str(soup))

                # Save DataFrame to PostgreSQL
                df = tables[0] 


                engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
                table_name = 'dayAheadPrices'

                df.columns=['MTU','Day-ahead Price EUR / MWh']

                df['date']=yesterday
                df['country'] = i
                df['country bidding zones'] = j[1]

                df.replace('n/e', None, inplace=True)
  
                # Try to append data to the existing table, create if not exists
                try:
                    df.to_sql(table_name, engine, if_exists='append', index=False)
                    log(f"Data appended to PostgreSQL table '{table_name}' | country '{i}':'{j[0]}' successfully.")
                except Exception as e:
                    log(f"Error: Unable to append data to table '{table_name}' | country '{i}':'{j[0]}'. Error message: {str(e)}")

            else:
                # Log the error if the request fails
                log(f"Error: Failed to retrieve the webpage. Status code: {response.status_code}")


def prices(now=None):
    today_date = get_default_datetime(now)
    yesterday_date = today_date - timedelta(days=1)
    start_date_str = yesterday_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")
    # Make the initial request to get the list of countries
    initial_url = f"https://api.iea.org/rte/list/countries?from={start_date_str}&to={end_date_str}&type=price"
    response_countries = requests.get(initial_url)
    countries_data = response_countries.json()

    # Parse and print the ISO3 values
    iso3_values = [country["ISO3"] for country in countries_data]

    all_results = []



    # Make individual requests for each country and append the results
    for iso3 in iso3_values:
        country_url = f"https://api.iea.org/rte/price/{iso3}/timeseries?from={start_date_str}&to={end_date_str}&precision=hour&currency=EUR"
        response_country = requests.get(country_url)
        country_data = response_country.json()

        for result in country_data:
            result["ISO3"] = iso3
            all_results.append(result)

        # Create a DataFrame from the list of results
        df = pd.DataFrame(all_results)

        df['start date']=today_date
        df['end date'] = yesterday_date
        df['Country'] = iso3

        # Save DataFrame to PostgreSQL
        engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
        table_name = 'prices'


        df.to_sql(table_name, engine, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table '{table_name}' successfully.")


def trade(now=None):
    today_date = get_default_datetime(now)
    yesterday_date = today_date - timedelta(days=1)
    start_date_str = yesterday_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")

    # Make the initial request to get the list of countries
    initial_url = f"https://api.iea.org/rte/list/countries?from={start_date_str}&to={end_date_str}&type=trade"
    response_countries = requests.get(initial_url)
    countries_data = response_countries.json()

    # Parse and print the ISO3 values
    iso3_values = [country["ISO3"] for country in countries_data]


    all_results = []

    # Make individual requests for each country and append the results
    for iso3 in iso3_values:
        country_url=f"https://api.iea.org/rte/trade/subnational/{iso3}?from={start_date_str}&to={end_date_str}"
        response_country = requests.get(country_url)
        country_data = response_country.json()

        for result in country_data:
            result["ISO3"] = iso3
            all_results.append(result)

        # Create a DataFrame from the list of results
        df = pd.DataFrame(all_results)

        df['start date']=today_date
        df['end date'] = yesterday_date
        df['Country'] = iso3
        
        # Save DataFrame to PostgreSQL
        engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
        table_name = 'trade'


        df.to_sql(table_name, engine, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table '{table_name}' successfully.")


def trade_Net(now=None):
    today_date = get_default_datetime(now)
    yesterday_date = today_date - timedelta(days=1)
    start_date_str = yesterday_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")
    country_url=f"https://api.iea.org/rte/trade?from={start_date_str}&to={end_date_str}"
    response_country = requests.get(country_url)
    country_data = response_country.json()

    df_list=[]
    for result in country_data:
        df = pd.DataFrame([result])
        df['start date']=today_date
        df['end date'] = yesterday_date
        df_list.append(df)

    df_list = pd.concat(df_list, ignore_index=True)
    # Save DataFrame to PostgreSQL
    engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
    table_name = 'trade_Net'


    df_list.to_sql(table_name, engine, if_exists='append', index=False)
    log(f"Data appended to PostgreSQL table '{table_name}' successfully.")

trade_Net()

def trade_netflows(now=None):
    today_date = get_default_datetime(now)
    yesterday_date = today_date - timedelta(days=1)

    start_date_str = yesterday_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")
    country_url=f"https://api.iea.org/rte/trade/netflows?from={start_date_str}&to={end_date_str}"
    response_country = requests.get(country_url)
    country_data = response_country.json()

    df_list=[]
    for result in country_data:
        df = pd.DataFrame([result])
        df['start date']=today_date
        df['end date'] = yesterday_date

        df_list.append(df)

    df_list = pd.concat(df_list, ignore_index=True)
        
    # Save DataFrame to PostgreSQL
    engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
    table_name = 'trade_netflows'


    df_list.to_sql(table_name, engine, if_exists='append', index=False)
    log(f"Data appended to PostgreSQL table '{table_name}' successfully.")

def mibgas_dayahead(now=None):
    """
    Download the OPCOM day-ahead electricity market report for yesterday's date and save it to PostgreSQL.
    """
    table_name='mibgas'
    filepath=''
    # Ensure SSL warnings are not shown
    requests.packages.urllib3.disable_warnings()

    # Set start_date to yesterday's date
    start_date = get_default_datetime(now) - timedelta(days=1)

    # Format the date as needed for the URL
    date_str = start_date.strftime("%d/%m/%Y")
    url = f'https://www.mibgas.es/en/ajax/table/daily-price/pvb/export?date={date_str}'

    # Make the request, WARNING: Disabling SSL verification is insecure
    response = requests.get(url, verify=False)

    if response.status_code == 200:
        # Define the file path dynamically based on the date
        file_path = f'{filepath}{start_date.strftime("%Y-%m-%d")}_mibgas_dayahead_el.csv'
        with open(file_path, 'wb') as file:
            file.write(response.content)
        log(f"File for {date_str} downloaded successfully.")
        
        # Attempt to read the first few lines of the file to guess the correct separator
        with open(file_path, 'r') as file:
            first_lines = file.readlines()  # Read the first 1000 bytes to get a sample 

        # Convert extracted lines to a DataFrame
        extracted_data_str = ''.join(first_lines)
        extracted_data = pd.read_csv(StringIO(extracted_data_str))
        extracted_data['date'] = start_date
        extracted_data['country'] = "Spain"

        # Save DataFrame to PostgreSQL
        engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
        extracted_data.to_sql(table_name, engine, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table '{table_name}' successfully.")
        
    else:
        log(f"Failed to download the file for {date_str}. Status code: {response.status_code}")




all_symbols =     ['EE', 'LT', 'LV', 'AT', 'BE', 'FR', 'GER', 'NL', 'PL',
                   'DK1', 'DK2', 'FI', 'NO1', 'NO2','NO3', 'NO4', 'NO5',
                   'SE1', 'SE2', 'SE3', 'SE4']

intraday_symbols =  ['EE', 'LT', 'LV', '50Hz', 'AMP','AT', 'BE',
                     'FR', 'NL', 'PL', 'TBW', 'TTG', 'DK1', 'DK2',
                     'FI', 'NO1', 'NO2','NO3', 'NO4', 'NO5', 
                     'SE1', 'SE2', 'SE3', 'SE4', 'UK']

production_symbols = ['EE', 'LT', 'LV', 'DK1', 'DK2', 'FI', 'NO1', 'NO2','NO3', 'NO4', 'NO5', 'SE1', 'SE2', 'SE3', 'SE4']


def update_symbol_volume_peak(row, new_data, symbol):
    existing_data = row.get('symbol_volume_peak', {})
    
    if not isinstance(existing_data, dict):
        existing_data = {}

    new_buy_sell = new_data[new_data['blockName'] == row['blockName']]
    
    if not new_buy_sell.empty:
        existing_data[symbol] = {'buy': new_buy_sell.iloc[0]['buy'], 'sell': new_buy_sell.iloc[0]['sell']}
    
    return existing_data

def update_symbol_volume(row, new_data, symbol):
    existing_data = row.get('symbol_volume', {})
    new_buy_sell = new_data[new_data['deliveryStart'] == row['deliveryStart']]

    if not isinstance(existing_data, dict):
        existing_data = {}

    if not new_buy_sell.empty:
        existing_data[symbol] = {'buy': new_buy_sell.iloc[0]['buy'], 'sell': new_buy_sell.iloc[0]['sell']}
    return existing_data

def fetch_and_update_volumes(all_symbols,now=None):

    yesterday = get_default_datetime(now)- timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')
    
    url_volumes = f'https://dataportal-api.nordpoolgroup.com/api/DayAheadVolumes?date={date}&market=DayAhead&deliveryArea=EE'
    resp = requests.get(url_volumes)
    a = resp.json()
    
    all_peaks = pd.DataFrame(a['blockVolumes'])
    all_peaks = all_peaks[['blockName', 'blockStart', 'blockEnd']]
    
    all_volumes = pd.DataFrame(a['volumes'])
    all_volumes = all_volumes[['deliveryStart', 'deliveryEnd']]
    
    for symbol in all_symbols:
        # gather info from all symbols and put in one big dataframe
        url_volumes = f'https://dataportal-api.nordpoolgroup.com/api/DayAheadVolumes?date={date}&market=DayAhead&deliveryArea={symbol}'
        resp = requests.get(url_volumes)
        a = resp.json()
        
        peak_symbol = pd.DataFrame(a['blockVolumes'])
        volume_df = pd.DataFrame(a['volumes'])
        
        all_peaks['symbol_volume_peak'] = all_peaks.apply(update_symbol_volume_peak, axis=1, args=(peak_symbol, symbol))
        all_peaks['symbol_volume_peak']=all_peaks['symbol_volume_peak'].astype(str)

        all_volumes['symbol_volume'] = all_volumes.apply(update_symbol_volume, axis=1, args=(volume_df, symbol))
        all_volumes['symbol_volume']=all_volumes['symbol_volume'].astype(str)
        
        engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
        all_peaks.to_sql('all_peaks', engine, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table all_peaks successfully.")
        
        engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
        all_volumes.to_sql('all_volumes', engine, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table all_volumes successfully.")

def get_price(now=None):
    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')
    url = f'https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date={date}&market=DayAhead&deliveryArea=EE,LT,LV,AT,BE,FR,GER,NL,PL,DK1,DK2,FI,NO1,NO2,NO3,NO4,NO5,SE1,SE2,SE3,SE4,SYS&currency=EUR'
    
    resp = requests.get(url)
    data= resp.json()
    prices_df = pd.DataFrame(data['multiAreaEntries'])

    prices_df['entryPerArea']=prices_df['entryPerArea'].astype(str)

    engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
    prices_df.to_sql('all_prices', engine, if_exists='append', index=False)
    log(f"Data appended to PostgreSQL table all_prices successfully.")

    peak_df = pd.DataFrame(data['blockPriceAggregates'])
    peak_df['averagePricePerArea']=peak_df['averagePricePerArea'].astype(str)

    engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
    peak_df.to_sql('all_prices_peaks ', engine, if_exists='append', index=False)
    log(f"Data appended to PostgreSQL table all_prices_peaks successfully.")

def get_capacity(symbols,now=None):
    # Initialize an empty DataFrame for the final result
    all_data = pd.DataFrame()
    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')
    for symbol in symbols:
        url_capacity = f'https://dataportal-api.nordpoolgroup.com/api/DayAheadCapacities?date={date}&market=DayAhead&deliveryArea={symbol}'
        
        # Make the web request
        resp = requests.get(url_capacity)
        try:
            if resp.status_code == 200:  # Successful response
                data = resp.json()
                
                # Create a DataFrame from the capacities part of the response
                symbol_data = pd.DataFrame(data['capacities'])
                
                # Add additional info
                symbol_data['deliveryArea'] = str(data['deliveryArea'])
                symbol_data['importAreas'] = str(data['importAreas'])
                symbol_data['exportAreas'] = str(data['exportAreas'])
            

                # Concatenate with the main DataFrame
                all_data = pd.concat([all_data, symbol_data], ignore_index=True)
                all_data['importsByConnection']=all_data['importsByConnection'].astype(str)
                all_data['exportsByConnection']=all_data['exportsByConnection'].astype(str)
                
                all_data['importsByGroupRestriction']=all_data['importsByGroupRestriction'].astype(str)
                all_data['exportsByGroupRestriction']=all_data['exportsByGroupRestriction'].astype(str)
                
                all_data['importsByGroupRestriction'].fillna("{}", inplace=True)
                all_data['exportsByGroupRestriction'].fillna("{}", inplace=True)


                all_data['importAreas']=all_data['importAreas'].astype(str)
                all_data['exportAreas']=all_data['exportAreas'].astype(str)

                            
                engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
                all_data.to_sql('all_capacity', engine, if_exists='append', index=False)
                log(f"Data appended to PostgreSQL table all_capacity successfully.")
            else:
                log(f"Failed to fetch data HTTP Status(get_capacity): {resp.status_code}")
        except:
            pass
def get_flow(symbols,now=None):
    # Initialize an empty DataFrame for the final result
    all_data = pd.DataFrame()
    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')

    for symbol in symbols:
        try:
            url_flow = f'https://dataportal-api.nordpoolgroup.com/api/DayAheadFlow?date={date}&market=DayAhead&deliveryArea={symbol}'
            # Make the web request
            resp = requests.get(url_flow)
            if resp.status_code == 200:  # Successful response
                data = resp.json()
                
                # Create a DataFrame from the capacities part of the response
                symbol_data = pd.DataFrame(data['flows'])
                
                # Add additional info
                symbol_data['connectionAreas'] = str(data['connectionAreas'])
                symbol_data['symbol'] = symbol

                # Concatenate with the main DataFrame
                all_data = pd.concat([all_data, symbol_data], ignore_index=True)
                all_data['byConnectionArea']=all_data['byConnectionArea'].astype(str)
                all_data['connectionAreas']=all_data['connectionAreas'].astype(str)
                
                engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
                all_data.to_sql('all_flow', engine, if_exists='append', index=False)
         
                log(f"Data appended to PostgreSQL table all_flow successfully.")
            else:
                log(f"Failed to fetch data HTTP Status(get_flow): {resp.status_code}")
        except:
            pass

    
def get_intraday(symbols,now=None):
    # Initialize an empty DataFrame for the final result
    all_data = pd.DataFrame()
    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')
    for symbol in symbols:
        url_intraday = f'https://dataportal-api.nordpoolgroup.com/api/IntradayMarketStatistics?date={date}&deliveryArea={symbol}'
        # Make the web request
        resp = requests.get(url_intraday)
        if resp.status_code == 200:  # Successful response
            data = resp.json()
            
            # Create a DataFrame from the capacities part of the response
            symbol_data = pd.DataFrame(data['contracts'])
            
            # Add additional info
            symbol_data['symbol'] = symbol

            # Concatenate with the main DataFrame
            all_data = pd.concat([all_data, symbol_data], ignore_index=True)
            log(f"Data appended to PostgreSQL table all_intraday successfully.")
        else:
            log(f"Failed to fetch data HTTP Status(get_intraday): {resp.status_code}")
    engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
    all_data.to_sql('all_intraday', engine, if_exists='append', index=False)
def get_consumption(now=None):
    # Initialize an empty DataFrame for the final result
    all_data = pd.DataFrame()
    
    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')

    url_consumption =f'https://dataportal-api.nordpoolgroup.com/api/Consumption?date={date}&deliveryAreas=EE,LT,LV,DK1,DK2,FI,NO1,NO2,NO3,NO4,NO5,SE1,SE2,SE3,SE4'
    # Make the web request
    resp = requests.get(url_consumption)
    if resp.status_code == 200:  # Successful response
        data = resp.json()
        # Create a DataFrame from the capacities part of the response
        all_data = pd.DataFrame(data['multiAreaEntries'])

        all_data['entryPerArea']=all_data['entryPerArea'].astype(str)

        engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
        all_data.to_sql('all_consumption', engine, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table all_consumption successfully.")
    else:
        log(f"Failed to fetch data HTTP Status(get_consumption): {resp.status_code}")

def get_production(symbols,now=None):
    # Initialize an empty DataFrame for the final result
    all_data = pd.DataFrame()

    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')

    for symbol in symbols:
        url_prod = f'https://dataportal-api.nordpoolgroup.com/api/ProductionData?date={date}&deliveryArea={symbol}'
        resp = requests.get(url_prod)
        try:
            if resp.status_code == 200:  # Successful response
            
                data = resp.json()
                # Create a DataFrame from the capacities part of the response
                symbol_data = pd.DataFrame(data['content'])
                byType_expanded = symbol_data['byType'].apply(pd.Series)
                symbol_data = symbol_data.drop('byType', axis=1).join(byType_expanded)
                # Add additional info
                symbol_data['symbol'] = symbol
                # Concatenate with the main DataFrame
                all_data = pd.concat([all_data, symbol_data], ignore_index=True)

                log(f"Data appended to PostgreSQL table all_production successfully.")
            else:
                log(f"Failed to fetch data HTTP Status(get_production): {resp.status_code}")
        except:
            pass
    engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
    
    all_data.fillna("",inplace=True)
    all_data.to_sql('all_production', engine, if_exists='append', index=False)
def get_balance_data(now=None):
    # Initialize an empty DataFrame for the final result
    all_data = pd.DataFrame()
    yesterday = get_default_datetime(now) - timedelta(days=1)
    date = yesterday.strftime('%Y-%m-%d')

    url_balance = f'https://dataportal-api.nordpoolgroup.com/ManualFrequencyRestorationReserve/multiple?date={date}&deliveryAreas=DK1,DK2,FI,NO1,NO2,NO3,NO4,NO5,SE1,SE2,SE3,SE4'
    # Make the web request
    resp = requests.get(url_balance)
    try:
        if resp.status_code == 200:  # Successful response
            data = resp.json()
            # Create a DataFrame from the capacities part of the response
            all_data = pd.DataFrame(data['contentPerArea'])

            all_data['entryPerArea']=all_data['entryPerArea'].astype(str)

            engine = create_engine('postgresql://tradeapollo:PASSWORD@SERVER_NAME:5432/tradeapollo')
            all_data.to_sql('all_balance_data', engine, if_exists='append', index=False)
            log(f"Data appended to PostgreSQL table all_balance_data successfully.")
        else:
            log(f"Failed to fetch data HTTP Status(get_balance_data): {resp.status_code}")
    except:
        pass



def datetime_range_reverse(start_date, end_date):
    current_date = end_date
    while current_date >= start_date:
        yield current_date
        current_date -= timedelta(days=1)

start_date = datetime(2010, 1, 1)
end_date = datetime.today()

save_to_json()
for now in datetime_range_reverse(start_date, end_date):
    print(now)
    try:
        mibgas_dayahead(now)
    except Exception as e:
        print(f"Error in mibgas_dayahead for {now}: {e}")

    try:
        price_spot_market(now)
    except Exception as e:
        print(f"Error in price_spot_market for {now}: {e}")

    try:
        price_data(now)
    except Exception as e:
        print(f"Error in price_data for {now}: {e}")

    try:
        get_capacity(all_symbols, now)
    except Exception as e:
        print(f"Error in get_capacity for {now}: {e}")

    try:
        get_flow(all_symbols, now)
    except Exception as e:
        print(f"Error in get_flow for {now}: {e}")

    try:
        get_intraday(intraday_symbols, now)
    except Exception as e:
        print(f"Error in get_intraday for {now}: {e}")

    try:
        get_consumption(now)
    except Exception as e:
        print(f"Error in get_consumption for {now}: {e}")

    try:
        get_balance_data(now)
    except Exception as e:
        print(f"Error in get_balance_data for {now}: {e}")

    try:
        dayAheadPrices(now)
    except Exception as e:
        print(f"Error in dayAheadPrices for {now}: {e}")

    try:
        prices(now)
    except Exception as e:
        print(f"Error in prices for {now}: {e}")

    try:
        trade(now)
    except Exception as e:
        print(f"Error in trade for {now}: {e}")

    try:
        trade_Net(now)
    except Exception as e:
        print(f"Error in trade_Net for {now}: {e}")

    try:
        trade_netflows(now)
    except Exception as e:
        print(f"Error in trade_netflows for {now}: {e}")

    try:
        get_production(production_symbols, now)
    except Exception as e:
        print(f"Error in get_production for {now}: {e}")

    try:
        get_price(now)
    except Exception as e:
        print(f"Error in get_price for {now}: {e}")

    try:
        fetch_and_update_volumes(all_symbols, now)
    except Exception as e:
        print(f"Error in fetch_and_update_volumes for {now}: {e}")
