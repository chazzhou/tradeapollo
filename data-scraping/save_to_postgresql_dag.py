import pandas as pd
import requests
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from collections import defaultdict
import json

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
from contextlib import contextmanager

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}
country_codes = [
    'AT', 'BE', 'BG', 'CH', 'CZ', 'DK1', 'DK2', 'EE', 'ES', 'FI', 'FR', 'GR', 'HR',
    'HU', 'IT-Calabria', 'IT-Centre-North', 'IT-Centre-South', 'IT-North', 'IT-SACOAC',
    'IT-SACODC', 'IT-Sardinia', 'IT-Sicily', 'IT-South', 'LT', 'LV','NL',
    'NO1', 'NO2', 'NO2NSL', 'NO3', 'NO4', 'NO5', 'PL', 'PT', 'RO', 'RS', 'SE1', 'SE2',
    'SE3', 'SE4', 'SI', 'SK'
    ]

@contextmanager
def get_db_connection():
    postgres_hook = PostgresHook(postgres_conn_id='scraped_data')
    engine = postgres_hook.get_sqlalchemy_engine()
    connection = engine.connect()
    try:
        yield connection
    finally:
        connection.close()

def get_yesterday_date_range():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    start_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + '+01:00'
    end_date = today.replace(hour=0, minute=0, second=0, microsecond=0).isoformat() + '+01:00'
    return start_date, end_date

def log(message):
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_file_path = f"log_{current_date}.txt"

    with open(log_file_path, "a") as log_file:
        log_file.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {message}\n")


def price_data():
    # API endpoint and parameters
    api_url = 'https://api.energy-charts.info/price'
    start_date, end_date = get_yesterday_date_range()

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

            # Save DataFrame to PostgreSQL
            table_name = 'price_data_EnergyCharts'
            with get_db_connection() as connection:
                # Try to append data to the existing table, create if not exists
                try:
                    df.to_sql(table_name, connection, if_exists='append', index=False)
                    log(f"Data appended to PostgreSQL table '{table_name}' | country: '{country}' successfully.")
                except Exception as e:
                    log(f"Error: Unable to append data to table '{table_name}' | country: '{country}'. Error message: {str(e)}")

        else:
            # Log the error if the API request fails
            log(f"Error: Unable to fetch data from API. Status code: {response.status_code}")

def price_spot_market():
    api_url = 'https://api.energy-charts.info/price_spot_market'
    start_date, end_date = get_yesterday_date_range()

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

            df['xAxisValues (Unix timestamp)'] = pd.to_datetime(df['xAxisValues (Unix timestamp)'], unit='s')
            df['country bidding zones'] = country
            df.columns=['xAxisValues (Unix timestamp)','Day Ahead Auction','country bidding zones']
            
            table_name = 'price_spot_market_EnergyCharts'
            with get_db_connection() as connection:
                try:
                    df.to_sql(table_name, connection, if_exists='append', index=False)
                    log(f"Data appended to PostgreSQL table '{table_name}' | country: '{country}' successfully.")
                except Exception as e:
                    log(f"Error: Unable to append data to table '{table_name}' | country: '{country}'. Error message: {str(e)}")

        else:
            log(f"Error: Unable to fetch data from API. Status code: {response.status_code}")


def save_to_json(cookies={'emfip-welcome': 'true'}):
    yesterday = datetime.now() - timedelta(days=1)
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
    with open("/opt/airflow/dags/result.json", "w") as json_file:
        json.dump(my_defaultdict, json_file)



def dayAheadPrices():
    # Assuming my_defaultdict is defined elsewhere in your code
    # Replace it with your actual data structure
    with open("/opt/airflow/dags/result.json", "r") as json_file:
        data = json.load(json_file)

    my_defaultdict=defaultdict(list, data)

    yesterday = datetime.now() - timedelta(days=1)
    yesterday_date = yesterday.strftime("%d.%m.%Y")

    for i in my_defaultdict:
        for j in my_defaultdict[i]:
            url = "https://transparency.entsoe.eu/transmission-domain/r2/dayAheadPrices/show?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime={}|CET|DAY&biddingZone.values={}&resolution.values=PT60M&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC+1)+/+CEST+(UTC+2)".format(yesterday_date, j[0])

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
                df = tables[0]  # Assuming the desired table is the first one
                table_name = 'dayAheadPrices_ENTSOE'
                df.columns=['MTU','Day-ahead Price EUR / MWh']

                df.replace('n/e', None, inplace=True)
                with get_db_connection() as connection:
                    # Try to append data to the existing table, create if not exists
                    try:
                        df.to_sql(table_name, connection, if_exists='append', index=False)
                        log(f"Data appended to PostgreSQL table '{table_name}' | country '{i}':'{j[0]}' successfully.")
                    except Exception as e:
                        log(f"Error: Unable to append data to table '{table_name}' | country '{i}':'{j[0]}'. Error message: {str(e)}")

            else:
                # Log the error if the request fails
                log(f"Error: Failed to retrieve the webpage. Status code: {response.status_code}")


def prices():
    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)
    start_date_str = yesterday_date.strftime("%Y-%-m-%-d")
    end_date_str = today_date.strftime("%Y-%-m-%-d")
    # Make the initial request to get the list of countries
    initial_url = f"https://api.iea.org/rte/list/countries?from={start_date_str}&to={end_date_str}&type=price"
    response_countries = requests.get(initial_url)
    countries_data = response_countries.json()

    # Parse and print the ISO3 values
    iso3_values = [country["ISO3"] for country in countries_data]

    all_results = []

    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)



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

    # Save DataFrame to PostgreSQL
    table_name = 'prices_IEA'
    with get_db_connection() as connection:
        df.to_sql(table_name, connection, if_exists='append', index=False)
    log(f"Data appended to PostgreSQL table '{table_name}' successfully.")


def trade():
    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)

    start_date_str = yesterday_date.strftime("%Y-%-m-%-d")
    end_date_str = today_date.strftime("%Y-%-m-%-d")
    # Make the initial request to get the list of countries
    initial_url = f"https://api.iea.org/rte/list/countries?from={start_date_str}&to={end_date_str}&type=trade"
    response_countries = requests.get(initial_url)
    countries_data = response_countries.json()

    # Parse and print the ISO3 values
    iso3_values = [country["ISO3"] for country in countries_data]

    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)

    start_date_str = yesterday_date.strftime("%Y-%-m-%-d")
    end_date_str = today_date.strftime("%Y-%-m-%-d")

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

    # Save DataFrame to PostgreSQL
    table_name = 'trade_IEA'

    with get_db_connection() as connection:
        df.to_sql(table_name, connection, if_exists='append', index=False)
    log(f"Data appended to PostgreSQL table '{table_name}' successfully.")


def trade_Net():
    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)
    start_date_str = yesterday_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")
    country_url=f"https://api.iea.org/rte/trade?from={start_date_str}&to={end_date_str}"
    response_country = requests.get(country_url)
    country_data = response_country.json()

    df_list=[]
    for result in country_data:
        df = pd.DataFrame([result])
        df_list.append(df)

    df_list = pd.concat(df_list, ignore_index=True)
    # Save DataFrame to PostgreSQL
    table_name = 'trade_Net_IEA'

    with get_db_connection() as connection:
        df_list.to_sql(table_name, connection, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table '{table_name}' successfully.")

trade_Net()

def trade_netflows():
    today_date = datetime.now().date()
    yesterday_date = today_date - timedelta(days=1)

    start_date_str = yesterday_date.strftime("%Y-%m-%d")
    end_date_str = today_date.strftime("%Y-%m-%d")
    country_url=f"https://api.iea.org/rte/trade/netflows?from={start_date_str}&to={end_date_str}"
    response_country = requests.get(country_url)
    country_data = response_country.json()

    df_list=[]
    for result in country_data:
        df = pd.DataFrame([result])
        df_list.append(df)

    df_list = pd.concat(df_list, ignore_index=True)
        
    # Save DataFrame to PostgreSQL
    table_name = 'trade_netflows_IEA'

    with get_db_connection() as connection:
        df.to_sql(table_name, connection, if_exists='append', index=False)
        log(f"Data appended to PostgreSQL table '{table_name}' successfully.")


# Define default_args for the DAG
default_args = {
    'owner': 'Hachem Sfar',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'save_to_postgresql_dag',
    default_args=default_args,
    description='DAG for saving data to PostgreSQL',
    schedule_interval=timedelta(days=1),
)

# Define the functions to be executed
functions = [price_data, price_spot_market, dayAheadPrices,
             prices, trade, trade_Net, trade_netflows]

# Define a dummy task to trigger all functions in parallel
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Define a dummy task for successful completion
success_task = DummyOperator(
    task_id='success',
    dag=dag,
)

# Define a dummy task for failure
failure_task = DummyOperator(
    task_id='failure',
    dag=dag,
)

# Define email notification task
email_task = EmailOperator(
    task_id='send_email',
    to='hachem.sfar@supcom.tn',
    subject='Airflow DAG Execution Status',
    html_content='<p>All tasks have been successfully executed.</p>',
    dag=dag,
)

# Set up dependencies and parallel execution
start_task >> success_task
start_task >> failure_task

for i, function in enumerate(functions):
    task_id = f'task_{i}'
    execute_function = PythonOperator(
        task_id=task_id,
        python_callable=function,
        dag=dag,
    )
    execute_function >> success_task
    execute_function >> failure_task

# Set up email notification after all tasks are complete
[success_task, failure_task] >> email_task
