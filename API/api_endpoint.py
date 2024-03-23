from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
from geopy.geocoders import Nominatim
import time
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from datetime import datetime
import psycopg2

app = Flask(__name__)
CORS(app)

# PostgreSQL connection details
db_config = {
    "host": "HOSTNAME",
    "port": "5432",
    "dbname": "tradeapollo",
    "user": "tradeapollo",
    "password": "PASSWORD",
}

def get_id_from_zipcode(zipcode):
    url = f'https://www.stromauskunft.de/kundenportal/?ajax=getLocations&productType=electricity&zipcode={zipcode}&value='
    response = requests.get(url)
    try:
        response_list = json.loads(response.text)
        return response_list
    except:
        return None

def scrape_germany_electricity_by_zipcode(zipcode, kw_total, whole_city=True, id_=0, contract_duration=12, contract_extension=12, max_tariffs=2,
                                          cancellation_period='6_week', eco_product_type=0, price_guarantee=0):
    if whole_city:
        location_id = get_id_from_zipcode(zipcode)
        if location_id:
            id_ = location_id[0]['id']
        else:
            return None

    url = f"https://www.stromauskunft.de/stromrechner-ergebnisse?newcalc=1&subsettingsIncluded=1&calcButton=&profile=h0&calcSubmit=Berechnen&checkout=production&annualTotal={kw_total}&plz={zipcode}&locationID={id_}&bonus=compliant&yearlyPrices=0&priceGuarantee={price_guarantee}&maxContractDuration={contract_duration}&cancellationPeriodAmount={cancellation_period}&maxContractProlongation={contract_extension}&maxTariffsPerProvider={max_tariffs}&ecoProductType={eco_product_type}&sortBy=TotalCosts&sortDirection=Ascending&currentPaymentForm=monthly&currentAmount="
    resp = requests.get(url)
    soup = BeautifulSoup(resp.text, 'html.parser')
    all_values = []

    rows = soup.select('.row')
    for row in rows:
        name_tariff = row.select_one('.col-md-3.logo > p:nth-child(2)')
        if name_tariff:
            name_tariff = name_tariff.text.replace('Tarif', ' Tarif').strip()

        description = row.select_one('.col-md-4.info')
        if description:
            description = description.text.strip()

        price = row.select_one('.col-md-3.preis > p > strong > span.preis')
        if price:
            price = price.text.replace('i', '').strip()

        img_link = row.select_one('.col-md-3.logo > a > img')
        if img_link:
            img_link = img_link['src']

        if name_tariff:
            row_dict = {
                'name_and_tariff': name_tariff,
                'description': description,
                'price': price,
                'img_link': img_link
            }
            all_values.append(row_dict)

    if all_values:
        big_df = pd.DataFrame(all_values)
        big_df.dropna(subset=['name_and_tariff'], inplace=True)
        
        # Convert DataFrame to a list of dictionaries
        data = big_df.to_dict(orient='records')

        # Remove duplicate items based on 'name_and_tariff' and 'price'
        unique_data = []
        seen = set()
        for item in data:
            key = (item['name_and_tariff'], item['price'])
            if key not in seen:
                seen.add(key)
                unique_data.append(item)

        return pd.DataFrame(unique_data)
    else:
        return None

def get_city_name_from_zip(zip_code, max_retries=2, retry_delay=1):
    geolocator = Nominatim(user_agent="TradeApollo Dashboard")
    
    retry_count = 0
    while retry_count < max_retries:
        try:
            location = geolocator.geocode(zip_code)
            
            if location:
                return location.address, 200
            else:
                return "City not found for the given ZIP code.", 404
        
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            retry_count += 1
            if retry_count < max_retries:
                time.sleep(retry_delay)  # Add a delay before retrying
            else:
                return f"Failed to retrieve city name after {max_retries} retries. Reason: {str(e)}",  500

def scrape_italy_electricity_by_zipcode(zipcode, kw_total):
    number_float = int(kw_total) / 1000
    kw_total = f"{number_float:.3f}"
    power_of_system = 3

    city_name, city_name_status = get_city_name_from_zip(zipcode)
    if city_name_status != 200:
        return jsonify({'error': city_name}), city_name_status
    
    city = city_name.split(',')[2].strip()

    url = f'https://tariffe.segugio.it/helper/ricercacomune/{city}'
    response = requests.get(url)
    city_data = json.loads(response.text)
    if city_data:
        city_code = city_data[0]['code']
    else:
        return None

    url = 'https://tariffe.segugio.it/costo-energia-elettrica/ricerca-offerte-energia-elettrica.aspx'
    form_data = {
        'IsRicercaCompleta': 'true',
        'TipoCliente': '5',
        'TipoPrezzo': '0',
        'CABComune': f'{city_code}',
        'PotenzaImpianto': f'{power_of_system}',
        'ConsumoAnnuo': kw_total,
        'ContatoreLuceAttivo': '1',
        'AbitazioneResidenza': '1',
        'UtilizzoEnergia': '1',
        'PercGiorno': '44',
        'PercSera': '33',
        'PercWeekend': '23',
        'TipoPagamento': '2',
        'TipoInvioBolletta': '1',
        'isMobile': 'false',
        'IsSubmit': 'true',
        'isDeduplicaRichiesta': 'false'
    }

    response = requests.post(url, data=form_data)
    soup = BeautifulSoup(response.text, 'html.parser')
    rows = soup.select('.offert-item.vantaggi')
    all_values = []

    for row in rows:
        name = row.select_one('.society-name')
        if name:
            name = name.text.strip()

        description = row.select_one('.hyd-hide-desktop.browser-default.prices-list')
        if description:
            description = description.text.replace('\n', '').strip()

        img_link = row.select_one('.logo-border > img')
        if img_link:
            img_link = img_link['data-src'].replace('//', 'https://')

        price = row.select_one('.main-price')
        if price:
            price = price.text.strip()

        if name:
            row_dict = {
                'name_and_tariff': name,
                'description': description,
                'price': price,
                'img_link': img_link
            }
            all_values.append(row_dict)

    if all_values:
        big_df = pd.DataFrame(all_values)
        big_df.dropna(subset=['name_and_tariff'], inplace=True)
        return big_df
    else:
        return None

def get_electricity_normal_people(country, zipcode, kw_total):
    if country == 'Italy':
        df = scrape_italy_electricity_by_zipcode(zipcode, kw_total)
    elif country == 'Germany':
        df = scrape_germany_electricity_by_zipcode(zipcode, kw_total)
    else:
        return None

    return df

@app.route('/electricity', methods=['GET'])
def get_electricity():
    country = request.args.get('country')
    zipcode = request.args.get('zipcode')
    kw_total = request.args.get('kw_total')

    if country and zipcode and kw_total:
        try:
            df = get_electricity_normal_people(country, zipcode, int(kw_total))
            if df is not None:
                result = df.to_dict(orient='records')
                return jsonify(result)
            else:
                return jsonify({'error': 'No data found for the given parameters'}), 404
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    else:
        return jsonify({'error': 'Missing required parameters'}), 400

@app.route("/price_data", methods=["GET"])
def get_price_data():
    # Get the date and country bidding zone code from the request parameters
    date_str = request.args.get("date")
    country_bidding_zone = request.args.get("country_bidding_zone")
    
    if country_bidding_zone == "DE" or country_bidding_zone == "LU":
        country_bidding_zone = "DE-LU"
    elif country_bidding_zone == "IT-NO":
        country_bidding_zone = "IT-North"
    elif country_bidding_zone == "IT-SO":
        country_bidding_zone = "IT-South"
    elif country_bidding_zone == "IT-CSO":
        country_bidding_zone = "IT-Centre-South"
    elif country_bidding_zone == "IT-CNO":
        country_bidding_zone = "IT-Centre-North"
    elif country_bidding_zone == "IT-SIC":
        country_bidding_zone = "IT-Sicily"
    elif country_bidding_zone == "IT-SAR":
        country_bidding_zone = "IT-Sardinia"
    # If country_bidding_zone has a "-" in it, use the second part of the string
    elif "-" in country_bidding_zone:
        country_bidding_zone = country_bidding_zone.split("-")[1]

    # Convert the date string to a datetime object
    date = datetime.strptime(date_str, "%Y-%m-%d")

    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Execute the SQL query to fetch the price data for the given date and country bidding zone
        query = """
            SELECT unix_seconds AS time, price, unit
            FROM public.price_data
            WHERE start::timestamp >= %s AND start::timestamp < %s + INTERVAL '1 day' AND "country bidding zones" = %s
            """
        cur.execute(query, (date, date, country_bidding_zone))

        # Fetch all the rows from the result
        rows = cur.fetchall()

        # Convert the result to a list of dictionaries
        price_data = []
        for row in rows:
            price_data.append({"time": row[0], "price": float(row[1]), "unit": row[2]})

        # Return the price data as JSON response
        return jsonify(price_data)

    except Exception as e:
        # Handle any errors and return an error response
        return jsonify({"error": str(e)}), 500

    finally:
        # Close the database connection
        cur.close()
        conn.close()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)