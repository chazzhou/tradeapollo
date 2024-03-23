# TradeApollo Electricity Price and Carbon Intensity API

This API provides endpoints to retrieve electricity price data and carbon intensity information for various countries and regions.

## Endpoints

### 1. Get Electricity Prices

- **URL:** `/electricity`
- **Method:** GET
- **Query Parameters:**
  - `country` (required): The country for which to retrieve electricity prices (e.g., "Italy", "Germany").
  - `zipcode` (required): The ZIP code of the location for which to retrieve electricity prices.
  - `kw_total` (required): The total kilowatt usage for which to retrieve electricity prices.
- **Response:**
  - Success:
    - Status Code: 200
    - Body: JSON array containing electricity price data, including name and tariff, description, price, and image link.
  - Error:
    - Status Code: 404 (No data found for the given parameters)
    - Status Code: 400 (Missing required parameters)
    - Status Code: 500 (Internal server error)

### 2. Get Historical Price Data

- **URL:** `/price_data`
- **Method:** GET
- **Query Parameters:**
  - `date` (required): The date for which to retrieve historical price data (format: "YYYY-MM-DD").
  - `country_bidding_zone` (required): The country bidding zone code for which to retrieve historical price data.
- **Response:**
  - Success:
    - Status Code: 200
    - Body: JSON array containing historical price data, including time (unix seconds), price, and unit.
  - Error:
    - Status Code: 500 (Internal server error)

## Usage

1. Make a GET request to the desired endpoint with the required query parameters.
2. Parse the JSON response to obtain the electricity price data or historical price data.

## Examples

### Get Electricity Prices

```bash
curl -X GET "http://localhost:80/electricity?country=Italy&zipcode=12345&kw_total=1000"
```

### Get Historical Price Data

```bash
curl -X GET "http://localhost:80/price_data?date=2023-05-22&country_bidding_zone=IT-North"
```

## Dependencies

- Flask: Web framework for building the API endpoints.
- Flask-CORS: Extension for handling Cross-Origin Resource Sharing (CORS).
- pandas: Data manipulation library for handling electricity price data.
- requests: Library for making HTTP requests to scrape electricity price data.
- BeautifulSoup: Library for parsing HTML content during web scraping.
- geopy: Library for geocoding and reverse geocoding.
- psycopg2: PostgreSQL database adapter for Python.

## Configuration

The API requires the following configuration:

- PostgreSQL database connection details:
  - Host: "HOST_NAME"
  - Port: "5432"
  - Database Name: "tradeapollo"
  - Username: "tradeapollo"
  - Password: "PASSWORD"

Make sure to update the `db_config` dictionary in the code with the appropriate database connection details.

## Error Handling

The API handles various error scenarios and returns appropriate error responses:

- If no data is found for the given parameters, a 404 status code is returned.
- If required parameters are missing, a 400 status code is returned.
- If an internal server error occurs, a 500 status code is returned.

## Notes

- The API uses web scraping techniques to retrieve electricity price data from external websites. The scraping logic is specific to the websites being scraped and may need to be updated if the website structure changes.
- The API uses a PostgreSQL database to store and retrieve historical price data. Make sure the database is set up and accessible with the provided connection details.
- The API uses the Nominatim geocoding service from geopy to retrieve city names based on ZIP codes. Make sure the geocoding service is available and the API key (if required) is properly configured.