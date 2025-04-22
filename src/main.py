import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision, BucketsApi
from influxdb_client.client.write_api import WriteOptions
import os
from datetime import datetime
from bs4 import BeautifulSoup

# Function to get the gas price from GasBuddy API
def getGasPriceFromAPI(station_id):
    url = "https://www.gasbuddy.com/graphql"
    headers = {
        'content-type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Origin': 'https://www.gasbuddy.com',
        'Referer': 'https://www.gasbuddy.com/',
    }
    data = {
        "operationName": "GetStation",
        "variables": {
            "id": station_id
        },
        "query": "query GetStation($id: ID!) { station(id: $id) { prices { credit { nickname postedTime price } } } }"
    }

    with requests.Session() as session:
        try:
            response = session.post(url, headers=headers, json=data, timeout=10)
            if response.status_code == 200:
                json_response = response.json()
                try:
                    prices = json_response['data']['station']['prices']
                    if prices and prices[0]['credit']:
                        price = prices[0]['credit']['price']
                        return float(price)
                except KeyError:
                    pass
        except (requests.exceptions.RequestException, ValueError) as e:
            print(f"API request failed for station {station_id}: {e}")
    return None

# Function to scrape gas price from GasBuddy website
def getGasPriceFromWebsite(station_id):
    url = f"https://www.gasbuddy.com/station/{station_id}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            price_span = soup.find('span', class_='FuelTypePriceDisplay-module__price___3iizb')
            if price_span:
                price_text = price_span.text.strip().replace('$', '')
                return float(price_text)
    except (requests.exceptions.RequestException, ValueError, AttributeError) as e:
        print(f"Web scraping failed for station {station_id}: {e}")
    return None

# Combined function to try API first, then fall back to scraping
def getGasPrice(station_id):
    # Try API first
    price = getGasPriceFromAPI(station_id)
    if price is not None:
        return price
    
    # If API fails, try scraping
    print(f"Falling back to website scraping for station {station_id}")
    return getGasPriceFromWebsite(station_id)

# Check if the bucket exists, create it if it doesn't
def ensureBucketExists(client, bucketName, org):
    bucketsApi = client.buckets_api()

    buckets = bucketsApi.find_buckets().buckets
    for bucket in buckets:
        if bucket.name == bucketName:
            print(f"Bucket '{bucketName}' already exists.")
            return

    bucketsApi.create_bucket(bucket_name=bucketName, org=org, retention_rules=None)
    print(f"Bucket '{bucketName}' created.")

# Save the gas price to InfluxDB
def saveToInfluxDB(price, client, bucket, org, source):
    writeApi = client.write_api(write_options=WriteOptions(batch_size=1))

    try:
        point = (
            Point("gas_price")
            .tag("source", source)
            .field("price", price)
            .time(datetime.utcnow(), WritePrecision.NS)
        )
        writeApi.write(bucket=bucket, org=org, record=point)
        print(f"Price {price} from {source} written to InfluxDB.")
    finally:
        writeApi.close()

# Main function
def main():
    token = os.getenv('INFLUXDB_TOKEN')
    org = os.getenv('INFLUXDB_ORG')
    url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    bucket = os.getenv('INFLUXDB_BUCKET', 'gas_prices')
    
    station_ids = os.getenv('STATION_IDS', '').split(',')
    station_names = os.getenv('STATION_NAMES', '').split(',')

    if not station_ids or not station_names or len(station_ids) != len(station_names):
        raise ValueError("STATION_IDS and STATION_NAMES environment variables are required and must be of the same length.")

    client = InfluxDBClient(url=url, token=token, org=org)

    ensureBucketExists(client, bucket, org)

    # Iterate over the station IDs and names
    for station_id, station_name in zip(station_ids, station_names):
        gasPrice = getGasPrice(station_id)

        if gasPrice is not None:
            print(f"Extracted price for {station_name}: {gasPrice}")
            saveToInfluxDB(gasPrice, client, bucket, org, station_name)
        else:
            print(f"Failed to extract price for {station_name} from both API and website.")

    client.close()

if __name__ == "__main__":
    main()