import requests
from influxdb_client import InfluxDBClient, Point, WritePrecision, BucketsApi
from influxdb_client.client.write_api import WriteOptions
import os
from datetime import datetime

# Function to get the gas price from GasBuddy
def getGasPrice(station_id):
    url = "https://www.gasbuddy.com/graphql"
    headers = {
        'content-type': 'application/json',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',  # Mimic a browser
        'Origin': 'https://www.gasbuddy.com',  # Add Origin header
        'Referer': 'https://www.gasbuddy.com/',  # Add Referer header
    }
    data = {
        "operationName": "GetStation",
        "variables": {
            "id": station_id  # Use the provided station ID
        },
        "query": "query GetStation($id: ID!) { station(id: $id) { prices { credit { nickname postedTime price } } } }"
    }

    # Use a session to handle cookies
    with requests.Session() as session:
        response = session.post(url, headers=headers, json=data)

        if response.status_code == 200:
            json_response = response.json()
            # Safely extract the price from the response
            try:
                prices = json_response['data']['station']['prices']
                if prices and prices[0]['credit']:
                    price = prices[0]['credit']['price']
                    return float(price)
            except KeyError:
                pass  # Ignore errors if the data structure is unexpected
    return None

# Check if the bucket exists, create it if it doesn't
def ensureBucketExists(client, bucketName, org):
    bucketsApi = client.buckets_api()

    # Check if the bucket exists
    buckets = bucketsApi.find_buckets().buckets
    for bucket in buckets:
        if bucket.name == bucketName:
            print(f"Bucket '{bucketName}' already exists.")
            return

    # Create the bucket if it doesn't exist
    retentionRules = None  # Set custom retention rules if needed
    bucketsApi.create_bucket(bucket_name=bucketName, org=org, retention_rules=retentionRules)
    print(f"Bucket '{bucketName}' created.")

# Save the gas price to InfluxDB
def saveToInfluxDB(price, client, bucket, org, source):
    # Initialize the write API with WriteOptions
    writeApi = client.write_api(write_options=WriteOptions(batch_size=1))

    try:
        # Prepare data point to write
        point = (
            Point("gas_price")
            .tag("source", source)  # Tag with the source of the price
            .field("price", price)
            .time(datetime.utcnow(), WritePrecision.NS)  # Add the timestamp
        )

        # Write the data point to the specified bucket
        writeApi.write(bucket=bucket, org=org, record=point)
        print(f"Price {price} from {source} written to InfluxDB.")
    finally:
        # Ensure the writeApi is properly closed
        writeApi.close()

# Main function
def main():
    # InfluxDB configuration (replace with your InfluxDB details)
    token = os.getenv('INFLUXDB_TOKEN')
    org = os.getenv('INFLUXDB_ORG')
    url = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
    bucket = os.getenv('INFLUXDB_BUCKET', 'gas_prices')
    station_id = os.getenv('STATION_ID')  # Get the station ID from environment variables

    if not station_id:
        raise ValueError("STATION_ID environment variable is required.")

    # Initialize the InfluxDB client
    client = InfluxDBClient(url=url, token=token, org=org)

    # Ensure the bucket exists, or create it
    ensureBucketExists(client, bucket, org)

    # Get gas price from GasBuddy
    gasPrice = getGasPrice(station_id)

    # Write the gas price to InfluxDB if available
    if gasPrice:
        print(f"Extracted GasBuddy price: {gasPrice}")
        saveToInfluxDB(gasPrice, client, bucket, org, "Roseville-Costco")
    else:
        print("Failed to extract the GasBuddy price.")

    # Close the InfluxDB client
    client.close()

if __name__ == "__main__":
    main()