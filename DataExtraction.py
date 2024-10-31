import json
import boto3
import requests
from datetime import datetime
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "weather-api-key"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return secret
    

#get the weather data for cities
def fetch_weather(city,api_key,forecast_days):
    url =url = f"http://api.weatherapi.com/v1/forecast.json?key={api_key}&q={city}&days={forecast_days}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"{city}: Error - {str(e)}")
        return None


def lambda_handler(event, context):
    # to convert str format to dict format
    secret = json.loads(get_secret())
    api_key = secret['api_key']

    cities = ["New Delhi", "Bangalore", "Pune", "Mumbai", "Chennai", "Hyderabad", "Jaipur", "Kochi","Kolkata", "Ahmedabad"]
    forecast_days = 3
    
    s3_client = boto3.client('s3')
    bucket = 'weather-etl-raw-bucket'
    folder = 'api_data_stage/'
    
    timestamp = datetime.now()
    file_timestamp = timestamp.strftime("%Y%m%d")
    
    for city in cities:
        city_data = fetch_weather(city=city,api_key=api_key,forecast_days=forecast_days)
        if city_data is not None:
            file_name = f"{city}_{file_timestamp}.json"
            to_path = f"{folder}{file_name}"
            
            try:
                s3_client.put_object(
                    Body=json.dumps(city_data),
                    Bucket=bucket,
                    Key=to_path
                    )
                print(f"Successfully uploaded {file_name} to s3://{bucket}/{to_path}")
                
            except ClientError as e:
                print(f"ClientError: Failed to upload {city} data to S3: {e}")
                
        else:
            print(f"Skipping upload for {city} due to previous error in fetching data.")
            
    

    