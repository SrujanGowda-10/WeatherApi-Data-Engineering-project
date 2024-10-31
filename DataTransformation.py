import json
import boto3
import botocore
import pandas as pd
from io import StringIO
from datetime import datetime
import time
import urllib.parse

location_map = {
    'New Delhi': 'DEL',
    'Bangalore': 'BAN',
    'Chennai': 'CHE',
    'Pune': 'PUN',
    'Mumbai': 'MUM',
    'Hyderabad': 'HYD',
    'Jaipur': 'JAI',
    'Kochi': 'KOC',
    'Kolkata': 'KOL',
    'Ahmedabad': 'ADB'
}



# Define the columns to select and rename
day_columns_to_select = ['date','hour','day.maxtemp_c','day.avgtemp_c','day.mintemp_c','day.maxwind_kph','day.totalprecip_mm','day.totalsnow_cm',
                     'day.avghumidity','day.daily_will_it_rain','day.daily_chance_of_rain','day.daily_will_it_snow', 'day.daily_chance_of_snow',
                     'day.uv','astro.sunrise','astro.sunset','astro.moonrise','astro.moonset','day.condition.code','day.condition.text']
    
    
day_columns_to_rename = {'date':'forecast_date',
    'day.maxtemp_c':'max_temp_c','day.avgtemp_c':'avg_temp_c','day.mintemp_c':'min_temp_c','day.maxwind_kph':'max_wind_kph','day.totalprecip_mm':'total_precip_mm',
    'day.totalsnow_cm':'total_snow_cm','day.avghumidity':'avg_humidity','day.daily_will_it_rain':'daily_will_it_rain','day.daily_chance_of_rain':'daily_chance_of_rain',
    'day.daily_will_it_snow':'daily_will_it_snow','day.daily_chance_of_snow':'daily_chance_of_snow','day.uv':'uv','astro.sunrise':'sunrise_time',
    'astro.sunset':'sunset_time','astro.moonrise':'moonrise_time','astro.moonset':'moonset_time','day.condition.code':'condition_code',
    'day.condition.text':'condition_text'
}

final_day_columns = [
    'forecast_date','max_temp_c','avg_temp_c','min_temp_c','max_wind_kph','total_precip_mm',
   'total_snow_cm','avg_humidity','daily_will_it_rain','daily_chance_of_rain',
    'daily_will_it_snow','daily_chance_of_snow','uv','sunrise_time',
   'sunset_time','moonrise_time','moonset_time','condition_code'
]

hour_columns_to_rename = {'time':'forecast_datetime','condition.code':'condition_code','condition.text':'condition_text'}

final_hour_columns = [
    'forecast_datetime','condition_code','temp_c','is_day','wind_kph','wind_dir','pressure_mb',
    'precip_mm','humidity','cloud','dewpoint_c','gust_kph','will_it_rain','chance_of_rain','will_it_snow','chance_of_snow','snow_cm','uv'
]



def convert_and_upload_csv_to_bucket_sf(df,Key):
    s3_client = boto3.client('s3')
    bucket_name = 'weather-etl-snowflake-stage-bucket'
    buffer = StringIO()
    df.to_csv(buffer, index=False)
    data = buffer.getvalue()
    s3_client.put_object(
        Bucket=bucket_name,
        Key=Key,
        Body=data
    )
    print(f"Uploaded {Key} to {bucket_name}")


def condition_load(df,condition_data):
    code = df['condition_code'].iloc[0]
    text = df['condition_text'].iloc[0]
    if code not in condition_data.keys():
        condition_data[code] = text

def process_forecast(forecast_df, location_key,fact_forecast_day, fact_forecast_hour,condition_data):
    cols = ['day1','day2']
    for col in cols:
        fc_df = pd.json_normalize(forecast_df[col])
        fc_day_df = fc_df[day_columns_to_select].rename(columns=day_columns_to_rename)
        condition_load(df=fc_day_df,condition_data=condition_data)
        fc_day_df = fc_day_df[final_day_columns]
        fc_day_df['forecast_date'] = pd.to_datetime(fc_day_df['forecast_date']).apply(lambda x: x.date())
        fc_day_dict = fc_day_df.to_dict(orient='records')
        curr_forecast_date_key = ""
        for record in fc_day_dict:
            record['location_id'] = location_key
            # forecast_date_key = f"{location_key}_{record['forecast_date'].replace('-','')}"
            forecast_date_key = f"{location_key}_{record['forecast_date'].strftime('%Y%m%d')}"
            record['forecast_day_weather_id'] = forecast_date_key

        fact_forecast_day.append(fc_day_dict)
        
        
        # Process hours for this day
        fc_hrs_df = pd.json_normalize(fc_df['hour'])
        fc_req_hrs = fc_hrs_df[[0,10,20]]
        for hour_idx in [0, 10, 20]:
            fc_req_hr = pd.json_normalize(fc_req_hrs[hour_idx])
            fc_hr_df = fc_req_hr.rename(columns=hour_columns_to_rename)
            condition_load(df=fc_hr_df,condition_data=condition_data)
            fc_hr_df = fc_hr_df[final_hour_columns]
            fc_hr_df['forecast_datetime'] = pd.to_datetime(fc_hr_df['forecast_datetime'])
            fc_hr_dict = fc_hr_df.to_dict(orient='records')

            for record in fc_hr_dict:
                record['location_id'] = location_key
                # fc_date = record['forecast_datetime'].split(' ')[0].replace('-','')
                fc_date = record['forecast_datetime'].strftime('%Y%m%d')
                forecast_date_key = f"{location_key}_{fc_date}"
                record['forecast_day_weather_id'] = forecast_date_key
                record['forecast_hour_weather_id'] = f"{forecast_date_key}_{hour_idx}"
            # print(fc_hr_dict)
            fact_forecast_hour.append(fc_hr_dict)

    
def file_exist(source_bucket, source_key):
    s3 = boto3.client('s3')
    try:
        s3.head_object(Bucket=source_bucket, Key=source_key)
        return True  # Object exists
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False  # Object not found


def lambda_handler(event, context):
    print("start")
    # TODO implement
    bucket_name = 'weather-etl-raw-bucket'
    api_data_key_path = 'api_data_stage/'
    s3_client = boto3.client('s3')
    
    source_bucket = bucket_name
    destination_bucket = 'weather-bucket-history'
    
    fact_forecast_day = []
    fact_forecast_hour = []
    condition_data = {}
    
    timestamp = datetime.now()
    file_timestamp = timestamp.strftime("%Y%m%d")
    source_key = event['Records'][0]['s3']['object']['key']
    decoded_key = urllib.parse.unquote_plus(source_key)
    if file_exist(source_bucket=source_bucket, source_key=decoded_key):
        print("src key exist")
        response = s3_client.get_object(Bucket=bucket_name, Key=decoded_key)
        object_data = response['Body'].read()
        data = json.loads(object_data)
        
        df = pd.json_normalize(data)
        
        #location
        location_key = location_map.get(df['location.name'].iloc[0])
        loc_path_key_path = f"location/{df['location.name'].iloc[0]}.csv"
        location_data = {
            'location_id':location_key,
            'name':df['location.name'].iloc[0],
            'region':df['location.region'].iloc[0],
            'country':df['location.country'].iloc[0],
            'latitude':df['location.lat'].iloc[0],
            'longitude':df['location.lon'].iloc[0]
        }
        location_df = pd.DataFrame([location_data])
        convert_and_upload_csv_to_bucket_sf(df=location_df,Key=loc_path_key_path)
        
        
        #current day
        last_updated = pd.to_datetime(df['current.last_updated'].iloc[0])  # Convert to datetime
        current_weather_key = last_updated.strftime("%Y%m%d")  # Format the date
        current_weather_id = f"{location_key}_{current_weather_key}"
        
        #condition condition from current day weather
        code = df['current.condition.code'].iloc[0]
        text = df['current.condition.text'].iloc[0]
        if code not in condition_data.keys():
            condition_data[code] = text
        
        
        current_weather_data = {
            'current_weather_id':current_weather_id,
            'location_id':location_key,
            'condition_code':code,
            'temperature_c':df['current.temp_c'].iloc[0],
            'is_day':df['current.is_day'].iloc[0],
            'wind_kph':df['current.wind_kph'].iloc[0],
            'wind_dir':df['current.wind_dir'].iloc[0],
            'pressure_mb':df['current.pressure_mb'].iloc[0],
            'precip_mm':df['current.precip_mm'].iloc[0],
            'humidity':df['current.cloud'].iloc[0],
            'cloud':df['current.cloud'].iloc[0],
            'dewpoint_c':df['current.dewpoint_c'].iloc[0],
            'gust_kph':df['current.gust_kph'].iloc[0],
            'weather_date':pd.to_datetime(df['current.last_updated'].iloc[0]).date()
        }
        curr_weather_key = f"current_weather/{location_key}_current_weather_{file_timestamp}.csv"
        current_weather_df = pd.DataFrame([current_weather_data])
        
        convert_and_upload_csv_to_bucket_sf(df=current_weather_df,Key=curr_weather_key)
        
        
        #forecast weather data
        forecast_df = pd.json_normalize(df['forecast.forecastday'])
        
        forecast_days = forecast_df[[1,2]]
        forecast_days = forecast_days.rename(columns = {1:'day1',2:'day2'})
        
        process_forecast(
            forecast_df=forecast_days,
            location_key=location_key,
            fact_forecast_day=fact_forecast_day,
            fact_forecast_hour=fact_forecast_hour,
            condition_data=condition_data
        )
        
        
        fcd_key = f"forecast_day_weather/{location_key}_forecast_day_{file_timestamp}.csv"
        flat_fact_forecast_day = [day for day_list in fact_forecast_day for day in day_list]
        forecast_day_df = pd.DataFrame(flat_fact_forecast_day)
        # print(forecast_day_df)
        convert_and_upload_csv_to_bucket_sf(df=forecast_day_df,Key=fcd_key)
        
        
        fch_key = f"forecast_hour_weather/{location_key}_forecast_hour_{file_timestamp}.csv"
        flat_fact_forecast_hour = [hour for hour_list in fact_forecast_hour for hour in hour_list]
        forecast_hour_df = pd.DataFrame(flat_fact_forecast_hour)
        convert_and_upload_csv_to_bucket_sf(df=forecast_hour_df,Key=fch_key)
        
        # weather conditon of the city location_key
        condition_df = pd.DataFrame(list(condition_data.items()), columns=['condition_code', 'condition_name'])
        cond_key = f"condition/{location_key}_condition_{file_timestamp}.csv"
        convert_and_upload_csv_to_bucket_sf(df=condition_df,Key=cond_key)
        
        # copy to history bucket
        filename = urllib.parse.unquote_plus(source_key.split('/')[-1])
        destination_key = f'api_data_history/{filename}'
        s3_client.copy_object(
            Bucket=source_bucket,
            CopySource={'Bucket':source_bucket,'Key':decoded_key},
            Key=destination_key
        )
        
        # delete from source bucket
        s3_client.delete_object(
            Bucket=source_bucket,
            Key=decoded_key
        )
        print(f"{source_bucket}/{decoded_key} moved to {source_bucket}/{destination_key}")
        
    

    