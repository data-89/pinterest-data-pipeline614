import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text


random.seed(100)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                payload_pin = json.dumps({
                "records": [
                    {
                    "value": {"index": pin_result["index"], "unique_id": pin_result["unique_id"], "title": pin_result["title"], "description": pin_result["description"], "poster_name": pin_result["poster_name"], "follower_count": pin_result["follower_count"], "tag_list": pin_result["tag_list"], "is_image_or_video": pin_result["is_image_or_video"], "image_src": pin_result["image_src"], "downloaded": pin_result["downloaded"], "save_location": pin_result["save_location"], "category": pin_result["category"]}
                    }
                    ]
                })
                print(pin_result)

                invoke_url_pin = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/topics/12951463f185.pin"
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response_pin = requests.request("POST", invoke_url_pin, headers=headers, data=payload_pin)
                print(response_pin.status_code)

                # Set up Kinesis stream
                invoke_url_stream_pin = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12951463f185-pin/record"
                headers_stream = {'Content-Type': 'application/json'}
                response_stream_pin = requests.request("PUT", invoke_url_stream_pin, headers=headers_stream, data=payload_pin)
                print(response_stream_pin.status_code)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                geo_result['timestamp'] = geo_result['timestamp'].isoformat()
                payload_geo = json.dumps({
                "records": [
                    {
                    "value": {"ind": geo_result["ind"], "timestamp": geo_result["timestamp"], "latitude": geo_result["latitude"], "longitude": geo_result["longitude"], "country": geo_result["country"]}
                    }
                    ]
                })
                print(geo_result)

                invoke_url_geo = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/topics/12951463f185.geo"
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response_geo = requests.request("POST", invoke_url_geo, headers=headers, data=payload_geo)
                print(response_geo.status_code)

                # Set up Kinesis streams
                invoke_url_stream_geo = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12951463f185-geo/record"
                response_stream_geo = requests.request("PUT", invoke_url_stream_geo, headers=headers_stream, data=payload_geo)
                print(response_stream_geo.status_code)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
                user_result['date_joined'] = user_result['date_joined'].isoformat()
                payload_user = json.dumps({
                "records": [
                    {
                    #Data should be send as pairs of column_name:value, with different columns separated by commas       
                    "value": {"ind": user_result["ind"], "first_name": user_result["first_name"], "last_name": user_result["last_name"], "age": user_result["age"], "date_joined": user_result["date_joined"]}
                    }
                    ]
                })
                print(user_result)

                invoke_url_user = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/topics/12951463f185.user"
                headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
                response_user = requests.request("POST", invoke_url_user, headers=headers, data=payload_user)
                print(response_user.status_code)

                # Set up Kinesis streams
                invoke_url_stream_user = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12951463f185-user/record"
                response_stream_user = requests.request("PUT", invoke_url_stream_user, headers=headers_stream, data=payload_user)
                print(response_stream_user.status_code)
            
            '''print(pin_result)
            print(geo_result)
            print(user_result)
            

            invoke_url_pin = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/topics/12951463f185.pin"
            invoke_url_geo = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/topics/12951463f185.geo"
            invoke_url_user = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/topics/12951463f185.user"

            headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
            response_pin = requests.request("POST", invoke_url_pin, headers=headers, data=payload_pin)
            print(response_pin.status_code)
            response_geo = requests.request("POST", invoke_url_geo, headers=headers, data=payload_geo)
            response_user = requests.request("POST", invoke_url_user, headers=headers, data=payload_user)

            # Set up Kinesis streams
            invoke_url_stream_pin = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12951463f185-pin/record"
            invoke_url_stream_geo = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12951463f185-geo/record"
            invoke_url_stream_user = "https://7txpgfi8ok.execute-api.us-east-1.amazonaws.com/dev/streams/streaming-12951463f185-user/record"

            headers_stream = {'Content-Type': 'application/json'}

            response_stream_pin = requests.request("PUT", invoke_url_stream_pin, headers=headers_stream, data=payload_pin)
            response_stream_geo = requests.request("PUT", invoke_url_stream_geo, headers=headers_stream, data=payload_geo)
            response_stream_user = requests.request("PUT", invoke_url_stream_user, headers=headers_stream, data=payload_user)
            print(response_stream_pin.status_code)'''


if __name__ == "__main__":
    run_infinite_post_data_loop()
    print('Working')
