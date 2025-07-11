import pandas as pd
import boto3
import json
import time

def mimic_live_tweet_stream(csv_file_path, stream_name, delay_between=1.0):
    try:
        tweet_records = pd.read_csv(csv_file_path)
        tweet_records = tweet_records.dropna(subset=["hashtags"])

        data_pipe = boto3.client('kinesis', region_name='us-east-1')

        print(f"Starting to send {len(tweet_records)} tweets to stream: {stream_name}")

        for idx, row in tweet_records.iterrows():
            tweet_object = {
                "user_name": row["user_name"],
                "location": row["user_location"],
                "content": row["user_description"],
                "hashtags": row["hashtags"],
                "device": row["source"]
            }
            data_pipe.put_record(
                StreamName=stream_name,
                Data=json.dumps(tweet_object),
                PartitionKey="partition_tweet"
            )

            print(f"Sent tweet #{idx + 1} to the stream.")
            time.sleep(delay_between)

        print("All tweets have been sent!")

    except Exception as problem:
        print("Something went wrong while sending tweets:")
        print(problem)

if __name__ == "__main__":
    kinesis_stream = "covid_stream"
    file_to_read = "covid_dataset/covid19_tweets.csv"
    mimic_live_tweet_stream(file_to_read, kinesis_stream, delay_between=1)
