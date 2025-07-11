import boto3
import json
import time
import ast
from collections import Counter, deque

def grab_live_covid_data(stream_name, read_duration=60, window_size=5):
    try:
        stream_reader = boto3.client('kinesis', region_name='us-east-1')
        stream_info = stream_reader.describe_stream(StreamName=stream_name)
        shard_id = stream_info['StreamDescription']['Shards'][0]['ShardId']
        shard_iterator = stream_reader.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="LATEST"
        )["ShardIterator"]

        print(f"Reading from stream: {stream_name}")
        start_time = time.time()

        all_time_counter = Counter()
        sliding_window = deque()

        while time.time() - start_time < read_duration:
            current_time = time.time()
            response = stream_reader.get_records(ShardIterator=shard_iterator, Limit=10)
            shard_iterator = response["NextShardIterator"]

            for record in response["Records"]:
                try:
                    covid_tweet = json.loads(record["Data"])
                    if "hashtags" in covid_tweet and covid_tweet["hashtags"]:
                        raw_tags = covid_tweet["hashtags"]
                        tags = ast.literal_eval(raw_tags) if isinstance(raw_tags, str) else raw_tags
                        tags = [tag.strip().lower() for tag in tags if tag.strip()]
                        all_time_counter.update(tags)
                        sliding_window.append((current_time, tags))
                except Exception as e:
                    print("Could not read a covid_tweet:", e)

            while sliding_window and current_time - sliding_window[0][0] > window_size:
                _, old_tags = sliding_window.popleft()
            window_counter = Counter()
            for _, tag_list in sliding_window:
                window_counter.update(tag_list)

            print("\nTop 5 Hashtags (All-Time):", all_time_counter.most_common(5))
            print("Top 5 Hashtags (Last", window_size, "Seconds):", window_counter.most_common(5))
            time.sleep(1)

    except Exception as error:
        print("Could not connect to stream:", error)

if __name__ == "__main__":
    stream_title = "covid_stream"
    grab_live_covid_data(stream_title, read_duration=60)
