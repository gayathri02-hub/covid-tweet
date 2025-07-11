import pandas as pd
import time
import ast
from multiprocessing import Pool, Process, cpu_count, Manager
from collections import Counter
import boto3
from io import StringIO

def extract_tag_list(blob):
    try:
        parsed = ast.literal_eval(blob)
        return [entry.strip().lower() for entry in parsed if entry.strip()]
    except (ValueError, SyntaxError):
        return []

def count_tags_solo(data_chunk):
    begin = time.time()
    bucket = Counter()
    for row in data_chunk:
        bucket.update(extract_tag_list(row))
    end = time.time()
    duration = end - begin
    speed = len(data_chunk) / duration if duration > 0 else 0
    delay = duration / len(data_chunk) if len(data_chunk) > 0 else 0
    return bucket, duration, speed, delay

def counter_for_chunk(chunk):
    interim = Counter()
    for row in chunk:
        interim.update(extract_tag_list(row))
    return interim

def count_tags_multi(data_chunk):
    begin = time.time()
    slots = cpu_count()
    pieces = [data_chunk[i::slots] for i in range(slots)]
    with Pool(slots) as crew:
        partials = crew.map(counter_for_chunk, pieces)
    combined = Counter()
    for p in partials:
        combined.update(p)
    end = time.time()
    duration = end - begin
    speed = len(data_chunk) / duration if duration > 0 else 0
    delay = duration / len(data_chunk) if len(data_chunk) > 0 else 0
    return combined, duration, speed, delay

def tag_process_parallel(tag_data, shared_box):
    counter, timer, speed, delay = count_tags_multi(tag_data)
    shared_box["tagset"] = (counter, timer, speed, delay)

def mood_process_sim(text_data, shared_box):
    begin = time.time()
    mood = {"positive": 0, "negative": 0}
    for line in text_data:
        lowered = line.lower()
        if "good" in lowered or "great" in lowered:
            mood["positive"] += 1
        elif "bad" in lowered or "worst" in lowered:
            mood["negative"] += 1
    end = time.time()
    duration = end - begin
    speed = len(text_data) / duration if duration > 0 else 0
    delay = duration / len(text_data) if len(text_data) > 0 else 0
    shared_box["moodset"] = (mood, duration, speed, delay)

def hybrid_combination_mode(full_chunk):
    manager = Manager()
    result_box = manager.dict()
    tag_area = full_chunk["hashtags"].dropna().tolist()
    desc_area = full_chunk["user_description"].dropna().tolist()

    part1 = Process(target=tag_process_parallel, args=(tag_area, result_box))
    part2 = Process(target=mood_process_sim, args=(desc_area, result_box))

    start_time = time.time()
    part1.start()
    part2.start()
    part1.join()
    part2.join()
    end_time = time.time()

    overall_time = end_time - start_time
    overall_speed = len(full_chunk) / overall_time if overall_time > 0 else 0
    overall_delay = overall_time / len(full_chunk) if len(full_chunk) > 0 else 0

    return result_box, overall_time, overall_speed, overall_delay

def push_to_cloud(csv_payload, s3_bucket, s3_filekey):
    s3hub = boto3.client('s3')
    try:
        s3hub.put_object(Body=csv_payload, Bucket=s3_bucket, Key=s3_filekey)
        print(f"Uploaded to s3://{s3_bucket}/{s3_filekey}")
    except Exception as problem:
        print(f"S3 upload failed: {problem}")

if __name__ == "__main__":
    file_location = "covid_dataset/covid19_tweets.csv"
    bucket_name = "scalablecovidbucket"
    object_key = "benchmarks/processing_results.csv"

    frame = pd.read_csv(file_location)
    portions = [0.25, 0.5, 0.75, 1.0]

    table = []

    for chunk in portions:
        print(f"\nRunning mode on {int(chunk * 100)}% of records")
        part_frame = frame.iloc[:int(len(frame) * chunk)]
        tags_data = part_frame["hashtags"].dropna().tolist()

        result_a, time_a, speed_a, gap_a = count_tags_solo(tags_data)
        table.append(["Sequential", f"{int(chunk*100)}%", time_a, speed_a, gap_a])
        print("Top 5 tags (Sequential):")
        for t, v in result_a.most_common(5):
            print(f"{t}: {v}")

        result_b, time_b, speed_b, gap_b = count_tags_multi(tags_data)
        table.append(["Parallel", f"{int(chunk*100)}%", time_b, speed_b, gap_b])
        print("Top 5 tags (Parallel):")
        for t, v in result_b.most_common(5):
            print(f"{t}: {v}")

        result_c, time_c, speed_c, gap_c = hybrid_combination_mode(part_frame)
        table.append(["Hybrid", f"{int(chunk*100)}%", time_c, speed_c, gap_c])
        print("Sentiment counts (Hybrid):")
        for label, score in result_c["moodset"][0].items():
            print(f"{label}: {score}")

    summary = pd.DataFrame(table, columns=["Approach", "Portion", "Time", "Throughput", "Latency"])
    memory_stream = StringIO()
    summary.to_csv(memory_stream, index=False)
    push_to_cloud(memory_stream.getvalue(), bucket_name, object_key)

