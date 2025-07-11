import pandas as pd
import matplotlib.pyplot as plt
import boto3
from io import StringIO, BytesIO

def fetch_csv_from_s3(bucket_name, object_key):
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket_name, Key=object_key)
    return pd.read_csv(response["Body"])

def upload_chart_to_s3(bucket_name, s3_path, plot_stream):
    client = boto3.client("s3")
    client.put_object(
        Bucket=bucket_name,
        Key=s3_path,
        Body=plot_stream,
        ContentType="image/png"
    )
    print(f"Uploaded to s3://{bucket_name}/{s3_path}")

def save_plot_to_buffer():
    buf = BytesIO()
    plt.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return buf

def generate_bar_chart(df):
    avg_time = df.groupby("Approach")["Time"].mean()
    plt.figure(figsize=(7, 5))
    avg_time.plot(kind="bar", color=["#66c2a5", "#fc8d62", "#8da0cb"])
    plt.ylabel("Average Time (seconds)")
    plt.title("Avg Time per Processing Mode")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    return save_plot_to_buffer()

def generate_line_chart(df, metric, y_label, title):
    plt.figure(figsize=(7, 5))
    for method in df["Approach"].unique():
        temp = df[df["Approach"] == method]
        x_vals = temp["Portion"].str.rstrip('%').astype(int)
        y_vals = temp[metric]
        plt.plot(x_vals, y_vals, label=method, marker="o", linewidth=2)
    plt.xlabel("Dataset Portion (%)")
    plt.ylabel(y_label)
    plt.title(title)
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.5)
    return save_plot_to_buffer()

if __name__ == "__main__":
    bucket = "scalablecovidbucket"
    source_csv = "benchmarks/processing_results.csv"

    df = fetch_csv_from_s3(bucket, source_csv)

    chart1 = generate_bar_chart(df)
    upload_chart_to_s3(bucket, "benchmarks/plots/time_bar_chart.png", chart1)

    chart2 = generate_line_chart(df, "Throughput", "Records per Second", "Throughput vs Dataset Size")
    upload_chart_to_s3(bucket, "benchmarks/plots/throughput_line_chart.png", chart2)

    chart3 = generate_line_chart(df, "Latency", "Seconds per Record", "Latency vs Dataset Size")
    upload_chart_to_s3(bucket, "benchmarks/plots/latency_line_chart.png", chart3)
