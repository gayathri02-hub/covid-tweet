# Scalable Cloud-Based COVID Tweet Processing System

This project implements a real-time and batch-processing pipeline for analyzing COVID-19 tweet data using Python and AWS services. It supports parallel processing, auto-scaling, and cloud-native storage and monitoring.

## Project Features

- Real-time data ingestion using AWS Kinesis
- Batch and stream processing with sequential, multiprocessing, and MapReduce modes
- Performance benchmarking (throughput, latency, time)
- Output storage on AWS S3
- Auto-scaling based on CPU utilization (threshold: 50%)
- Monitoring via AWS CloudWatch
- All results and graphs are exported as CSV and PNG

---

## File Structure

- send_covid_data_to_pipe.py # Simulates real-time tweet stream (producer)
- recieve_covid_data.py # Processes data from Kinesis stream (consumer)
- multi_mode_runner.py # Runs sequential, parallel, and hybrid processing
- metrics_plot.py # Plots performance graphs from result CSV
- processing_results.csv # Benchmark results for all modes


## Requirements

- Python 3.12+
- AWS CLI configured (with access to Kinesis, EC2, S3, CloudWatch)
- Boto3
- Pandas
- Matplotlib

## How to Run

- python file_name.py
