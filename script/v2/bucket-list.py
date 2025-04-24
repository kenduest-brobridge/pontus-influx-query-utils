#!/bin/env python3

from influxdb_client import InfluxDBClient
import argparse

def list_buckets(url: str, token: str, org: str):
    client = InfluxDBClient(url=url, token=token, org=org)
    buckets_api = client.buckets_api()
    buckets = buckets_api.find_buckets()

    print(f"{'Bucket Name':<30} {'Bucket ID':<40} {'Retention'}")
    print("-" * 80)
    for b in buckets.buckets:
        retention = b.retention_rules[0].every_seconds if b.retention_rules else "infinite"
        print(f"{b.name:<30} {b.id:<40} {retention}")

    client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List all InfluxDB v2 buckets.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--token", required=True, help="InfluxDB access token")
    parser.add_argument("--org", required=True, help="Organization name")

    args = parser.parse_args()
    list_buckets(url=args.url, token=args.token, org=args.org)

