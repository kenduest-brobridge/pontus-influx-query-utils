#!/bin/env python3

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
import argparse

def list_buckets_and_measurements(url: str, token: str, org: str):
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        buckets_api = client.buckets_api()
        query_api = client.query_api()
        buckets = buckets_api.find_buckets()

        print(f"{'Bucket Name':<30} {'Bucket ID':<40} {'Retention':<15}")
        print("-" * 85)
        for b in buckets.buckets:
            retention = b.retention_rules[0].every_seconds if b.retention_rules else "infinite"
            print(f"{b.name:<30} {b.id:<40} {retention:<15}")

            # Query measurements (tables) in the bucket
            query = f'''
                import "influxdata/influxdb/schema"
                schema.measurements(bucket: "{b.name}")
            '''
            try:
                tables = query_api.query(query=query)
                measurements = [record["_value"] for table in tables for record in table.records]
                if measurements:
                    print(f"  Measurements (Tables): {', '.join(measurements)}")
                else:
                    print("  Measurements (Tables): None")
            except InfluxDBError as e:
                print(f"  Error querying measurements for bucket {b.name}: {e}")
            print()  # Empty line for readability

    except InfluxDBError as e:
        print(f"Error accessing InfluxDB: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List all InfluxDB v2 buckets and their measurements.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--token", required=True, help="InfluxDB access token")
    parser.add_argument("--org", required=True, help="Organization name")

    args = parser.parse_args()
    list_buckets_and_measurements(url=args.url, token=args.token, org=args.org)

