#!/bin/env python3

from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
import argparse
import requests
import json

def list_buckets_and_measurements(url: str, token: str, org: str):
    try:
        client = InfluxDBClient(url=url, token=token, org=org)
        buckets_api = client.buckets_api()

        buckets = buckets_api.find_buckets()
        print(f"{'Bucket Name':<30} {'Bucket ID':<40} {'Retention':<15}")
        print("-" * 85)
        for b in buckets.buckets:
            retention = b.retention_rules[0].every_seconds if b.retention_rules else "infinite"
            print(f"{b.name:<30} {b.id:<40} {retention:<15}")

            # Use InfluxQL SHOW MEASUREMENTS via HTTP
            query = f'SHOW MEASUREMENTS'
            try:
                response = requests.post(
                    f"{url}/query?org={org}&db={b.name}",
                    headers={"Authorization": f"Token {token}"},
                    params={"q": query}
                )
                response.raise_for_status()
                data = response.json()

                # Log raw response for debugging
                print(f"  Debug: Raw response for SHOW MEASUREMENTS on bucket {b.name}: {json.dumps(data, indent=2)}")

                measurements = []
                if "results" in data and data["results"] and "series" in data["results"][0]:
                    measurements = [m["values"][0][0] for m in data["results"][0]["series"]]
                print(f"  Measurements (Tables): {', '.join(measurements) if measurements else 'None'}")
                if not measurements:
                    print(f"  Warning: No measurements found for bucket {b.name}. Check DBRP mapping, token permissions, or data presence.")

            except requests.exceptions.HTTPError as e:
                print(f"  Error querying measurements for bucket {b.name}: HTTP {e.response.status_code} - {e.response.text}")
            except requests.exceptions.RequestException as e:
                print(f"  Error querying measurements for bucket {b.name}: {e}")
            except Exception as e:
                print(f"  Unexpected error querying measurements for bucket {b.name}: {e}")
            print()

    except InfluxDBError as e:
        print(f"Error accessing InfluxDB: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        client.close()

def query_measurement(url: str, token: str, org: str, bucket: str, measurement: str = None):
    try:
        # Use InfluxQL SHOW TAG VALUES via HTTP
        tag_key = "host"
        if measurement:
            query = f'SHOW TAG VALUES ON "{bucket}" FROM "{measurement}" WITH KEY = "{tag_key}"'
            print(f"Querying tag '{tag_key}' values for measurement '{measurement}' in bucket '{bucket}'.")
        else:
            query = f'SHOW TAG VALUES ON "{bucket}" WITH KEY = "{tag_key}"'
            print(f"Querying tag '{tag_key}' values across all measurements in bucket '{bucket}'.")

        response = requests.post(
            f"{url}/query?org={org}&db={bucket}",
            headers={"Authorization": f"Token {token}"},
            params={"q": query}
        )
        response.raise_for_status()
        data = response.json()

        # Log raw response for debugging
        print(f"  Debug: Raw response for SHOW TAG VALUES: {json.dumps(data, indent=2)}")

        if "series" not in data["results"][0]:
            print(f"No '{tag_key}' tag values found in bucket '{bucket}'{' for measurement ' + measurement if measurement else ''}.")
            return

        print(f"\nTag: {tag_key}")
        print(f"List of '{tag_key}' tag values in bucket '{bucket}'{' for measurement ' + measurement if measurement else ' (all measurements)'}:")
        print("-" * 60)
        host_values = []
        for series in data["results"][0]["series"]:
            host_values.extend(v[1] for v in series["values"])
        host_values = list(set(host_values))  # Remove duplicates
        if host_values:
            for host in sorted(host_values):
                print(host)
        else:
            print("No 'host' tag values found.")

    except requests.exceptions.HTTPError as e:
        print(f"Error querying '{tag_key}' tag values in bucket '{bucket}'{' for measurement ' + measurement if measurement else ''}: HTTP {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error querying '{tag_key}' tag values in bucket '{bucket}'{' for measurement ' + measurement if measurement else ''}: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def get_measurements(url: str, token: str, org: str, bucket: str):
    """Helper function to get all measurements in a bucket using InfluxQL."""
    try:
        query = f'SHOW MEASUREMENTS'
        response = requests.post(
            f"{url}/query?org={org}&db={bucket}",
            headers={"Authorization": f"Token {token}"},
            params={"q": query}
        )
        response.raise_for_status()
        data = response.json()

        measurements = []
        if "results" in data and data["results"] and "series" in data["results"][0]:
            measurements = [m["values"][0][0] for m in data["results"][0]["series"]]
        return measurements
    except requests.exceptions.HTTPError as e:
        print(f"Error querying measurements for bucket {bucket}: HTTP {e.response.status_code} - {e.response.text}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error querying measurements for bucket {bucket}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error while querying measurements: {e}")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List InfluxDB v2 buckets and query host tag values.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--token", required=True, help="InfluxDB access token")
    parser.add_argument("--org", required=True, help="Organization name")
    parser.add_argument("--bucket", help="Bucket name to query (optional)")
    parser.add_argument("--measurement", help="Measurement (table) to query (optional)")
    parser.add_argument("--all-measurement", action="store_true", help="Query host tag values for all measurements in the bucket")

    args = parser.parse_args()

    # List buckets and measurements
    list_buckets_and_measurements(url=args.url, token=args.token, org=args.org)

    # Query host tag values
    if args.bucket:
        if args.all_measurement:
            # Query host tag values across all measurements in the bucket
            query_measurement(
                url=args.url,
                token=args.token,
                org=args.org,
                bucket=args.bucket,
                measurement=None
            )
        elif args.measurement:
            # Query a single measurement
            query_measurement(
                url=args.url,
                token=args.token,
                org=args.org,
                bucket=args.bucket,
                measurement=args.measurement
            )
        else:
            print("Error: Please specify --measurement or --all-measurement when providing --bucket.")
    else:
        if args.measurement or args.all_measurement:
            print("Error: --bucket is required when using --measurement or --all-measurement.")
