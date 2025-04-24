#!/bin/env python3
from influxdb_client import InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
import argparse
import requests
import json
from datetime import datetime
import os
import csv
import pytz  # Added for local time conversion

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

            query = f'SHOW MEASUREMENTS'
            try:
                response = requests.post(
                    f"{url}/query?org={org}&db={b.name}",
                    headers={"Authorization": f"Token {token}"},
                    params={"q": query}
                )
                response.raise_for_status()
                data = response.json()

                print(f"  Debug: Raw response for SHOW MEASUREMENTS on bucket {b.name}: {json.dumps(data, indent=2)}")

                measurements = []
                if "results" in data and data["results"] and "series" in data["results"][0] and "values" in data["results"][0]["series"][0]:
                    measurements = [m[0] for m in data["results"][0]["series"][0]["values"]]
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

def query_measurement(url: str, token: str, org: str, bucket: str, measurement: str = None, latest_time: bool = False, all_measurement: bool = False, output_dir: str = "output"):
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Dictionary to track all host times across measurements
        host_times = {}  # {host: [(utc_time_str, utc_formatted, local_formatted), ...]}

        # Get local timezone from system
        local_tz = datetime.now().astimezone().tzinfo

        # Get measurements to query
        if measurement:
            measurements = [measurement]
            print(f"\nQuerying {'latest record time' if latest_time else 'host tag values'} for measurement '{measurement}' in bucket '{bucket}'.")
        else:
            measurements = get_measurements(url, token, org, bucket)
            if not measurements:
                print(f"\nNo measurements found in bucket '{bucket}'. Check DBRP mapping or data presence.")
                return
            print(f"\nQuerying {'latest record time' if latest_time else 'host tag values'} for all measurements in bucket '{bucket}' ({len(measurements)} measurements found).")

        processed_count = 0
        for meas in measurements:
            processed_count += 1
            print(f"\nMeasurement: {meas} ({processed_count}/{len(measurements)})")
            print("=" * 80)

            # Initialize CSV data for this measurement
            csv_data = []

            # Step 1: Get all host tag values for the measurement
            tag_key = "host"
            query = f'SHOW TAG VALUES ON "{bucket}" FROM "{meas}" WITH KEY = "{tag_key}"'
            print(f"  Fetching all '{tag_key}' tag values for measurement '{meas}'.")

            try:
                response = requests.post(
                    f"{url}/query?org={org}&db={bucket}",
                    headers={"Authorization": f"Token {token}"},
                    params={"q": query}
                )
                response.raise_for_status()
                data = response.json()

                print(f"    Debug: Raw response for SHOW TAG VALUES: {json.dumps(data, indent=2)}")

                host_values = []
                if "results" in data and data["results"] and "series" in data["results"][0]:
                    for series in data["results"][0]["series"]:
                        if "values" in series:
                            host_values.extend(v[1] for v in series["values"])
                    host_values = list(set(host_values))  # Remove duplicates
                else:
                    print(f"  No '{tag_key}' tag values found for measurement '{meas}' in bucket '{bucket}'.")
                    continue

                if not host_values:
                    print(f"  No '{tag_key}' tag values found for measurement '{meas}'.")
                    continue

                # Step 2: Process based on mode
                if latest_time:
                    # Query latest record for each host
                    print(f"  Latest record time for each host in measurement '{meas}' (bucket: {bucket}):")
                    print(f"  {'Host':<30} {'Time (UTC)':<30} {'Time (Local)':<30}")
                    print("  " + "-" * 90)

                    for host in sorted(host_values):
                        query = f'SELECT * FROM "{meas}" WHERE host=\'{host}\' LIMIT 1'
                        try:
                            response = requests.post(
                                f"{url}/query?org={org}&db={bucket}",
                                headers={"Authorization": f"Token {token}"},
                                params={"q": query}
                            )
                            response.raise_for_status()
                            data = response.json()

                            print(f"    Debug: Raw response for SELECT * WHERE host='{host}': {json.dumps(data, indent=2)}")

                            if "results" not in data or not data["results"] or "series" not in data["results"][0]:
                                print(f"  No data found for host '{host}' in measurement '{meas}'.")
                                continue

                            series = data["results"][0]["series"][0]
                            utc_time_str = series["values"][0][0]  # Time is the first column

                            # Parse UTC time
                            try:
                                utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                                utc_time = pytz.utc.localize(utc_time)
                            except ValueError:
                                try:
                                    utc_time = datetime.strptime(utc_time_str, "%Y-%m-%dT%H:%M:%SZ")
                                    utc_time = pytz.utc.localize(utc_time)
                                except ValueError:
                                    print(f"  Invalid time format for host '{host}': {utc_time_str}")
                                    continue

                            # Convert to local time
                            local_time = utc_time.astimezone(local_tz)

                            # Format times
                            formatted_utc_time = utc_time.strftime("%Y-%m-%d %H:%M:%S.%f")
                            formatted_local_time = local_time.strftime("%Y-%m-%d %H:%M:%S.%f")

                            # Store times for OldTime/NewTime calculation
                            if host not in host_times:
                                host_times[host] = []
                            host_times[host].append((utc_time_str, formatted_utc_time, formatted_local_time))

                            print(f"  {host:<30} {formatted_utc_time:<30} {formatted_local_time:<30}")
                            csv_data.append({
                                "Host": host,
                                "LastTime_UTC": formatted_utc_time,
                                "LastTime_Local": formatted_local_time
                            })

                        except requests.exceptions.HTTPError as e:
                            print(f"  Error querying host '{host}' in measurement '{meas}': HTTP {e.response.status_code} - {e.response.text}")
                            continue
                        except requests.exceptions.RequestException as e:
                            print(f"  Error querying host '{host}' in measurement '{meas}': {e}")
                            continue
                        except Exception as e:
                            print(f"  Unexpected error querying host '{host}' in measurement '{meas}': {e}")
                            continue

                    # Write CSV file for this measurement
                    if csv_data:
                        csv_file = os.path.join(output_dir, f"{bucket}_{meas}.csv")
                        try:
                            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.DictWriter(f, fieldnames=["Host", "LastTime_UTC", "LastTime_Local"])
                                writer.writeheader()
                                writer.writerows(csv_data)
                            print(f"  Saved results to {csv_file}")
                        except IOError as e:
                            print(f"  Error writing CSV file '{csv_file}': {e}")
                    else:
                        print(f"  No data to save for measurement '{meas}'.")

                else:
                    # Display host tag values
                    print(f"  List of '{tag_key}' tag values in measurement '{meas}' (bucket: {bucket}):")
                    print("  " + "-" * 60)
                    for host in sorted(host_values):
                        print(f"  {host}")
                        csv_data.append({"Host": host, "LastTime_UTC": "", "LastTime_Local": ""})

                    # Write CSV file for this measurement
                    if csv_data:
                        csv_file = os.path.join(output_dir, f"{bucket}_{meas}.csv")
                        try:
                            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                                writer = csv.DictWriter(f, fieldnames=["Host", "LastTime_UTC", "LastTime_Local"])
                                writer.writeheader()
                                writer.writerows(csv_data)
                            print(f"  Saved results to {csv_file}")
                        except IOError as e:
                            print(f"  Error writing CSV file '{csv_file}': {e}")
                    else:
                        print(f"  No data to save for measurement '{meas}'.")

            except requests.exceptions.HTTPError as e:
                print(f"  Error querying '{tag_key}' tag values for measurement '{meas}' in bucket '{bucket}': HTTP {e.response.status_code} - {e.response.text}")
                continue
            except requests.exceptions.RequestException as e:
                print(f"  Error querying '{tag_key}' tag values for measurement '{meas}' in bucket '{bucket}': {e}")
                continue
            except Exception as e:
                print(f"  Unexpected error querying measurement '{meas}' in bucket '{bucket}': {e}")
                continue
            print()

        # Generate all-result.csv with Host, OldTime_UTC, OldTime_Local, NewTime_UTC, NewTime_Local
        if latest_time and host_times:
            result_file = os.path.join(output_dir, "all-result.csv")
            try:
                with open(result_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["Host", "OldTime_UTC", "OldTime_Local", "NewTime_UTC", "NewTime_Local"])
                    writer.writeheader()

                    for host in sorted(host_times.keys()):
                        times = host_times[host]
                        if not times:
                            continue

                        oldest_time = min(times, key=lambda x: x[0])  # Compare by raw UTC string
                        newest_time = max(times, key=lambda x: x[0])

                        writer.writerow({
                            "Host": host,
                            "OldTime_UTC": oldest_time[1],
                            "OldTime_Local": oldest_time[2],
                            "NewTime_UTC": newest_time[1],
                            "NewTime_Local": newest_time[2]
                        })

                print(f"\nSaved summary to {result_file}")
            except IOError as e:
                print(f"Error writing summary file '{result_file}': {e}")
        elif latest_time:
            print("\nNo host times found to generate all-result.csv.")

        print(f"\nCompleted querying {processed_count}/{len(measurements)} measurements in bucket '{bucket}'.")

    except Exception as e:
        print(f"Unexpected error processing bucket '{bucket}': {e}")

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
        if "results" in data and data["results"] and "series" in data["results"][0] and "values" in data["results"][0]["series"][0]:
            measurements = [m[0] for m in data["results"][0]["series"][0]["values"]]
        print(f"  Debug: Found measurements in bucket {bucket}: {', '.join(measurements) if measurements else 'None'}")
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
    parser = argparse.ArgumentParser(description="List InfluxDB v2 buckets and query host tag values or latest record time.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--token", required=True, help="InfluxDB access token")
    parser.add_argument("--org", required=True, help="Organization name")
    parser.add_argument("--bucket", help="Bucket name to query (optional)")
    parser.add_argument("--measurement", help="Measurement (table) to query (optional)")
    parser.add_argument("--all-measurement", action="store_true", help="Query host tag values or latest time for all measurements in the bucket")
    parser.add_argument("--latest-time", action="store_true", help="Query the latest record time for each host in the measurement(s)")
    parser.add_argument("--output-dir", default="output", help="Directory to save CSV and summary output files (default: output)")

    args = parser.parse_args()

    # List buckets and measurements
    list_buckets_and_measurements(url=args.url, token=args.token, org=args.org)

    # Query host tag values or latest time
    if args.bucket:
        if args.latest_time or args.all_measurement or args.measurement:
            query_measurement(
                url=args.url,
                token=args.token,
                org=args.org,
                bucket=args.bucket,
                measurement=args.measurement,
                latest_time=args.latest_time,
                all_measurement=args.all_measurement,
                output_dir=args.output_dir
            )
        else:
            print("Error: Please specify --measurement, --all-measurement, or --latest-time when providing --bucket.")
    else:
        print("Error: --bucket is required when using --measurement, --all-measurement, or --latest-time.")
