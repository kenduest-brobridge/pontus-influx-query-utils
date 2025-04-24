#!/bin/env python3

import argparse
import requests
import json
from datetime import datetime
import os
import csv
import pytz
from tzlocal import get_localzone

def list_databases_and_measurements(url: str, username: str = None, password: str = None):
    try:
        print(f"{'Database Name':<30} {'Measurements':<50}")
        print("-" * 80)

        # Query all databases
        query = "SHOW DATABASES"
        params = {"q": query}
        if username and password:
            params["u"] = username
            params["p"] = password

        response = requests.get(f"{url}/query", params=params)
        response.raise_for_status()
        data = response.json()

        print(f"  Debug: Raw response for SHOW DATABASES: {json.dumps(data, indent=2)}")

        databases = []
        if "results" in data and data["results"] and "series" in data["results"][0] and "values" in data["results"][0]["series"][0]:
            databases = [d[0] for d in data["results"][0]["series"][0]["values"]]

        for db in databases:
            # Query measurements for each database
            query = f"SHOW MEASUREMENTS"
            params = {"q": query, "db": db}
            if username and password:
                params["u"] = username
                params["p"] = password

            try:
                response = requests.get(f"{url}/query", params=params)
                response.raise_for_status()
                data = response.json()

                print(f"  Debug: Raw response for SHOW MEASUREMENTS on database {db}: {json.dumps(data, indent=2)}")

                measurements = []
                if "results" in data and data["results"] and "series" in data["results"][0] and "values" in data["results"][0]["series"][0]:
                    measurements = [m[0] for m in data["results"][0]["series"][0]["values"]]
                print(f"{db:<30} {', '.join(measurements) if measurements else 'None':<50}")
                if not measurements:
                    print(f"  Warning: No measurements found for database {db}. Check permissions or data presence.")

            except requests.exceptions.HTTPError as e:
                print(f"  Error querying measurements for database {db}: HTTP {e.response.status_code} - {e.response.text}")
            except requests.exceptions.RequestException as e:
                print(f"  Error querying measurements for database {db}: {e}")
            except Exception as e:
                print(f"  Unexpected error querying measurements for database {db}: {e}")
            print()

    except requests.exceptions.HTTPError as e:
        print(f"Error querying databases: HTTP {e.response.status_code} - {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Error querying databases: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")

def format_time(time_value):
    """Format time in both UTC and local timezone, handling nanosecond precision."""
    # Remove nanoseconds by truncating to microseconds
    if '.' in time_value:
        base_time, fraction = time_value.split('.')
        # Take first 6 digits of fractional part (microseconds)
        fraction = fraction[:6] + 'Z'
        time_value_truncated = f"{base_time}.{fraction}"

    try:
        # Parse UTC time (microseconds)
        utc_time = datetime.strptime(time_value_truncated, "%Y-%m-%dT%H:%M:%S.%fZ")
        utc_time = utc_time.replace(tzinfo=pytz.UTC)
        formatted_utc = utc_time.strftime("%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        try:
            # Try without microseconds
            utc_time = datetime.strptime(time_value, "%Y-%m-%dT%H:%M:%SZ")
            utc_time = utc_time.replace(tzinfo=pytz.UTC)
            formatted_utc = utc_time.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            formatted_utc = time_value
            utc_time = None

    # Convert to local time
    if utc_time:
        local_tz = get_localzone()
        local_time = utc_time.astimezone(local_tz)
        formatted_local = local_time.strftime("%Y-%m-%d %H:%M:%S.%f" if formatted_utc.endswith(".%f") else "%Y-%m-%d %H:%M:%S")
    else:
        formatted_local = time_value

    return formatted_utc, formatted_local

def query_measurement(url: str, username: str, password: str, database: str, measurement: str = None, latest_time: bool = False, all_measurement: bool = False, output_dir: str = "output"):
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Dictionary to track all host times across measurements
        host_times = {}  # {host: [time1, time2, ...]}

        # Get measurements to query
        if measurement:
            measurements = [measurement]
            print(f"\nQuerying {'latest record time' if latest_time else 'host tag values'} for measurement '{measurement}' in database '{database}'.")
        else:
            measurements = get_measurements(url, username, password, database)
            if not measurements:
                print(f"\nNo measurements found in database '{database}'. Check permissions or data presence.")
                return
            print(f"\nQuerying {'latest record time' if latest_time else 'host tag values'} for all measurements in database '{database}' ({len(measurements)} measurements found).")

        processed_count = 0
        for meas in measurements:
            processed_count += 1
            print(f"\nMeasurement: {meas} ({processed_count}/{len(measurements)})")
            print("=" * 80)

            # Initialize CSV data for this measurement
            csv_data = []

            # Step 1: Get all host tag values for the measurement
            tag_key = "host"
            query = f'SHOW TAG VALUES FROM "{meas}" WITH KEY = "{tag_key}"'
            print(f"  Fetching all '{tag_key}' tag values for measurement '{meas}'.")

            try:
                params = {"q": query, "db": database}
                if username and password:
                    params["u"] = username
                    params["p"] = password
                response = requests.get(f"{url}/query", params=params)
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
                    print(f"  No '{tag_key}' tag values found for measurement '{meas}' in database '{database}'.")
                    continue

                if not host_values:
                    print(f"  No '{tag_key}' tag values found for measurement '{meas}'.")
                    continue

                # Step 2: Process based on mode
                if latest_time:
                    # Query latest record for each host
                    print(f"  Latest record time for each host in measurement '{meas}' (database: {database}):")
                    print(f"  {'Host':<30} {'Time_UTC':<30} {'Time_Local':<30}")
                    print("  " + "-" * 90)

                    for host in sorted(host_values):
                        query = f'SELECT * FROM "{meas}" WHERE host=\'{host}\' LIMIT 1'
                        try:
                            params = {"q": query, "db": database}
                            if username and password:
                                params["u"] = username
                                params["p"] = password
                            response = requests.get(f"{url}/query", params=params)
                            response.raise_for_status()
                            data = response.json()

                            print(f"    Debug: Raw response for SELECT * WHERE host='{host}': {json.dumps(data, indent=2)}")

                            if "results" not in data or not data["results"] or "series" not in data["results"][0]:
                                print(f"  No data found for host '{host}' in measurement '{meas}'.")
                                continue

                            series = data["results"][0]["series"][0]
                            time_value = series["values"][0][0]  # Time is the first column

                            # Store raw time for OldTime/NewTime calculation
                            if host not in host_times:
                                host_times[host] = []
                            host_times[host].append(time_value)

                            # Format time in UTC and local
                            formatted_utc, formatted_local = format_time(time_value)

                            print(f"  {host:<30} {formatted_utc:<30} {formatted_local:<30}")
                            csv_data.append({"Host": host, "LastTime_UTC": formatted_utc, "LastTime_Local": formatted_local})

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
                        csv_file = os.path.join(output_dir, f"{database}_{meas}.csv")
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
                    print(f"  List of '{tag_key}' tag values in measurement '{meas}' (database: {database}):")
                    print("  " + "-" * 60)
                    for host in sorted(host_values):
                        print(f"  {host}")
                        csv_data.append({"Host": host, "LastTime_UTC": "", "LastTime_Local": ""})

                    # Write CSV file for this measurement (only Host column)
                    if csv_data:
                        csv_file = os.path.join(output_dir, f"{database}_{meas}.csv")
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
                print(f"  Error querying '{tag_key}' tag values for measurement '{meas}' in database '{database}': HTTP {e.response.status_code} - {e.response.text}")
                continue
            except requests.exceptions.RequestException as e:
                print(f"  Error querying '{tag_key}' tag values for measurement '{meas}' in database '{database}': {e}")
                continue
            except Exception as e:
                print(f"  Unexpected error querying measurement '{meas}' in database '{database}': {e}")
                continue
            print()  # Add spacing after each measurement

        # Generate all-result.csv and all-result.txt with Host, OldTime_UTC, OldTime_Local, NewTime_UTC, NewTime_Local
        if latest_time and host_times:
            # Generate all-result.csv
            result_csv_file = os.path.join(output_dir, "all-result.csv")
            try:
                with open(result_csv_file, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=["Host", "OldTime_UTC", "OldTime_Local", "NewTime_UTC", "NewTime_Local"])
                    writer.writeheader()

                    # Process each host
                    for host in sorted(host_times.keys()):
                        times = host_times[host]
                        if not times:
                            continue

                        # Find oldest and newest times
                        oldest_time = min(times)
                        newest_time = max(times)

                        # Format times in UTC and local
                        formatted_old_utc, formatted_old_local = format_time(oldest_time)
                        formatted_new_utc, formatted_new_local = format_time(newest_time)

                        # Write row
                        writer.writerow({
                            "Host": host,
                            "OldTime_UTC": formatted_old_utc,
                            "OldTime_Local": formatted_old_local,
                            "NewTime_UTC": formatted_new_utc,
                            "NewTime_Local": formatted_new_local
                        })

                print(f"\nSaved summary to {result_csv_file}")
            except IOError as e:
                print(f"Error writing summary file '{result_csv_file}': {e}")

            # Generate all-result.txt
            result_txt_file = os.path.join(output_dir, "all-result.txt")
            try:
                with open(result_txt_file, 'w', encoding='utf-8') as f:
                    # Write header
                    f.write(f"{'Host':<30} {'OldTime_UTC':<30} {'OldTime_Local':<30} {'NewTime_UTC':<30} {'NewTime_Local':<30}\n")
                    f.write("-" * 150 + "\n")

                    # Process each host
                    for host in sorted(host_times.keys()):
                        times = host_times[host]
                        if not times:
                            continue

                        # Find oldest and newest times
                        oldest_time = min(times)
                        newest_time = max(times)

                        # Format times in UTC and local
                        formatted_old_utc, formatted_old_local = format_time(oldest_time)
                        formatted_new_utc, formatted_new_local = format_time(newest_time)

                        # Write row
                        f.write(f"{host:<30} {formatted_old_utc:<30} {formatted_old_local:<30} {formatted_new_utc:<30} {formatted_new_local:<30}\n")

                print(f"Saved summary to {result_txt_file}")
            except IOError as e:
                print(f"Error writing summary file '{result_txt_file}': {e}")
        elif latest_time:
            print("\nNo host times found to generate all-result.csv or all-result.txt.")

        print(f"\nCompleted querying {processed_count}/{len(measurements)} measurements in database '{database}'.")

    except Exception as e:
        print(f"Unexpected error processing database '{database}': {e}")

def get_measurements(url: str, username: str, password: str, database: str):
    """Helper function to get all measurements in a database using InfluxQL."""
    try:
        query = f'SHOW MEASUREMENTS'
        params = {"q": query, "db": database}
        if username and password:
            params["u"] = username
            params["p"] = password
        response = requests.get(f"{url}/query", params=params)
        response.raise_for_status()
        data = response.json()

        measurements = []
        if "results" in data and data["results"] and "series" in data["results"][0] and "values" in data["results"][0]["series"][0]:
            measurements = [m[0] for m in data["results"][0]["series"][0]["values"]]
        print(f"  Debug: Found measurements in database {database}: {', '.join(measurements) if measurements else 'None'}")
        return measurements
    except requests.exceptions.HTTPError as e:
        print(f"Error querying measurements for database {database}: HTTP {e.response.status_code} - {e.response.text}")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Error querying measurements for database {database}: {e}")
        return []
    except Exception as e:
        print(f"Unexpected error while querying measurements: {e}")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List InfluxDB v1 databases and query host tag values or latest record time.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--username", help="InfluxDB username (optional)")
    parser.add_argument("--password", help="InfluxDB password (optional)")
    parser.add_argument("--database", help="Database name to query (optional)")
    parser.add_argument("--measurement", help="Measurement (table) to query (optional)")
    parser.add_argument("--all-measurement", action="store_true", help="Query host tag values or latest time for all measurements in the database")
    parser.add_argument("--latest-time", action="store_true", help="Query the latest record time for each host in the measurement(s)")
    parser.add_argument("--output-dir", default="output", help="Directory to save CSV and summary output files (default: output)")

    args = parser.parse_args()

    # List databases and measurements
    list_databases_and_measurements(url=args.url, username=args.username, password=args.password)

    # Query host tag values or latest time
    if args.database:
        if args.latest_time or args.all_measurement or args.measurement:
            query_measurement(
                url=args.url,
                username=args.username,
                password=args.password,
                database=args.database,
                measurement=args.measurement,
                latest_time=args.latest_time,
                all_measurement=args.all_measurement,
                output_dir=args.output_dir
            )
        else:
            print("Error: Please specify --measurement, --all-measurement, or --latest-time when providing --database.")
    else:
        print("Error: --database is required when using --measurement, --all-measurement, or --latest-time.")

