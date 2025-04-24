#!/bin/env python3

from influxdb import InfluxDBClient
import argparse
import json

def list_databases_and_measurements(url: str, username: str, password: str):
    try:
        # Initialize InfluxDB v1.x client
        client = InfluxDBClient(host=url.split('://')[1].split(':')[0], 
                               port=int(url.split(':')[-1]), 
                               username=username or None, 
                               password=password or None)

        # Query all databases
        databases = client.query('SHOW DATABASES')
        db_list = [db['name'] for db in databases.get_points()]

        print(f"{'Database Name':<30} {'Retention Policy':<40} {'Retention':<15}")
        print("-" * 85)
        for db in db_list:
            # Query retention policies
            try:
                rp_query = client.query(f'SHOW RETENTION POLICIES ON "{db}"')
                retention_policies = list(rp_query.get_points())
                # Log raw response for debugging
                print(f"  Debug: Raw response for SHOW RETENTION POLICIES on database {db}: {json.dumps(retention_policies, indent=2)}")
            except Exception as e:
                print(f"  Error querying retention policies for database {db}: {e}")
                retention_policies = []

            if retention_policies:
                for rp in retention_policies:
                    duration = rp['duration'] if rp['duration'] != '0s' else 'infinite'
                    print(f"{db:<30} {rp['name']:<40} {duration:<15}")
            else:
                print(f"{db:<30} {'none':<40} {'infinite':<15}")

            # Query measurements
            try:
                measurements = client.query(f'SHOW MEASUREMENTS ON "{db}"')
                measurement_list = [m['name'] for m in measurements.get_points()]
                # Log raw response for debugging
                print(f"  Debug: Raw response for SHOW MEASUREMENTS on database {db}: {json.dumps(measurement_list, indent=2)}")
                print(f"  Measurements (Tables): {', '.join(measurement_list) if measurement_list else 'None'}")
                if not measurement_list:
                    print(f"  Warning: No measurements found for database {db}. Check data presence or retention policies.")
            except Exception as e:
                print(f"  Error querying measurements for database {db}: {e}")
                print("  Measurements (Tables): None")
            print()

    except Exception as e:
        print(f"Error accessing InfluxDB: {e}")
    finally:
        client.close()

def query_measurement(url: str, username: str, password: str, database: str, measurement: str = None):
    try:
        # Initialize client with specific database
        client = InfluxDBClient(host=url.split('://')[1].split(':')[0], 
                               port=int(url.split(':')[-1]), 
                               username=username or None, 
                               password=password or None, 
                               database=database)

        # Query tag values using InfluxQL
        tag_key = "host"
        if measurement:
            query = f'SHOW TAG VALUES ON "{database}" FROM "{measurement}" WITH KEY = "{tag_key}"'
            print(f"Querying tag '{tag_key}' values for measurement '{measurement}' in database '{database}'.")
        else:
            query = f'SHOW TAG VALUES ON "{database}" WITH KEY = "{tag_key}"'
            print(f"Querying tag '{tag_key}' values across all measurements in database '{database}'.")

        result = client.query(query)
        # Log raw response for debugging
        raw_result = list(result.raw.get('series', []))
        print(f"  Debug: Raw response for SHOW TAG VALUES: {json.dumps(raw_result, indent=2)}")

        tag_values = [point['value'] for point in result.get_points()]
        tag_values = list(set(tag_values))  # Remove duplicates

        print(f"\nTag: {tag_key}")
        print(f"List of '{tag_key}' tag values in database '{database}'{' for measurement ' + measurement if measurement else ' (all measurements)'}:")
        print("-" * 60)
        if tag_values:
            for value in sorted(tag_values):
                print(value)
        else:
            print(f"No '{tag_key}' tag values found.")

    except Exception as e:
        print(f"Error querying '{tag_key}' tag values in database '{database}'{' for measurement ' + measurement if measurement else ''}: {e}")
    finally:
        client.close()

def get_measurements(url: str, username: str, password: str, database: str):
    """Helper function to get all measurements in a database using InfluxQL."""
    try:
        client = InfluxDBClient(host=url.split('://')[1].split(':')[0], 
                               port=int(url.split(':')[-1]), 
                               username=username or None, 
                               password=password or None, 
                               database=database)
        measurements = client.query('SHOW MEASUREMENTS')
        measurement_list = [m['name'] for m in measurements.get_points()]
        client.close()
        return measurement_list
    except Exception as e:
        print(f"Error querying measurements for database {database}: {e}")
        return []

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List InfluxDB v1 databases and query host tag values.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--username", default="", help="InfluxDB username (optional if authentication is disabled)")
    parser.add_argument("--password", default="", help="InfluxDB password (optional if authentication is disabled)")
    parser.add_argument("--database", help="Database name to query (optional)")
    parser.add_argument("--measurement", help="Measurement to query for host tag values (optional)")
    parser.add_argument("--all-measurement", action="store_true", help="Query host tag values for all measurements in the database")

    args = parser.parse_args()

    # List databases and measurements
    list_databases_and_measurements(url=args.url, username=args.username, password=args.password)

    # Query host tag values
    if args.database:
        if args.all_measurement:
            query_measurement(
                url=args.url,
                username=args.username,
                password=args.password,
                database=args.database,
                measurement=None
            )
        elif args.measurement:
            query_measurement(
                url=args.url,
                username=args.username,
                password=args.password,
                database=args.database,
                measurement=args.measurement
            )
        else:
            print("Error: Please specify --measurement or --all-measurement when providing --database.")
    else:
        if args.measurement or args.all_measurement:
            print("Error: --database is required when using --measurement or --all-measurement.")

