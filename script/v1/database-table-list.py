#!/bin/env python3

from influxdb import InfluxDBClient
import argparse

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
            # Query retention policies for the database
            try:
                rp_query = client.query(f'SHOW RETENTION POLICIES ON "{db}"')
                retention_policies = list(rp_query.get_points())
            except Exception as e:
                print(f"  Error querying retention policies for database {db}: {e}")
                retention_policies = []

            if retention_policies:
                for rp in retention_policies:
                    duration = rp['duration'] if rp['duration'] != '0s' else 'infinite'
                    print(f"{db:<30} {rp['name']:<40} {duration:<15}")
            else:
                print(f"{db:<30} {'none':<40} {'infinite':<15}")

            # Query measurements in the database
            try:
                measurements = client.query(f'SHOW MEASUREMENTS ON "{db}"')
                measurement_list = [m['name'] for m in measurements.get_points()]
                print(f"  Measurements (Tables): {', '.join(measurement_list) if measurement_list else 'None'}")
            except Exception as e:
                print(f"  Error querying measurements for database {db}: {e}")
                print("  Measurements (Tables): None")
            print()  # Empty line for readability

    except Exception as e:
        print(f"Error accessing InfluxDB: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="List all InfluxDB v1 databases and their measurements.")
    parser.add_argument("--url", required=True, help="InfluxDB server URL (e.g., http://localhost:8086)")
    parser.add_argument("--username", default="", help="InfluxDB username (optional if authentication is disabled)")
    parser.add_argument("--password", default="", help="InfluxDB password (optional if authentication is disabled)")

    args = parser.parse_args()
    list_databases_and_measurements(url=args.url, username=args.username, password=args.password)

