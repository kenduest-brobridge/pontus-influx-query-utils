# InfluxDB Query Scripts for v1 and v2

This repository contains Python scripts for querying InfluxDB v1 and v2 instances. The scripts are organized into two directories: `v1` for InfluxDB v1 and `v2` for InfluxDB v2. They provide functionality to list databases/buckets, measurements/tables, and host tag values, as well as retrieve the latest record times for hosts in specified measurements.

## Table of Contents
- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Directory Structure](#directory-structure)
- [Scripts](#scripts)
  - [InfluxDB v1 Scripts (`v1`)](#influxdb-v1-scripts-v1)
  - [InfluxDB v2 Scripts (`v2`)](#influxdb-v2-scripts-v2)
- [Usage Examples](#usage-examples)
- [Output Formats](#output-formats)
- [Notes](#notes)

## Overview
These scripts are designed to interact with InfluxDB v1 and v2 to perform administrative and querying tasks, such as listing databases or buckets, retrieving measurement names, and extracting host tag values or the latest record times. The scripts are written in Python and use the `influxdb-python` library for v1 and `influxdb-client` for v2, along with the `requests` library for InfluxQL queries.

The scripts are particularly useful for:
- Auditing InfluxDB instances to understand database/bucket structure.
- Extracting host-specific data for monitoring or reporting.
- Generating CSV and text reports with UTC and local timestamps (for v2 scripts).

## Prerequisites
- **Python**: Version 3.6 or higher.
- **Dependencies**:
  - For v1 scripts: `influxdb` (install with `pip install influxdb`).
  - For v2 scripts: `influxdb-client`, `requests`, `pytz` (install with `pip install influxdb-client requests pytz`).
- **InfluxDB Setup**:
  - For v1: An InfluxDB v1.x instance with a valid username and password.
  - For v2: An InfluxDB v2.x instance with a valid token, organization, and DBRP mappings for InfluxQL compatibility.
- **Environment**:
  - A virtual environment is recommended (e.g., `python -m venv venv` and `source venv/bin/activate`).
  - System timezone configured for accurate local time conversions in v2 scripts.

## Directory Structure
```
script/
├── v1/
│   ├── database-host-tag-list.py
│   ├── database-list.py
│   ├── database-table-host-list.py
│   ├── database-table-list.py
├── v2/
│   ├── bucket-host-tag-list.py
│   ├── bucket-list.py
│   ├── bucket-tables-host-list.py
│   ├── bucket-tables-list.py
├── README.md
```

- `v1/`: Scripts for InfluxDB v1.
- `v2/`: Scripts for InfluxDB v2.

## Scripts

### InfluxDB v1 Scripts (`v1`)
These scripts interact with InfluxDB v1 using the `influxdb-python` library or HTTP API.

1. **`database-list.py`**
   - **Purpose**: Lists all databases in the InfluxDB v1 instance.
   - **Arguments**:
     - `--url`: InfluxDB server URL (e.g., `http://localhost:8086`).
     - `--username`: InfluxDB username.
     - `--password`: InfluxDB password.
   - **Output**: Prints a list of database names to the console.

2. **`database-table-list.py`**
   - **Purpose**: Lists all measurements (tables) in each database.
   - **Arguments**:
     - `--url`, `--username`, `--password`: Same as above.
   - **Output**: Prints database names and their measurements to the console.

3. **`database-host-tag-list.py`**
   - **Purpose**: Lists all unique host tag values for measurements in each database.
   - **Arguments**:
     - `--url`, `--username`, `--password`: Same as above.
     - `--database`: Optional, specify a database to query.
   - **Output**: Prints host tag values per measurement, saved as CSV files in the `output` directory.

4. **`database-table-host-list.py`**
   - **Purpose**: Lists host tag values and optionally the latest record time for each host in specified measurements.
   - **Arguments**:
     - `--url`, `--username`, `--password`, `--database`: Same as above.
     - `--measurement`: Optional, specify a measurement.
     - `--all-measurement`: Query all measurements in the database.
     - `--latest-time`: Include the latest record time for each host.
     - `--output-dir`: Directory for CSV output (default: `output`).
   - **Output**: Console output and CSV files with host and timestamp data.

### InfluxDB v2 Scripts (`v2`)
These scripts use the `influxdb-client` library and InfluxQL via the v2 HTTP API.

1. **`bucket-list.py`**
   - **Purpose**: Lists all buckets in the InfluxDB v2 instance.
   - **Arguments**:
     - `--url`: InfluxDB server URL (e.g., `http://localhost:8086`).
     - `--token`: InfluxDB access token.
     - `--org`: Organization name.
   - **Output**: Prints bucket names, IDs, and retention periods to the console.

2. **`bucket-tables-list.py`**
   - **Purpose**: Lists all measurements (tables) in each bucket, using InfluxQL.
   - **Arguments**:
     - `--url`, `--token`, `--org`: Same as above.
   - **Output**: Prints bucket names and their measurements to the console, with debug information.

3. **`bucket-host-tag-list.py`**
   - **Purpose**: Lists unique host tag values for measurements in a specified bucket.
   - **Arguments**:
     - `--url`, `--token`, `--org`: Same as above.
     - `--bucket`: Bucket name to query.
     - `--measurement`: Optional, specify a measurement.
     - `--all-measurement`: Query all measurements in the bucket.
     - `--output-dir`: Directory for CSV output (default: `output`).
   - **Output**: Console output and CSV files with host tag values.

4. **`bucket-tables-host-list.py`**
   - **Purpose**: Lists host tag values and optionally the latest record time (in UTC and local time) for each host in specified measurements.
   - **Arguments**:
     - `--url`, `--token`, `--org`, `--bucket`: Same as above.
     - `--measurement`: Optional, specify a measurement.
     - `--all-measurement`: Query all measurements in the bucket.
     - `--latest-time`: Include the latest record time for each host.
     - `--output-dir`: Directory for CSV output (default: `output`).
   - **Output**:
     - Console output (redirectable to `.txt`) with host names, UTC, and local times.
     - Per-measurement CSV files (`<bucket>_<measurement>.csv`) with `Host`, `LastTime_UTC`, `LastTime_Local` columns.
     - Summary CSV (`all-result.csv`) with `Host`, `OldTime_UTC`, `OldTime_Local`, `NewTime_UTC`, `NewTime_Local` columns.

## Usage Examples

### InfluxDB v1
List all databases:
```bash
python v1/database-list.py --url http://localhost:8086 --username admin --password mypassword
```

List measurements in a database:
```bash
python v1/database-table-list.py --url http://localhost:8086 --username admin --password mypassword
```

List host tags and latest times for a measurement:
```bash
python v1/database-table-host-list.py --url http://localhost:8086 --username admin --password mypassword --database mydb --measurement cpu --latest-time
```

### InfluxDB v2
List all buckets:
```bash
python v2/bucket-list.py --url http://localhost:8086 --token my_token --org my_org
```

List measurements in buckets:
```bash
python v2/bucket-tables-list.py --url http://localhost:8086 --token my_token --org my_org
```

List host tags and latest times for a measurement, saving to CSV and text:
```bash
python v2/bucket-tables-host-list.py --url http://localhost:8086 --token my_token --org my_org --bucket my_bucket --measurement cpu --latest-time > output.txt
```

## Output Formats
- **Console Output (Text)**:
  - For scripts with `--latest-time` (e.g., `bucket-tables-host-list.py`), outputs include:
    - Host names.
    - Timestamps in UTC and local time (system timezone).
  - Redirect to a `.txt` file with `> output.txt`.
  - Example:
    ```
    Measurement: cpu (1/1)
    ================================================================================
    Latest record time for each host in measurement 'cpu' (bucket: my_bucket):
    Host                           Time (UTC)                    Time (Local)
    --------------------------------------------------------------------------------
    server1                        2025-04-24 10:00:00.123456    2025-04-24 18:00:00.123456
    ```

- **CSV Output**:
  - For `bucket-tables-host-list.py` with `--latest-time`:
    - **Per-measurement CSV** (`output/<bucket>_<measurement>.csv`):
      ```csv
      Host,LastTime_UTC,LastTime_Local
      server1,2025-04-24 10:00:00.123456,2025-04-24 18:00:00.123456
      ```
    - **Summary CSV** (`output/all-result.csv`):
      ```csv
      Host,OldTime_UTC,OldTime_Local,NewTime_UTC,NewTime_Local
      server1,2025-04-24 09:00:00.123456,2025-04-24 17:00:00.123456,2025-04-24 10:00:00.123456,2025-04-24 18:00:00.123456
      ```
  - Other scripts generate CSVs with host tags or measurement lists, without timestamps unless specified.

## Notes
- **Timezone Handling**:
  - The `bucket-tables-host-list.py` script uses the system's local timezone for local time conversions. Ensure the system timezone is correctly set (e.g., `Asia/Tokyo`).
  - UTC times are parsed from InfluxDB and converted to local time using the `pytz` library.
- **InfluxDB v2 Requirements**:
  - DBRP (Database Retention Policy) mappings are required for InfluxQL queries in v2. Ensure mappings are set up in the InfluxDB UI or CLI.
  - Tokens must have read permissions for the specified buckets.
- **Error Handling**:
  - Scripts include robust error handling for HTTP errors, invalid time formats, and missing data.
  - Debug output is printed to help diagnose issues (e.g., raw API responses).
- **Dependencies**:
  - Install required packages in the virtual environment: `pip install influxdb influxdb-client requests pytz`.
- **Output Directory**:
  - CSV files are saved to the `output` directory by default. Override with `--output-dir`.

For issues or contributions, please contact the repository maintainer.
