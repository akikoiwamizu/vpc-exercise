#!/opt/venv/bin/python3
"""
# Consumer Financial Protection Bureau
This script is used to extract the consumer complaint db from the CFPB and
import it into a Redshift table located in Civis Platform.
___
## Script Information
* Author: Akiko Iwamizu
* Last Updated: 04/26/2021

## Steps:
* 1a) Make a GET request to the CFPB Open API
* 1b) OR download complaint data from CFPB site
* 2) Inspect JSON response and format table
* 3) Upload the formatted table to S3 bucket
* 4) Copy CSV from S3 into a RedShift table
___
"""
import os
import sys
import requests
import logging
import pandas as pd
from json.decoder import JSONDecodeError
from datetime import datetime, timedelta
import argparse
from Database import *
from zipfile import ZipFile

# Assume the following environment variables are correctly populated.
AWS_ACCESS_KEY_ID = 'xxxxxxxxxxx'
AWS_SECRET_ACCESS_KEY = 'xxxxxxxxxxx'
TABLE_NAME = 'consumer_complaints'
S3_BUCKET_NAME = 'xxxxxxxxxxx'

# Set the fields extracted to column names and their equivalent data types.
SCHEMA = [
    {'name': 'complaint_id', 'type': 'INT64'},
    {'name': 'date_received', 'type': 'DATE'},
    {'name': 'product', 'type': 'STRING'},
    {'name': 'sub_product', 'type': 'STRING'},
    {'name': 'issue', 'type': 'STRING'},
    {'name': 'sub_issue', 'type': 'STRING'},
    {'name': 'company', 'type': 'STRING'},
    {'name': 'state', 'type': 'STRING'},
    {'name': 'zip', 'type': 'STRING'},
    {'name': 'consumer_consent', 'type': 'BOOL'},
    {'name': 'date_sent_to_company', 'type': 'DATE'},
    {'name': 'company_response', 'type': 'STRING'},
    {'name': 'timely_response', 'type': 'BOOL'},
    {'name': 'disputed', 'type': 'BOOL'}
]


def cast_columns(df, sch):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.
    sch : type
        Description of parameter `sch`.

    Returns
    -------
    dataframe
        Contains the complaint data passed, but with updated column data types.

    """
    schema = sch
    col_list = [i['name'] for i in schema]
    temp = col_list
    df = df[temp]

    for c in df.columns:
        j = temp.index(c)
        col_type = schema[j]['type']
        pd.set_option('mode.chained_assignment', None)

        if col_type == 'INT64':
            df[c] = df[c].fillna(0)
            df[c] = pd.to_numeric(df[c], downcast='integer')
            df[c] = df[c].astype('int64')

        if col_type == 'FLOAT64':
            df[c] = df[c].astype('float64')

        if col_type == 'STRING':
            df[c] = df[c].astype('str')

        if col_type == 'BOOL':
            df[c] = df[c].astype('bool')

        if col_type == 'DATE':
            df[c] = pd.to_datetime(df[c]).dt.date

    return df


def is_downloadable(url):
    """Short summary.

    Parameters
    ----------
    url : type
        Description of parameter `url`.

    Returns
    -------
    boolean
        A true/false is returned if the url link has a downloadable object.

    """
    h = requests.head(url, allow_redirects=True)
    header = h.headers
    content_type = header.get('content-type')

    if 'text' in content_type.lower():
        return False

    if 'html' in content_type.lower():
        return False

    return True


def download_cfpb():
    """CFPB Open API calls for large amounts of data are unreliable. Instead,
    download the complaint data file directly from the site if desired.

    Returns
    -------
    dataframe
        Contains complaint data after downloading directly from the CFPB site.

    """
    download_url = 'https://files.consumerfinance.gov/ccdb/complaints.csv.zip'
    results = pd.DataFrame()

    # Check if the file is downloadable before making any calls.
    logging.info(
        f'Can this file be downloaded? {is_downloadable(download_url)}')

    if is_downloadable(download_url) is True:
        try:
            r = requests.get(download_url, allow_redirects=True)
            open('complaints.csv.zip', 'wb').write(r.content)
            zf = ZipFile('complaints.csv.zip')
            df = pd.read_csv(zf.open('complaints.csv'))
            logging.info(f'Downloading {len(df)} rows from CSV.')
            logging.info(f'Columns found in CSV: {list(df.columns)}')

            # Rename columns before casting data types.
            df.columns = df.columns.str.lower()
            df.columns = df.columns.str.replace(' ', '_')
            df.columns = df.columns.str.replace('?', '')
            df.columns = df.columns.str.replace('-', '_')
            df.rename(columns={'company_response_to_consumer': 'company_response', 'zip_code': 'zip',
                               'consumer_consent_provided': 'consumer_consent', 'consumer_disputed': 'disputed'},  inplace=True)

            logging.info(f'Final columns after cleaning: {list(df.columns)}')
            results = cast_columns(df, SCHEMA)

            # Delete file in directory after successful df creation.
            zf.close()
            os.remove('complaints.csv.zip')
            logging.info('Deleting original zip file from directory.')
        except Exception as e:
            logging.error(f'There was an issue with the file: {e}')

    return results


def access_api(url, start, end):
    """Accesses the CFPB Open API and gets complaint data for a given date range.

    Returns
    -------
    dataframe
        Contains complaint data as an API response over a specific date range.

    """
    offset = 0
    reattempts = 0
    results = []
    next_page = True

    while next_page:
        logging.info(f'Page: {str(int(offset / 1000))} .')

        # Parameters to pass in the CFPB's Open API GET Request.
        data = {'no_aggs': 'true', 'sort': 'created_date_desc', 'frm': offset,
                'size': 1000, 'date_received_min': start, 'date_received_max': end}

        try:
            r = requests.get(url=url, params=data)
            r.raise_for_status()
        except requests.exceptions.HTTPError as eh:
            print('HTTP Error:', eh)
        except requests.exceptions.ConnectionError as ec:
            print('Error Connecting:', ec)
        except requests.exceptions.Timeout as et:
            print('Timeout Error:', et)
        except requests.exceptions.RequestException as er:
            print('Another Error:', er)

        try:
            response = r.json()
        except JSONDecodeError as e:
            logging.warning(f'No JSON response returned: {e}')

        # If HTTP response other than 200 or 504, then exit loop.
        if r.status_code not in (200, 504):
            logging.error('Terminating loop - HTTP error cannot be resolved.')
            break

        # If no complaints were found or returned, then exit loop.
        if response is None or response['hits']['total'] == 0:
            logging.error('Exit paging loop since no complaints were found.')
            break

        # Server timeouts occur frequently and at random. Try up to 10 times.
        if r.status_code == 504 and reattempts <= 10:
            logging.warning('Timeout error occurred.reattempt the request.')
            reattempts += 1
        elif r.status_code == 504 and reattempts > 10:
            logging.error('Reached the max reattempts allowed.exiting loop.')
            break

        try:
            # Get the total number of complaints in the database.
            total_complaints = int(response['hits']['total'])
            logging.info(f'{total_complaints} total complaints found!')
            logging.info(f'{abs(total_complaints - offset)} complaints left!')

            # Get the total number of complaints in the response (max is 1000).
            total_hits = response['hits']['hits']
            logging.info(
                f'{int(offset) + int(len(total_hits))} extracted so far.')

            if (data['size'] + offset) < total_complaints:
                next_page = True
            else:
                next_page = False

            # If the API request was successful, increment the offset.
            if r.status_code == 200:
                offset += data['size']

            for complaints in response['hits']['hits']:
                df = pd.json_normalize(complaints['_source'])
                results.append(df)
        except(IndexError, KeyError, TypeError):
            logging.error('JSON response was not structured as expected.')

    try:
        results = pd.concat(results)
        results.rename(columns={'timely': 'timely_response', 'zip_code': 'zip',
                                'consumer_consent_provided': 'consumer_consent', 'consumer_disputed': 'disputed'}, inplace=True)
    except Exception as e:
        logging.error(f'There was an issue saving the results to a df: {e}')

    return results


def generate_table(start, end):
    """Calls the CFPB Open API, creates a df, and casts column data types.

    Returns
    -------
    dataframe
        Returns the final dataframe containing all consumer complaints.

    """
    api_url = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/'
    response = access_api(api_url, start, end)
    results = pd.DataFrame()

    try:
        results = cast_columns(response, SCHEMA)
    except Exception as e:
        logging.error(f'There was an issue casting the df columns: {e}')

    return results


def write_df_to_csv(df, name):
    """Write pandas dataframe as a CSV and upload to S3 bucket.

    Parameters
    ----------
    df : DataFrame
        A pandas dataframe containing all consumer complaints.
    name : String
        A string representing the name of the CSV file to upload to S3.

    """
    if not os.path.exists('results'):
        os.mkdir('results')
    results_file = f'{name}.csv'
    results_file_path = f'results/{name}.csv'

    logging.info(f'Writing {len(df)} records to {results_file}')

    # Convert dataframe to CSV and write to current directory.
    df.to_csv(results_file_path, header=True, index=False)

    # Upload CSV file in directory to S3 bucket.
    d = Database()
    d.upload_file_to_s3(results_file_path, S3_BUCKET_NAME, results_file)
    logging.info(f'Uploading {results_file} to S3 bucket.')

    # TODO: Check for successful upload validation from S3 before deleting.
    # os.remove(results_file_path)

    return None


def load_csv_to_redshift():
    """Create RedShift table and copy data from S3 bucket into the table."""
    table_query = """CREATE TABLE IF NOT EXISTS cfpb.consumer_complaints(
        complaint_id BIGINT DISTKEY,
        date_received DATE,
        product VARCHAR(256),
        sub_product VARCHAR(256),
        issue VARCHAR(256),
        sub_issue VARCHAR(256),
        company VARCHAR(256),
        state VARCHAR(2),
        zip VARCHAR(5),
        consumer_consent BOOLEAN,
        date_sent_to_company DATE,
        company_response VARCHAR(256),
        timely_response BOOLEAN,
        disputed BOOLEAN);"""

    # Use dummy object to create table if it does not exist in Redshift.
    # TODO: Add error handling to check if sql call was successful or not.
    d = Database()
    d.run_sql(table_query)
    logging.info(f'Created {TABLE_NAME} table in Redshift.')

    # Fastest method for CSV to Redshift is using "copy" method.
    # TODO: Add error handling to check if file exists in S3 bucket.
    copy_query = """COPY cfpb.consumer_complaints
        FROM 's3://S3_BUCKET_NAME/consumer_complaints.csv'
        CREDENTIALS 'aws_access_key_id=YOUR_ACCESS_KEY;
        aws_secret_access_key=YOUR_SECRET_ACCESS_KEY'
        CSV IGNOREHEADER 1 TRIMBLANKS BLANKSASNULL NULL 'NaN' ACCEPTINVCHARS;"""

    # Use dummy object to copy table from S3 to Redshift.
    # TODO: Add error handling to check if sql call was successful or not.
    d.run_sql(copy_query)
    logging.info(f'Copied {TABLE_NAME}.csv from S3 to Redshift.')

    # TODO: Review any errors returned in STL_LOAD_ERRORS.

    return None


def main():
    logging.basicConfig(level=0)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--start', help='returns complaints with start >= date_received_min (format: YYYY-MM-DD)')
    parser.add_argument(
        '--end', help='returns complaints with end < date_received_max (format: YYYY-MM-DD)')
    parser.add_argument(
        '--method', default='download', help='a method to get data from the CFPB (options: download or api)')
    args = parser.parse_args()

    if args.method == 'api':
        if args.start is None or args.end is None:
            print('If using the API, then a date range should be passed.')
            today = datetime.now().strftime('%Y-%m-%d')
            if args.start is None:
                args.start = (datetime.strptime(today, '%Y-%m-%d')
                              - timedelta(days=3)).strftime('%Y-%m-%d')
            if args.end is None:
                args.end = (datetime.strptime(today, '%Y-%m-%d')
                            - timedelta(days=2)).strftime('%Y-%m-%d')

        # Checking that date range is valid.FYI CFPB has a 3 day lag time!
        if args.start >= args.end:
            logging.error(f'Date range {args.start} - {args.end} is invalid.')
            logging.error('Please try again!')
            sys.exit()

        # Print a warning if the date range passed is more than 3 months.
        date_diff = abs((datetime.strptime(args.end, '%Y-%m-%d')
                         - datetime.strptime(args.start, '%Y-%m-%d')).days)
        if date_diff >= 90:  # About 3 months
            logging.warning(f'Trying to pull {date_diff} days of data.')
            logging.warning('Server timeout might occur, so watch logs.')

        # Extract data from API and format as a table to post to RedShift.
        print(f'Using this date range: {args.start} - {args.end}')
        payload = generate_table(args.start, args.end)
    else:
        # Download the entire complaints CSV file directly from the CFPB site.
        payload = download_cfpb()

    # Write the pandas dataframe as a CSV to the S3 bucket.
    write_df_to_csv(payload, TABLE_NAME)

    # Create empty Redshift table and copy data from S3 bucket into it.
    # TODO: create args for deciding overwrite or append options for Redshift.
    load_csv_to_redshift()

    print('Consumer Complaints Table Sync Complete!')


if __name__ == '__main__':
    main()
