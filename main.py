#!/opt/venv/bin/python3
"""
# Consumer Financial Protection Bureau
This script is used to extract the consumer complaint db from the CFPB and
import it into a Redshift table located in Civis Platform.
___

## Script Information
* Author: Akiko Iwamizu
* Last Updated: 04/25/2021

## Steps:
* Make a GET request to the CFPB Open API
* Inspect JSON response and format table
* Load the formatted table into S3 bucket
* Import table from S3 into RedShift table
___
"""
import os
import requests
import logging
import pandas as pd
from json.decoder import JSONDecodeError
from datetime import datetime, timedelta
import argparse
from Database import *
from zipfile import ZipFile
from tempfile import NamedTemporaryFile

# Assume the following environment variables are correctly populated.
S3_BUCKET_NAME = 'xxxxxxxxxxx'
TABLE_NAME = 'consumer_complaints'

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

# Need to convert field data types since API fields default to type string.
def cast_columns(df, sch):
    schema = sch
    l = [i['name'] for i in schema]
    temp = l
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
    """
    Check for if the url passed contains a downloadable item.

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
    """Short summary.

    Returns
    -------
    type
        Description of returned object.

    """
    download_url = 'https://files.consumerfinance.gov/ccdb/complaints.csv.zip'
    results = pd.DataFrame()

    # Check if the file is downloadable before making any calls.
    print(f'Can this file be downloaded? {is_downloadable(download_url)}')

    if is_downloadable(download_url) is True:
        # TODO: Add error handling here if the file was not found.
        try:
            r = requests.get(download_url, allow_redirects=True)
            open('complaints.csv.zip', 'wb').write(r.content)
            zf = ZipFile('complaints.csv.zip')
            df = pd.read_csv(zf.open('complaints.csv'))
            print(f'Downloaded {len(df)} rows from CSV...')
            print(f'Columns found in CSV: {list(df.columns)}')
            # Rename columns before casting data types.
            df.columns = df.columns.str.lower()
            df.columns = df.columns.str.replace(' ','_')
            df.columns = df.columns.str.replace('?','')
            df.columns = df.columns.str.replace('-','_')
            df.rename(columns={
                        'company_response_to_consumer':'company_response'
                        ,'zip_code':'zip'
                        ,'consumer_consent_provided':'consumer_consent'
                        ,'consumer_disputed':'disputed'}
                        , inplace=True)
            print(f'Final columns after cleaning: {list(df.columns)}')
            results = cast_columns(df, SCHEMA)
        except Exception as e:
            logging.error(f'There was an issue with the file: {e}')

    return results


def access_api(url, start, end):
    """Short summary.

    Returns
    -------
    type
        Description of returned object.

    """
    offset = 0
    results = []
    next_page = True

    while next_page:
        logging.info(f'Page: ' + str(int(offset / 1000)) + '...')

        # Construct the CFPB's Open API GET Request.
        data = {'no_aggs': 'true'
                , 'sort': 'created_date_desc'
                , 'frm': offset
                , 'size': 1000
                , 'date_received_min': start
                , 'date_received_max': end}

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
            logging.warning('No JSON response returned...')

        if response is None or r.status_code != 200:
            logging.warning('Exit paging loop since JSON response is empty...')
            break

        try:
            # Get the total number of complaints in the database.
            total_complaints = int(response['hits']['total'])
            logging.info(f'{total_complaints} total complaints found!')

            # Get the total number of complaints in the response (max is 1000).
            total_hits = response['hits']['hits']
            logging.info(f'{int(offset) + int(len(total_hits))} extracted so far...')

            if (data['size'] + offset) < total_complaints:
                next_page = True
            else:
                next_page = False

            offset += data['size']

            for complaints in response['hits']['hits']:
                df = pd.json_normalize(complaints['_source'])
                results.append(df)
                # if len(results) % 1000 == 0:
                #     print(f'Processed {len(results)} complaints.')
        except(IndexError, KeyError, TypeError):
            logging.error('JSON response was not structured as expected...')

    results = pd.concat(results)
    results.rename(columns={
                'timely':'timely_response'
                ,'zip_code':'zip'
                ,'consumer_consent_provided':'consumer_consent'
                ,'consumer_disputed':'disputed'}
                , inplace=True)

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
    results = cast_columns(response, SCHEMA)

    return results


def write_df_to_csv(df, name):
    """Write pandas dataframe as a csv and upload to S3.

    Parameters
    ----------
    df : DataFrame
        A pandas dataframe containing all consumer complaints.
    name : String
        A string representing the name of the csv file to be uploaded to S3.

    """
    if not os.path.exists('results'):
        os.mkdir('results')
    results_file_path = f'results/{name}.zip'

    output_file_name = f'{name}.csv'
    output_file = NamedTemporaryFile(mode='wt')

    logging.info(f'Writing {len(df)} records to {output_file_name}')

    # Write dataframe as CSV to temp file. Implement chunk size if too big.
    df.to_csv(output_file, index=False)
    output_file.flush()
    zipfile = ZipFile(results_file_path, 'w')
    zipfile.write(output_file.name, output_file_name)
    zipfile.close()

    # Upload CSV file to S3 bucket.
    d = Database()
    d.upload_file_to_s3(results_file_path, S3_BUCKET_NAME, f'{name}.zip')

    # TODO: Add logic to delete file in directory after successful upload to S3.

    return None

def load_csv_to_redshift():
    # The RedShift table name, schema, and desired fields with their data types.
    sql_query = """CREATE TABLE cfpb.consumer_complaints(
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

    return None


def get_dates(sta, sto):
    start = datetime.strptime(sta, "%Y-%m-%d")
    end = datetime.strptime(sto, "%Y-%m-%d")

    date_generated = [start + timedelta(days=x) for x in range(0, (end - start).days)]

    result = []

    for date in date_generated:
        result.append(date.strftime("%Y-%m-%d"))

    return result


def main():
    logging.basicConfig(level=0)
    parser = argparse.ArgumentParser()
    parser.add_argument('--start'
        , help='returns complaints with start >= date_received_min (format: YYYY-MM-DD)')
    parser.add_argument('--end'
        , help='returns complaints with end < date_received_max (format: YYYY-MM-DD)')
    parser.add_argument('--method', default='download'
        , help='a method to get data from the CFPB (options: download or api)')
    args = parser.parse_args()

    if args.method == 'api':
        if args.start is None and args.end is None:
            print('If using the API, then a date range should be passed.')
            today = datetime.now().strftime('%Y-%m-%d')
            args.start = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
            args.end = (datetime.strptime(today, '%Y-%m-%d') - timedelta(days=2)).strftime('%Y-%m-%d')

        # TODO: Check that date range is valid...CFPB has a two day lag time!

        # Extract data from API and format as a table to post to RedShift.
        print(f'Using this date range: {args.start} - {args.end}')
        payload = generate_table(args.start, args.end)

        # Write the pandas dataframe as a CSV to the S3 bucket.
        write_df_to_csv(payload_api, TABLE_NAME)
    else:
        # Download the entire complaints CSV file directly from the CFPB site.
        payload_down = download_cfpb()

    # Create empty Redshift table and copy data from S3 bucket into it.


    print(f'Consumer Complaints Table Sync Complete!')


if __name__ == '__main__':
    main()
