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

import requests
import json
import argparse
import logging
import Database

# Assume the following environment variables are correctly populated.
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
# TABLE_NAME
# S3_BUCKET_NAME

# Set the fields extracted to column names and their equivalent data types.
BQ_SCHEMA = [
    {"name": "complaint_id", "type": bigquery.enums.SqlTypeNames.INT64},
    {"name": "date_received", "type": bigquery.enums.SqlTypeNames.DATE},
    {"name": "product", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "sub_product", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "issue", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "sub_issue", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "company", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "state", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "zip", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "consumer_consent", "type": bigquery.enums.SqlTypeNames.BOOL},
    {"name": "date_sent_to_company", "type": bigquery.enums.SqlTypeNames.DATE},
    {"name": "company_response", "type": bigquery.enums.SqlTypeNames.STRING},
    {"name": "timely_response", "type": bigquery.enums.SqlTypeNames.BOOL},
    {"name": "disputed", "type": bigquery.enums.SqlTypeNames.BOOL}
]

# Need to convert field data types since API fields default to type string.


# The RedShift table name, schema, and desired fields with their data types.
# CREATE TABLE cfpb.consumer_complaints (
#     complaint_id BIGINT DISTKEY,
#     date_received DATE,
#     product VARCHAR(256),
#     sub_product VARCHAR(256),
#     issue VARCHAR(256),
#     sub_issue VARCHAR(256),
#     company VARCHAR(256),
#     state VARCHAR(2),
#     zip VARCHAR(5),
#     consumer_consent BOOLEAN,
#     date_sent_to_company DATE,
#     company_response VARCHAR(256),
#     timely_response BOOLEAN,
#     disputed BOOLEAN
# );

def cast_columns(df, sch):
    BQ_POSTS = sch
    l = [i["name"] for i in BQ_POSTS]

    temp = l
    df = df[temp]

    for c in df.columns:
        j = temp.index(c)
        col = BQ_POSTS[j]["type"]
        pd.set_option('mode.chained_assignment', None)

        if col == bigquery.enums.SqlTypeNames.INT64:
            df[c] = df[c].fillna(0)
            df[c] = pd.to_numeric(df[c], downcast='integer')
            df[c] = df[c].astype('int64')

        if col == bigquery.enums.SqlTypeNames.FLOAT64:
            df[c] = df[c].astype('float64')

        if col == bigquery.enums.SqlTypeNames.STRING:
            df[c] = df[c].astype('str')

        if col == bigquery.enums.SqlTypeNames.BOOL:
            df[c] = df[c].astype('bool')

        if col == bigquery.enums.SqlTypeNames.DATE:
            df[c] = pd.to_datetime(df[c]).dt.date

    return df


def cfpb_api(url):
    """Short summary.

    Returns
    -------
    type
        Description of returned object.

    """

    # TODO: Add exception handling here when the status code returned != 200
    r = requests.get(url)

    # TODO: Add exception handling here if response is empty
    response = r.json()

    # TODO: Add exception handling here if "hits" does not exist in repsonse
    results = pd.json_normalize(response['hits'], sep = '_', max_level = 1)

return results


def format_tbl(url, schema):
    """Short summary.

    Parameters
    ----------
    df : type
        Description of parameter `df`.

    Returns
    -------
    type
        Description of returned object.

    """
    response = cfpb_api(url)
    response = cast_columns(response, schema)

return response


def main(request):
    """Short summary.

    Parameters
    ----------
    request : type
        Description of parameter `request`.

    Returns
    -------
    type
        Description of returned object.

    """
    try:
        print(f'Passing arguments for GCF...')
        arguments = request.get_json(force=True)
        start = escape(arguments['start']) if 'start' in arguments else None # Optional
        end = escape(arguments['end']) if 'end' in arguments else None # Optional
        write_type = escape(arguments['write_type']) if 'write_type' in arguments else None # Optional
    except (KeyError, NameError, TypeError, RuntimeError, ValueError) as e:
        print(f'WARNING: Handling error - {e}')

    # DELETE: Set & Update BigQuery Table
    BQ_TABLE = f"acronym-data-transfer.test.consumer_complaints"
    bq = bigquery.Client(project="acronym-data-transfer")

    # Construct the CFPB's Open API GET Request.
    api_url = 'https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/'

    # Extract data from API and format as a table to post to RedShift.
    payload = format_tbl(url, BQ_SCHEMA)

    BQ_CONFIG = []
    for i in BQ_SCHEMA:
        BQ_CONFIG.append(bigquery.SchemaField(i["name"], i["type"]))

    print(f"Syncing to {BQ_TABLE}...")

    try:
        # write_disposition = 'WRITE_TRUNCATE': If the table already exists, BigQuery overwrites the table data and uses the schema from the load.
        # DEFAULT: If the table already exists, BigQuery appends the table data and uses the schema from the load.
        if write_type == "TRUNCATE":
            job = bq.load_table_from_dataframe(payload, BQ_TABLE, job_config=bigquery.LoadJobConfig(schema=BQ_CONFIG, write_disposition='WRITE_TRUNCATE'))
        else:
            job = bq.load_table_from_dataframe(payload, BQ_TABLE, job_config=bigquery.LoadJobConfig(schema=BQ_CONFIG))
        print(job.result())
    except (KeyError, NameError, TypeError, ValueError, RuntimeError) as e:
        print(f'WARNING: Handling error - {e}')

    print(f"Consumer Complaints Table Sync Complete!")


if __name__ == '__main__':
    main()
