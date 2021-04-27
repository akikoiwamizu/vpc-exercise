# **Consumer Financial Protection Bureau** | Consumer Complaints

Access the CFPB's Open Data API or download the consumer complaints database
directly from their website as a zip file to your home directory.

The steps for this Python 3.7 script are fairly simple. If this is the first time extracting the CFPB’s consumer complaint database (2M+ rows), then download the zip file directly from the website here. For daily updates, I decided it was overkill to download the entire database and overwrite the Redshift table, especially due to the API throttling in place for large requests, so there is functionality implemented for accessing the Open Data API and appending any new complaint records (via a daily crontab to kick off the job). After some error handling and casting of column data types, the consumer complaint data is saved to an S3 bucket and copied into a Redshift table assuming that the dummy object, __Database.py__, can handle calls to the Redshift cluster & S3 resources and that the respective credentials are populated.

A dry run of the __main.py__ script will assume that this is the first time extracting the CFPB data and thus will download the entire database file and create a table in Redshift, so use --help to see what parameters can be passed to change the extraction method and date range required to call the API (pass params in daily crontab).

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the packages listed in requirements.txt.

```bash
pip install -r requirements.txt
```

## Usage

```python
# Review the parameters available and their defaults
python3 main.py --help  

# Download entire consumer complaints file directly from site
python3 main.py

# Access API and use date range defaults in the GET request
python3 main.py --method api

# Access API and add date range passed in the GET request
python3 main.py --method api --start 2021-04-01 --end 2021-04-02  

```

## Contributions
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.
