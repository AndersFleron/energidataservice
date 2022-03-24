from random import randint
from time import sleep
import requests
import argparse


def ingest_data(args: argparse.Namespace) -> dict:
    """
        Main ingestion function, pulls the data specified in the args from the API and return it as a dict
        :param args: argparse.Namespace object containing the command line arguments
        :return: dict of the data
    """
    query = build_query(args)
    request = f"{args.endpoint}?sql={query}"
    data = request_data(request)
    return data
    

def request_data(request, retry=10) -> dict:
    """
        Function for requesting data from API. Includes retry to avoid failure due to sporadic connection errors and so forth.
        May fail in case of HTTP codes in range 201-299.
        :param request: complete endpoint for the request
        :param retry: number of retries
        :return dict of data
    """
    response = requests.get(request)

    if response.status_code != 200 and retry > 0:
        sleep(randint(5, 10))
        request_data(request, retry-1)
    else:
        response.raise_for_status()

    return response.json()["result"]["records"]


def build_query(args: argparse.Namespace):
    """
        Builds the SQL query needed for the API request. 
        :return SQL query string
    """
    fields_with_quotes = [f"\"{field}\"" for field in args.fields]
    query = f'''
        SELECT {",".join(fields_with_quotes)} 
        FROM "{args.resource}" 
        WHERE 
            date_trunc(\'day\', "HourUTC") >= date \'{str(args.startdate)}\' AND 
            date_trunc(\'day\', "HourUTC")  < date \'{str(args.enddate)}\''''
    return query

