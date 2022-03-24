import argparse
from datetime import date

import util
import ingest
import cleaner


def parse_args():
    """
    Defines the possible command line arguments. All with default values (except outputpath) amounting to the solution of the assignment.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("outputpath", type=str, help="Local path to save data")
    parser.add_argument("-s", "--startdate", type=util.valid_date, default=date(2022, 1, 1),
                        help="Start date of data extraction, should be in format YYYY-MM-DD")
    parser.add_argument("-e", "--enddate", type=util.valid_date, default=date(2022, 1, 23),
                        help="End date of data extraction, should be in format YYYY-MM-DD")
    parser.add_argument("-p", "--endpoint", type=str, default="https://api.energidataservice.dk/datastore_search_sql",
                        help="Endpoint for SQL searches in energidataservice API")
    parser.add_argument("-r", "--resource", type=str, default="fcrreservesdk2",
                        help="The resource id in energidataservice")
    parser.add_argument("-f", "--fields", type=list,
                        default=["HourUTC", "HourDK", "FCR_N_PriceDKK", "FCR_N_PriceEUR", "FCR_D_UpPriceDKK", "FCR_D_UpPriceEUR"],
                        help="List of fields to extract")
    return parser.parse_args()

def main():
    args = parse_args()
    data = ingest.ingest_data(args)
    clean_data = cleaner.clean_and_validate(data, args.startdate, args.enddate)
    clean_data.to_parquet(args.outputpath, partition_cols=["month", "day"])

if __name__ == '__main__':
    main()




