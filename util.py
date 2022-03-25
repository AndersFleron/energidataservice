import datetime
import argparse

def valid_date(input):
    try:
        return datetime.strptime(input, "%Y-%m-%d")
    except ValueError:
        msg = f"Input {input} is not a valid date"
        raise argparse.ArgumentTypeError(msg)
