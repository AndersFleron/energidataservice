import datetime
import argparse

def valid_date(input):
    try:
        return datetime.strptime(input, "%Y-%m-%d")
    except ValueError:
        msg = f"Input {input} is not a valid date"
        raise argparse.ArgumentTypeError(msg)

def valid_int(input):
    if input < 1:
        raise argparse.ArgumentTypeError("Order should be larger than 0")