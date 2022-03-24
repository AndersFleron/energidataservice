import pandas as pd
import numpy as np
import logging


def clean_and_validate(json_data, start_date, end_date):
    """
    Main function for cleaning and validating, handles missing records, duplicated records and locates possible anomalies.
    :param json_data: API data in json format
    :param start_date: start date of data
    :param end_date: end date of data
    :return: cleaned and validated data as pd.DataFrame
    """
    data = pd.DataFrame.from_dict(json_data).replace(0, np.nan)
    data["HourUTC"] = pd.to_datetime(data["HourUTC"], format="%Y-%m-%dT%H:%M:%S")
    data["HourDK"] = pd.to_datetime(data["HourDK"], format="%Y-%m-%dT%H:%M:%S")
    data = data.sort_values(by="HourUTC")

    expected_records = pd.date_range(start_date, end_date, freq="H", closed="left")
    expected_num_records = len(expected_records)

    if len(data) != expected_num_records:
        data = handle_missing_entries(data, expected_records.tolist())

    if len(data) != expected_num_records:
        data = handle_duplicate_entries(data)

    data = interpolate_missing_values(data)
    identify_suspected_errors(data)
    return data

def handle_missing_entries(data: pd.DataFrame, expected_records: int):
    """
    Locates completely missing records and inserts blank records as substitutes
    :param data:
    :param expected_records: number of expected records based on specified dates
    :return DataFrame with no missing entries
    """
    missing_entries = [entry for entry in expected_records if entry not in data["HourUTC"].tolist()]
    missing_str = [str(entry) for entry in missing_entries]

    logging.warning("Missing entries for times: \n {}".format("\n".join(missing_str)))
    logging.info("Inserting blank entries for missing ones.")

    time_fields = {"HourUTC" : missing_entries, "HourDK": pd.Series(missing_entries).dt.tz_localize("utc").dt.tz_convert("Europe/Copenhagen").tolist()}
    data_incl_blank_entries = data.append(pd.DataFrame.from_dict(time_fields), ignore_index=True).sort_values(by="HourUTC")
    return data_incl_blank_entries


def handle_duplicate_entries(data: pd.DataFrame, drop=True):
    """
    Locates duplicate entries based on HourUTC and removes them if drop=True
    """
    duplicated_entries = data[data.duplicated(subset="HourUTC")]["HourUTC"].tolist()
    dup_str = [str(entry) for entry in duplicated_entries]
    logging.warning("Duplication detected for entries: {}".format("\n".join(dup_str)))
    if drop:
        return data.drop_duplicates(subset=["HourUTC"], keep="first")
    else:
        return data

def identify_suspected_errors(data: pd.DataFrame):
    numerical_columns = data.select_dtypes(include=np.number).columns
    for col in numerical_columns:
        log_anomalies(data, col)

def log_anomalies(data: pd.DataFrame, col):
    """
    Identifies values in col which are either negative of at least 4 std larger than the mean (my own arbitrary limit)
    and logs the records
    """
    avg = data[col].mean()
    std = data[col].std()
    anomaly_upper_limit = avg + (4 * std)
    anomalies = data.where((data[col] < 0) | (data[col] > anomaly_upper_limit)).dropna()

    if len(anomalies) > 0:
        logging.warning(f"Anomalies detected for field {col}: \n {anomalies.to_string}")


def interpolate_missing_values(data):
    """
    Interpolates missing values (I just use linear but it might be better to use polynomial and control the order from cmd)
    """
    numerical_columns = data.select_dtypes(include=np.number).columns
    data["HourUTC"] = pd.to_datetime(data["HourUTC"])
    data.set_index("HourUTC", inplace=True)
    for col in numerical_columns:
        tmp = data[col]
        tmp = tmp.interpolate()
        data[col] = tmp

    return data

