# Code for ETL operations on Country-GDP data

# Importing the required libraries
from typing import Dict, List
import requests
import sqlite3
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from datetime import datetime


def log_progress(message: str) -> None:
    """This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing"""
    date_format = "%Y-%h-%d-%H:%M:%S"
    now: str = datetime.now().strftime(date_format)

    with open(LOG_FILE, "a") as log:
        log.write(f"{now} : {message}\n")


def extract(url: str, table_attribs: List[str]) -> pd.DataFrame:
    """This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing."""
    html_page = requests.get(url).text
    soup = BeautifulSoup(html_page, "html.parser")
    df = pd.DataFrame(columns=table_attribs)

    tables = soup.find_all("tbody")
    TARGET_TABLE_INDEX = 0
    rows = tables[TARGET_TABLE_INDEX].find_all("tr")

    for row in rows:
        cols = row.find_all("td")
        if len(cols) != 0:
            bank_name = cols[1].find_all("a")[1].contents[0]
            mc_usd = float(cols[2].contents[0][:-1])
            data_dict = {table_attribs[0]: bank_name, table_attribs[1]: mc_usd}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)

    return df


def transform(df: pd.DataFrame, csv_path: str) -> pd.DataFrame:
    """This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies"""
    exchange_rate: pd.DataFrame = pd.read_csv(csv_path)
    exchange_rate_dict: Dict[str, float] = exchange_rate.set_index(
        exchange_rate.columns[0]
    ).to_dict()[exchange_rate.columns[1]]

    df["MC_GBP_Billion"] = [
        np.round(x * exchange_rate_dict["GBP"], 2) for x in df["MC_USD_Billion"]
    ]
    df["MC_EUR_Billion"] = [
        np.round(x * exchange_rate_dict["EUR"], 2) for x in df["MC_USD_Billion"]
    ]
    df["MC_INR_Billion"] = [
        np.round(x * exchange_rate_dict["INR"], 2) for x in df["MC_USD_Billion"]
    ]

    return df


def load_to_csv(df: pd.DataFrame, output_path: str) -> None:
    """This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing."""
    df.to_csv(output_path, index=True)


def load_to_db(
    df: pd.DataFrame, sql_connection: sqlite3.Connection, table_name: str
) -> None:
    """This function saves the final data frame to a database
    table with the provided name. Function returns nothing."""
    df.to_sql(table_name, sql_connection, if_exists="replace", index=False)


def run_query(query_statement: str, sql_connection: sqlite3.Connection) -> None:
    """This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing."""
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)


""" Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function."""
DATA_URL = "https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks"
EXTRACTION_TABLE_ATTRIBUTES = ["Name", "MC_USD_Billion"]
EXCHANGE_RATE_CSV_PATH = "./exchange_rate.csv"
FINAL_TABLE_ATTRIBUTES = [
    "Name",
    "MC_USD_Billion",
    "MC_GBP_Billion",
    "MC_EUR_Billion",
    "MC_INR_Billion",
]
OUTPUT_CSV_PATH = "./Largest_banks_data.csv"
DB_NAME = "Banks.db"
TABLE_NAME = "Largest_banks"
LOG_FILE = "code_log.txt"
log_progress("Preliminaries complete. Initiating ETL process")

extracted_data: pd.DataFrame = extract(DATA_URL, EXTRACTION_TABLE_ATTRIBUTES)
print(extracted_data)
log_progress("Data extraction complete. Initiating Transformation process")

transformed_data: pd.DataFrame = transform(extracted_data, EXCHANGE_RATE_CSV_PATH)
print(transformed_data)
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(transformed_data, OUTPUT_CSV_PATH)
log_progress("Data saved to CSV file")

db_conn = sqlite3.connect(DB_NAME)
log_progress("SQL Connection initiated")

load_to_db(transformed_data, db_conn, TABLE_NAME)
log_progress("Data loaded to Database as a table, Executing queries")

all_data_query = f"SELECT * FROM {TABLE_NAME}"
run_query(all_data_query, db_conn)

avg_mc_gbp_query = f"SELECT AVG(MC_GBP_Billion) FROM {TABLE_NAME}"
run_query(avg_mc_gbp_query, db_conn)

top_5_query = f"SELECT Name FROM {TABLE_NAME} LIMIT 5"
run_query(top_5_query, db_conn)
log_progress("Process Complete")

db_conn.close()
log_progress("Server Connection closed")
