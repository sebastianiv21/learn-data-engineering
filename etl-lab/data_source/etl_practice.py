import pandas as pd
from datetime import datetime
import xml.etree.ElementTree as ET
import glob

log_file = "log_file.txt"
target_file = "transformed_data.csv"

dataframe_columns = ["car_model", "year_of_manufacture", "price", "fuel"]


def extract_from_csv(file_to_process):
    return pd.read_csv(file_to_process)


def extract_from_json(file_to_process):
    return pd.read_json(file_to_process, lines=True)


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=dataframe_columns)
    tree = ET.parse(file_to_process)
    root = tree.getroot()

    for car in root:
        car_model = car.find("car_model").text
        year_of_manufacture = int(car.find("year_of_manufacture").text)
        price = float(car.find("price").text)
        fuel = car.find("fuel").text

        dataframe = pd.concat(
            [
                dataframe,
                pd.DataFrame(
                    [
                        {
                            "car_model": car_model,
                            "year_of_manufacture": year_of_manufacture,
                            "price": price,
                            "fuel": fuel,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )

        return dataframe


def extract():
    extracted_data = pd.DataFrame(columns=dataframe_columns)

    for file in glob.glob("*.csv"):
        extracted_data = pd.concat(
            [extracted_data, pd.DataFrame(extract_from_csv(file))], ignore_index=True
        )

    for file in glob.glob(("*.json")):
        extracted_data = pd.concat(
            [extracted_data, pd.DataFrame(extract_from_json(file))], ignore_index=True
        )

    for file in glob.glob("*.xml"):
        extracted_data = pd.concat(
            [extracted_data, pd.DataFrame(extract_from_xml(file))], ignore_index=True
        )

    return extracted_data


def transform(data):
    data["price"] = round(data.price, 2)
    return data


def load(target_file, data):
    data.to_csv(target_file)


def log_progress(message):
    # Year-Monthname-Day-Hour:Minute:Second
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as logger:
        logger.write(timestamp + "\t" + message + "\n")


# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)
print("Transformed Data:\n", transformed_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Load process
log_progress("Load phase Started")
load(target_file, transformed_data)

# Log the completion of the Load process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
