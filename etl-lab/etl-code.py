"""
In this lab, you will extract data from CSV, JSON, and XML formats. 
First, you need to import the appropriate Python libraries to use the relevant functions.

The xml library can be used to parse the information from an .xml file format. 
The .csv and .json file formats can be read using the pandas library. 
You will use the pandas library to create a data frame format that will store 
the extracted data from any file.

To call the correct function for data extraction, you need to access the file format
information. For this access, you can use the glob library.

To log the information correctly, you need the date and time information at the point
of logging. For this information, you require the datetime package.

While glob, xml, and datetime are inbuilt features of Python, you need to install the
pandas library to your IDE.
"""
import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime

log_file = "log_file.txt"
target_file = "transformed_data.csv"


def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


"""
It requires an extra argument 'lines=True' to enable
the function to read the file as a JSON object
on line to line basis as follows.
"""


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


"""
To extract from an XML file, you need first to parse the data
from the file using the ElementTree function. You can then extract 
relevant information from this data and append it to a pandas dataframe as follows.

Note: You must know the headers of the extracted data to write this function. 
In this data, you extract "name", "height", and "weight" headers for different persons.
"""


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat(
            [
                dataframe,
                pd.DataFrame([{"name": name, "height": height, "weight": weight}]),
            ],
            ignore_index=True,
        )
        return dataframe


def extract():
    # create an empty dataframe to hold the extracted data
    extracted_data = pd.DataFrame(columns=["name", "height", "weight"])

    # process all csv files
    for csvfile in glob.glob("*.csv"):
        extracted_data = pd.concat(
            [extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True
        )

    # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = pd.concat(
            [extracted_data, pd.DataFrame(extract_from_json(jsonfile))],
            ignore_index=True,
        )

    # process all xml files
    for xmlfile in glob.glob("*.xml"):
        extracted_data = pd.concat(
            [extracted_data, pd.DataFrame(extract_from_xml(xmlfile))],
            ignore_index=True,
        )

    return extracted_data


"""
The height in the extracted data is in inches, and the weight is in pounds. 
However, for your application, the height is required to be in meters, and the weight
is required to be in kilograms, rounded to two decimal places. Therefore, you need to
write the function to perform the unit conversion for the two parameters.

The name of this function will be transform(), and it will receive the extracted 
dataframe as the input. Since the dataframe is in the form of a dictionary with three
keys, "name", "height", and "weight", each of them having a list of values, you can 
apply the transform function on the entire list at one go. 
"""


def transform(data):
    """
    Convert inches to meters and round off to two decimals
    1 inch is 0.0254 meters
    """
    data["height"] = round(data.height * 0.0254, 2)

    """
    Convert pounds to kilograms and round off to two decimals
    1 pound is 0.45359237 kilograms
    """
    data["weight"] = round(data.weight * 0.45359237, 2)

    return data


"""
You need to load the transformed data to a CSV file that you can use to load to a
database as per requirement. 
"""


def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


"""
Finally, you need to implement the logging operation to record the progress of the
different operations. For this operation, you need to record a message, along with 
its timestamp, in the log_file.

To record the message, you need to implement a function log_progress() that accepts
the log message as the argument. The function captures the current date and time using
the datetime function from the datetime library. The use of this function requires the
definition of a date-time format, and you need to convert the timestamp to a string
format using the strftime attribute.
"""


def log_progress(message):
    # Year-Monthname-Day-Hour:Minute:Second
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as logger:
        logger.write(timestamp + "\t" + message + "\n")


"""
Now, test the functions you have developed so far and log your progress along the way. 
Insert the following lines into your code to complete the process. Note the comments 
on every step of the code. 
"""
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
load_data(target_file, transformed_data)

# Log the completion of the Load process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")
