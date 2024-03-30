import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

"""
You must declare a few entities at the beginning. For example, you know the required
URL, the CSV name for saving the record, the database name, and the table name for 
storing the record. You also know the entities to be saved. Additionally, since you
require only the top 50 results, you will require a loop counter initialized to 0.
"""
url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = "Movies.db"
table_name = "Top_50"
csv_path = "top_50_films.csv"
df = pd.DataFrame(columns=["Average Rank", "Film", "Year"])
count = 0
MAX_COUNT = 50

"""
To access the required information from the web page, you first need to load the
entire web page as an HTML document in python using the requests.get().text function
and then parse the text in the HTML format using BeautifulSoup to enable extraction 
of relevant information.
"""
html_page = requests.get(url).text
data = BeautifulSoup(html_page, "html.parser")

"""
You now need to write the loop to extract the appropriate information from the web page.
The rows of the table needed can be accessed using the find_all() function with the 
BeautifulSoup object using the statements below.
"""
TARGET_TABLE_INDEX = 0
tables = data.find_all("tbody")
rows = tables[TARGET_TABLE_INDEX].find_all("tr")

for row in rows:
    if count < MAX_COUNT:
        cols = row.find_all("td")
        if len(cols) != 0:
            data_dict = {
                "Average Rank": cols[0].contents[0],
                "Film": cols[1].contents[0],
                "Year": cols[2].contents[0],
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

print(df)

"""
The code functions as follows.

1. Iterate over the contents of the variable rows.
2. Check for the loop counter to restrict to 50 entries.
3. Extract all the td data objects in the row and save them to cols.
4. Check if the length of cols is 0, that is, if there is no data in a current row. 
This is important since, many timesm there are merged rows that are not apparent in 
the web page appearance.
5. Create a dictionary data_dict with the keys same as the columns of the dataframe
created for recording the output earlier and corresponding values from the first 
three headers of data.
6. Convert the dictionary to a dataframe and concatenate it with the existing one. 
This way, the data keeps getting appended to the dataframe with every iteration of the loop.
7. Increment the loop counter.
8. Once the counter hits 50, stop iterating over rows and break the loop.
"""

# After the dataframe has been created, you can save it to a CSV file using the following command:
df.to_csv(csv_path)

"""
To store the required data in a database, you first need to initialize a connection
to the database, save the dataframe as a table, and then close the connection. This can
be done using the following code:
"""
conn = sqlite3.connect(db_name)
df.to_sql(table_name, conn, if_exists="replace", index=False)
conn.close()

"""
The if_exists parameter can take any one of three possible values:
'fail': This denies the creation of a table if one with the same name exists in the database already.
'replace': This overwrites the existing table with the same name.
'append': This adds information to the existing table with the same name.

Keep the index parameter set to True only if the index of the data being sent holds
some informational value. Otherwise, keep it as False.
"""
