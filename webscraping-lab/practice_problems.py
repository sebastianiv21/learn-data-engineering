import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup

"""
Try the following practice problems to test your understanding of the lab. Please note
that the solutions for the following are not shared. You are encouraged to use the 
discussion forums in case you need help.
1. Modify the code to extract Film, Year, and Rotten Tomatoes' Top 100 headers.
2. Restrict the results to only the top 25 entries.
3. Filter the output to print only the films released in the 2000s (year 2000 included).
"""
url = "https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films"
db_name = "Movies25.db"
table_name = "Top_25"
csv_path = "top_25_films.csv"
df = pd.DataFrame(columns=["Film", "Year", "Rotten Tomatoes' Top 100"])
count = 0
MAX_COUNT = 25

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
                "Film": cols[1].contents[0],
                "Year": int(cols[2].contents[0]),
                "Rotten Tomatoes' Top 100": cols[3].contents[0],
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            count += 1
    else:
        break

films_from_2000s = df[df["Year"] >= 2000]
print(films_from_2000s)

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
