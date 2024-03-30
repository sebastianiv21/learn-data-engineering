import sqlite3
import pandas as pd

conn = sqlite3.connect("STAFF.db")

"""
To create a table in the database, you first need to have the attributes of the
required table. Attributes are columns of the table. Along with their names, the
knowledge of their data types are also required. The attributes for the required 
tables in this lab were shared in the Lab Scenario.

Add the following statements to db_code.py to feed the required table name and 
attribute details for the table.
"""
table_name = "INSTRUCTOR"
atribute_list = ["ID", "FNAME", "LNAME", "CITY", "CCODE"]

"""
Now, to read the CSV using Pandas, you use the read_csv() function. Since this CSV
does not contain headers, you can use the keys of the attribute_dict dictionary as
a list to assign headers to the data. For this, add the commands below to db_code.py.
"""
file_path = "INSTRUCTOR.csv"
df = pd.read_csv(file_path, names=atribute_list)

"""
The pandas library provides easy loading of its dataframes directly to the database. 
For this, you may use the to_sql() method of the dataframe object.

However, while you load the data for creating the table, you need to be careful if a
table with the same name already exists in the database. If so, and it isn't required
anymore, the tables should be replaced with the one you are loading here. You may also
need to append some information to an existing table. For this purpose, to_sql() 
function uses the argument if_exists. 

As you need to create a fresh table upon execution, add the following commands to the 
code. The print command is optional, but helps identify the completion of the steps of 
code until this point.
"""
df.to_sql(table_name, conn, if_exists="replace", index=False)
print("Table is ready")


# View all the data in the table
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# View only the FNAME column
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# View the total number of rows in the table
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


# Append data to the table
data_dict = {
    "ID": [100],
    "FNAME": ["John"],
    "LNAME": ["Doe"],
    "CITY": ["Paris"],
    "CCODE": ["FR"],
}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists="append", index=False)
print("Data appended successfully")

# View the total number of rows in the updated table
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

conn.close()
