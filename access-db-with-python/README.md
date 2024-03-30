# Accessing Databases using Python Script

Using databases is an important and useful method of sharing information. To
preserve repeated storage of the same files containing the required data, it is
a good practice to save the said data on a database on a server and access the
required subset of information using database management systems.

In this lab, you'll learn how to create a database, load data from a CSV file as
a table, and then run queries on the data using Python.

## Objectives

In this lab you'll learn how to:

- Create a database using Python
- Load the data from a CSV file as a table to the database
- Run basic "queries" on the database to access the information

## Scenario

Consider a dataset of employee records that is available with an HR team in a
CSV file. As a Data Engineer, you are required to create the database called
STAFF and load the contents of the CSV file as a table called INSTRUCTORS. The
headers of the available data are :

| Header | Description              |
| ------ | ------------------------ |
| ID     | Employee ID              |
| FNAME  | First Name               |
| LNAME  | Last Name                |
| CITY   | City of residence        |
| CCODE  | Country code (2 letters) |
