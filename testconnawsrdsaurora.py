import mysql.connector

# create a connection to the MySQL database
cnx = mysql.connector.connect(user='admin', password='Password123',
                              host='colaberrydb.ctkwfn0vycpa.us-east-2.rds.amazonaws.com',
                              database='colaberryrdsdb')

# create a cursor object to execute SQL queries
cursor = cnx.cursor()

# execute a SQL query to select data from a table
query = "SELECT * FROM crop_data;"
cursor.execute(query)

# fetch all the rows in the result set
rows = cursor.fetchall()

# print the rows
for row in rows:
    print(row)

# execute a SQL query to select data from a table
query = "SELECT * FROM weather_data;"
cursor.execute(query)

# fetch all the rows in the result set
rows = cursor.fetchall()

# print the rows
for row in rows:
    print(row)

# close the cursor and connection
cursor.close()
cnx.close()
