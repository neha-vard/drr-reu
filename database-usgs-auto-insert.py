import requests
import numpy as np
import io
import mysql.connector
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

# connect to database
db_config = {
    "user": "username",
    "password": "password",
    "host": "endpoint",
    "port": "3306",
    "database": "dbname",
}

connection = mysql.connector.connect(**db_config)
cursor = connection.cursor()

# get last_updated from database (SANTA CATALINA)
query = "SELECT datetime FROM last_updated WHERE location_id = %s"
cursor.execute(query, (50048565,))
last_updated = cursor.fetchone()[0]

# increase by 1 minute
start = last_updated + timedelta(minutes=1)

# make url request with startDT as this datetime
url = 'https://nwis.waterservices.usgs.gov/nwis/iv/?format=rdb&sites=50048565&startDT=' + start.strftime("%Y-%m-%dT%H:%M:%S") + '&parameterCd=00010,00065,00095,00300,00400&siteStatus=all'
print(url)
response = requests.get(url)

# insert rows into sensor_value database
if response.status_code == 200:
    tsv_data = response.content.decode('utf-8')
    tsv_io = io.StringIO(tsv_data)
    ndarray_data = np.genfromtxt(tsv_io, encoding='utf-8', delimiter='\t', dtype=None, comments='#')
    ndarray_data = ndarray_data[2:]
    ndarray_data = ndarray_data[:, 2:]
    ndarray_data = ndarray_data[:, ::2]
    print(ndarray_data[0])
    valid = False
    for row in ndarray_data:
        if len(row) == 6:
            valid = True
            sql = "INSERT INTO sensor_value (datetime, location_id, water_temperature, gage_height, specific_conductance, dissolved_oxygen, pH) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            values = (row[0] if row[0] != '' else None , 50048565, row[1] if row[1] != '' else None, row[2] if row[2] != '' else None, row[3] if row[3] != '' else None, row[4] if row[4] != '' else None, row[5] if row[5] != '' else None)
            cursor.execute(sql, values)
            
# update last_updated with last row's datetime
    if valid:
        query = "UPDATE last_updated SET datetime = %s WHERE location_id = %s"
        cursor.execute(query, (ndarray_data[-1][0], 50048565))

# commit changes
connection.commit()

# get last_updated from database (MARGARITA)
query = "SELECT datetime FROM last_updated WHERE location_id = %s"
cursor.execute(query, (50049620,))
last_updated = cursor.fetchone()[0]

# increase by 1 minute
start = last_updated + timedelta(minutes=1)

# make url request with startDT as this datetime
url = 'https://nwis.waterservices.usgs.gov/nwis/iv/?format=rdb&sites=50049620&startDT=' + start.strftime("%Y-%m-%dT%H:%M:%S") + '&parameterCd=00045,00060,00065&siteStatus=all'
print(url)
response = requests.get(url)

# insert rows into sensor_value database
if response.status_code == 200:
    tsv_data = response.content.decode('utf-8')
    tsv_io = io.StringIO(tsv_data)
    ndarray_data = np.genfromtxt(tsv_io, encoding='utf-8', delimiter='\t', dtype=None, comments='#')
    ndarray_data = ndarray_data[2:]
    ndarray_data = ndarray_data[:, 2:]
    ndarray_data = ndarray_data[:, ::2]
    print(ndarray_data[0])
    valid = False
    for row in ndarray_data:
        if len(row) == 4:
            valid = True
            sql = "INSERT INTO sensor_value (datetime, location_id, daily_precipitation, discharge, gage_height) VALUES (%s, %s, %s, %s, %s)"
            values = (row[0] if row[0] != '' else None , 50049620, row[1] if row[1] != '' else None, row[2] if row[2] != '' else None, row[3] if row[3] != '' else None)
            cursor.execute(sql, values)
            
# update last_updated with last row's datetime
    if valid:
        query = "UPDATE last_updated SET datetime = %s WHERE location_id = %s"
        cursor.execute(query, (ndarray_data[-1][0], 50049620))

# commit changes
connection.commit()

#close connection
connection.close()
