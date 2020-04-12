import pandas as pd
import numpy as np
import seaborn as sns
import scipy.stats as stats
import matplotlib.pyplot as plt

import pandas.io.sql
import pyodbc

import xlrd

import datetime


#--importing my data#--
df=pd.read_excel("C:/ORIGIN.xlsx",header=None)

df.dtypes 
#"outcome is 
#0             object
#1     datetime64[ns]
#2              int64
#3             object
#4             object
#5             object
#6             object
#7             object
#8             object
#9             object
#10             int64
#11    datetime64[ns]
#12            object
#13    datetime64[ns]
#14            object
#15             int64
#16           float64
#17    datetime64[ns]
#18            object
#19            object
#dtype: object

print(df.head(1))
#0                   1   2           3        4   \ --> number are colum headers
#0  INC1123313 2017-09-01 07:56:00   1  High  Network    --> these rows are sample of data for information purpose 
#5              6           7                8    9   \
#0  New-York  Supermarket  Closed Closed  Network: KO  South  NaN   
#      10(second)                  11   12                  13  \
#0  11053 2017-09-06 14:49:00  NaN 2017-09-01 10:57:00   
#                                   14  15   16                  17  \
#0  luc tiber (ltiber@mymail.here)   2  0.0 2017-09-06 14:49:00   
#18              19  
# 0  Not populated but ok    Not populated but ok  

df.shape
nbligne=(len(df.index))
print(nbligne, df.shape)

#51292 (51292, 20)

df.to_excel("C:/DESTINATION.xlsx", index=False)

# Open the workbook and define the worksheet
book = xlrd.open_workbook("C:/DESTINATION.xlsx")
sheet = book.sheet_by_name("Sheet1")

from __future__ import print_function

import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'DB1'

TABLES = {}
TABLES['history'] = (
    "CREATE TABLE IF NOT EXISTS `history` ("
    "ID VARCHAR(10) NOT NULL PRIMARY KEY,"
    "creation_date TIMESTAMP,"
    "priority REAL,"
    "priority_type TEXT,"
    "element_config TEXT,"
    "site TEXT,"
    "status TEXT,"
    "description TEXT,"
    "grpe_affectation TEXT,"
    "assigned_to TEXT,"
    "duraton_sec REAL,"
    "closed_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',"
    "closed_by TEXT,"
    "resolution_date TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',"
    "solved_by TEXT,"
    "num_iteration REAL,"
    "duration_for_company INTEGER,"
    "last_update TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',"
    "city TEXT,"
    "business_line TEXT"
    ") ENGINE=InnoDB;")
    
    
    #try the connexion and get the ok or appropriate error message
try:
    cnx = mysql.connector.connect(user='root', password='mypwd',
                              host='127.0.0.1',
                              database=DB_NAME)
    if cnx.is_connected():
        db_Info = cnx.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursr = cnx.cursor()
        cursr.execute("select database();")
        record = cursr.fetchone()
        print("You're connected to database: ", DB_NAME)
        
except mysql.connector.Error as e:
    if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print('Somethign is wrong with username or password')
    elif e.errno == errorcode.ER_BAD_DB_ERROR:
        print('Database does not exist')
    else:
        print(e)
        
def create_database(cursr):
    try:
        cursr.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)
        
       
for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursr.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

query = """
INSERT IGNORE INTO DB1.history (
    ID,
    creation_date,
    priority, 
    priority_type,
    element_config,
    site,
    status, 
    description,
    grpe_affectation,
    assigned_to,
    duraton_sec,
    closed_date,
    closed_by,
    resolution_date,
    solved_by,
    num_iteration,
    duration_for_company,
    last_update,
    city,
    business_line
) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

cursr.execute("SELECT count(*) FROM "+ DB_NAME+"."+table_name)

print(cursr.execute)
before_import = cursr.fetchone()

for r in range(1, sheet.nrows-1):
    ID = sheet.cell(r,0).value
    #creation_date = sheet.cell(r,1).value
    creation_date=datetime.datetime(*xlrd.xldate_as_tuple(sheet.cell(r,1).value, book.datemode))
    priority = sheet.cell(r,2).value
    priority_type = sheet.cell(r,3).value
    element_config = sheet.cell(r,4).value
    site = sheet.cell(r,5).value
    status = sheet.cell(r,6).value
    description = sheet.cell(r,7).value
    grpe_affectation = sheet.cell(r,8).value
    assigned_to = sheet.cell(r,9).value
    duraton_sec = sheet.cell(r,10).value
    #closed_date = sheet.cell(r,11).value
    closed_date = datetime.datetime(*(xlrd.xldate_as_tuple(sheet.cell(r,11).value, book.datemode)))#.strftime('%d -%m -%Y %H:%M:%S')
    closed_by = sheet.cell(r,12).value
    #resolution_date = sheet.cell(r,13).value
    resolution_date = datetime.datetime(*xlrd.xldate_as_tuple(sheet.cell(r,13).value, book.datemode))
    solved_by = sheet.cell(r,14).value
    num_iteration = sheet.cell(r,15).value
    duration_for_company = sheet.cell(r,16).value
    #last_update = sheet.cell(r,17).value
    last_update = datetime.datetime(*xlrd.xldate_as_tuple(sheet.cell(r,17).value, book.datemode))
    city = sheet.cell(r,18).value
    business_line = sheet.cell(r,19).value

    # Assign values from each row
    values = (ID, creation_date, priority, priority_type, 
              element_config, site, status, description, grpe_affectation, assigned_to, 
              duraton_sec, closed_date, closed_by, resolution_date, solved_by, num_iteration, 
              duration_for_company, last_update, city, business_line)

    # Execute sql Query
    cursr.execute(query, values)

# Commit the transaction
conn.commit()

# If you want to check if all rows are imported
cursr.execute("SELECT count(*) FROM DB1.history")
result = cursor.fetchone()

print((result[0] - before_import[0]) == len(data.index))  # should be True

# Close the database connection
DB_NAME.commit()
conn.close()
