# Demonstrates loading of files into a database.
import mariadb as mdb
import json
import glob
import os

# SQL is case insensitive.
dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    server_config = json.load(f)
db = mdb.connect(**server_config)
cursor = db.cursor()

#
command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')

# for file in glob.glob('./data/*.csv'):
#     print(file)
#
# print(os.listdir('./data'))

# LOAD DATA LOCAL INFILE 'source.csv' INTO target_db.target_table FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
