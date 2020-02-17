# Demonstrates loading of files into a database.
import mariadb as mdb
import json
import glob
import pandas as pd
import pleiades as ple
from pathlib import Path

# SQL is case insensitive.
dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    server_config = json.load(f)
db = mdb.connect(**server_config)
cursor = db.cursor()

# Select database to be used. Can also be set in server_config instead.
command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')


cz = ple.CZ(cursor)

print(cz.csv_table('./data/client.csv', 'id'))

command = 'DROP client'
print('command executed')

# Create tables in database.
# for file in glob.glob('./data/*.csv'):
#     print(Path(file).stem)
#     df = pd.read_csv(file, nrows=100)
#     print(df)
#     print(df.columns, df.dtypes)

# for file in glob.glob('./data/*.csv'):
#     print(file)
#
# print(os.listdir('./data'))

# LOAD DATA LOCAL INFILE 'source.csv' INTO target_db.target_table FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
