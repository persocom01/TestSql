# Demonstrates selection of data from database tables.
import mariadb as mdb
import json
import pandas as pd
import pleiades as ple

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 300)

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    server_config = json.load(f)
db = mdb.connect(**server_config)
cursor = db.cursor()

command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')

command = '''
SELECT * FROM konosuba
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
