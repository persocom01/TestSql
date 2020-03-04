# Demonstrates table constraints.
import json
import mariadb as mdb
import pandas as pd

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

cursor.close()
db.commit()
db.close()
print('commands executed.')
