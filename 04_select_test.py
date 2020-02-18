# Demonstrates selection of data from database tables.
# Contrasts using sql sqlalchemy vs mariadb base connector.
import mariadb as mdb
import json
import pandas as pd
from sqlalchemy import create_engine
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

# sqlalchemy's engine strings are written in the form:
# dialect[+driver]://user:password@host/dbname[?key=value..]
engine = create_engine('mysql+pymysql://root:@localhost/testDB')

command = '''
SELECT * FROM konosuba
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('mariadb base connector:')
print(df.head())
print()

df = pd.read_sql_query(command, engine)
print('sqlalchemy:')
print(df.head())
print()

command = '''
DESC konosuba
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
