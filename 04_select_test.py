# Demonstrates selection of data from database tables.
# Contrasts using sql sqlalchemy vs mariadb base connector.
import mariadb as mdb
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
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
con = engine.connect()

command = '''
SELECT *
FROM konosuba;
'''

# The mariadb connector returns column indexes.
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('mariadb base connector:')
print(df.head())
print()

# The sqlalchemy with pandas returns column names.
df = pd.read_sql_query(command, engine)
print('sqlalchemy:')
print(df.head())
print()

# Demonstrates getting column names.
# Show columns is more flexible, but for a quicker way to show all columns, use
# DESC konosuba;
command = '''
SHOW COLUMNS FROM konosuba;
'''

# mariadb and sqlalchemy are the same in this case. However, sqlalchemy has
# a shortcut method to return a list of tablenames using engine.table_names()
print('mariadb base connector:')
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

print('sqlalchemy:')
print(engine.table_names())
df = pd.DataFrame(con.execute(command))
print(df)
print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
