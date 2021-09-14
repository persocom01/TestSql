# Demonstrates the like keyword.
from sqlalchemy import create_engine
import json
import mariadb as mdb
import pandas as pd

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# Official mdb connector.
mdb_con = mdb.connect(**cfg)
cursor = mdb_con.cursor()

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}/{dbname}"
engine = create_engine(engine_string, pool_pre_ping=True, pool_recycle=300)
con = engine.connect()

command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')

# The like syntax goes like this:
# % = wildcard. 1% fits include 1, 10 and 100.
# _ = any single character.
# In this case, all ages starting with 1.
command = '''
SELECT *
FROM konosuba
WHERE age LIKE '1%';
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

# In this case, all ages at least 3 digits long.
command = '''
SELECT *
FROM konosuba
WHERE age LIKE '___';
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

# When using sqlalchemy, % has to be escaped with %.
# So 1% becomes 1%%.
command = '''
SELECT *
FROM konosuba
WHERE name LIKE 'megu%%';
'''
df = pd.read_sql_query(command, engine)
print(df.head())
print()

cursor.close()
mdb_con.commit()
mdb_con.close()
print('commands executed.')
