# Demonstrates the like keyword.
from sqlalchemy import create_engine
import json
import mariadb as mdb
import pleiades as ple
import pandas as pd

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

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

cz = ple.CZ(engine=engine)

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
