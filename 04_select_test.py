# Demonstrates selection of data from database tables.
# Contrasts using sql sqlalchemy vs mariadb base connector.
from sqlalchemy import create_engine
import mariadb as mdb
import json
import pandas as pd

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 300)

# sqlalchemy's engine strings are written in the form:
# dialect[+driver]://user:password@host/dbname[?key=value..]
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

command = '''
SELECT *
FROM konosuba
;
'''
# The mariadb connector returns column indexes.
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('mariadb base connector:')
print(df)
print()

# Demonstrates the DISTINCT keyword, which eliminates duplicate rows in SELECT.
# When used on multiple columns, the values in the row must be the same across
# all columns for the row to be dropped.
command = '''
SELECT DISTINCT race,sex
FROM konosuba
;
'''
# sqlalchemy with pandas returns column names.
df = pd.read_sql_query(command, engine)
print('sqlalchemy + distinct:')
print(df)
print()

# The LEFT(colname, n) keyword returns the leftmost n characters of each row in
# a column.
command = '''
SELECT name,class,LEFT(race, 4)
FROM konosuba
;
'''
df = pd.read_sql_query(command, engine)
print('select left:')
print(df)
print()

# Demonstrates renaming columns during select and the use of CASE.
# CASE may be used in statements such as SELECT, DELETE, and UPDATE or in
# clauses such as SELECT, ORDER BY, and HAVING.
# Using CASE in SELECT:
# CASE condition
#   WHEN value1 THEN new_value
#   ELSE value2
# END new_column_name
command = '''
SELECT id,name,class as 'job',race,sex,isekai,
    CASE (age >= 18)
        WHEN True THEN 'legal'
        ELSE 'jailbait'
    END age
FROM konosuba
;
'''
df = pd.read_sql_query(command, engine)
print('case select:')
print(df)
print()

# Demonstrates getting column names.
# Show columns is more flexible, but for a quicker way to show all columns, use
# DESC konosuba;
command = '''
SHOW COLUMNS FROM konosuba
;
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
