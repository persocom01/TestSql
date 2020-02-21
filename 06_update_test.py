# Demonstrates the where, and and or logical operators.
import pandas as pd
from sqlalchemy import create_engine
import pleiades as ple

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

# Demonstrates the basic update datement.
# If where is not specified all rows will be updated with the same value.
command = '''
UPDATE konosuba
SET age = 17000
WHERE id = 1;
'''
con.execute(command)

command = '''
SELECT * FROM konosuba
'''
df = pd.read_sql_query(command, engine)
print('before')
print(df)
print()

cz = ple.CZ()

# Demonstrates mass updating of a table from a file.
command = cz.csv_insert('./data/konosuba.csv', updatekey='id', printc=True)
print('command:')
print(command)
print()
con.execute(command)

command = '''
SELECT * FROM konosuba
'''
df = pd.read_sql_query(command, engine)
print('after')
print(df)
print()
