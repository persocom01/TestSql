# Demonstrates the where, and and or logical operators.
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

command = '''
SELECT name,class,race,age
FROM konosuba
WHERE age > 18;
'''
df = pd.read_sql_query(command, engine)
print('age > 18')
print(df.head())
print()

# Note that BOOLEAN columns don't need a value = 1 or value = True, but it does
# still work if written that way.
# Brackets are optional in this case, but makes the logic clearer, as AND has
# higher priority over OR.
command = '''
SELECT name,class,race,age
FROM konosuba
WHERE (NOT isekai AND race = 'human') OR class = 'adventurer';
'''
df = pd.read_sql_query(command, engine)
print('not isekai AND human OR class = adventurer')
print(df.head())
print()
