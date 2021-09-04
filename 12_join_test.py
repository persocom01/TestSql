# Demonstrates joins.
import json
from sqlalchemy import create_engine
import pandas as pd
import pleiades as ple

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 300)

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}/{dbname}"
engine = create_engine(engine_string)
con = engine.connect()

cz = ple.CZ(engine)

# INNER JOIN is most often used to connect multiple tables together. All
# elements common to those tables are displayed, and the rest discarded.
# Often used with SELECT agg(col) and GROUP BY col. JOIN == INNER JOIN by
# default.
command = '''
SELECT *
FROM konosuba k
INNER JOIN quest_map qm ON qm.char_id = k.id
INNER JOIN quest q ON q.id = qm.quest_id
;
'''
df = pd.read_sql_query(command, engine)
print(df)
print()

# LEFT JOIN is similar to INNER JOIN, but the elements in the parent from table
# that are not present in the daughter tables are retained.
command = '''
SELECT *
FROM konosuba AS k
LEFT JOIN quest_map AS qm ON qm.char_id = k.id
LEFT JOIN quest q ON q.id = qm.quest_id
;
'''
df = pd.read_sql_query(command, engine)
print(df)
print()

# CROSS JOIN returns all possible combinations of rows from table1 and table2.
# The result will always have n rows, n being table1_rows x table2_rows.
command = '''
SELECT *
FROM konosuba AS k
CROSS JOIN quest
;
'''
df = pd.read_sql_query(command, engine)
print(df)
print()
