from sqlalchemy import create_engine
import pandas as pd
import pleiades as ple

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 300)

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

cz = ple.CZ(engine, type='alchemy')

# INNER JOIN is most often used to connect multiple tables together. All
# elements common to those tables are displayed, and the rest discarded.
# Often used with SELECT agg(col) and GROUP BY col.
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
