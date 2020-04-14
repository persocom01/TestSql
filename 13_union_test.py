from sqlalchemy import create_engine
import pandas as pd
import pleiades as ple

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 300)

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

cz = ple.CZ(engine, type='alchemy')

# UNION is used to vertically join two or more SELECT FROM statements together
# with no duplicates. The result will always use the column names from the
# topmost SELECT FROM statement. While UNION can be used to join different
# types of columns together, the result wouldn't make any sense. It is normally
# used to join results with the same column names.
# The statements joined together must have the same or convertable datatype,
# and have the same number of columns arranged in the same order. (the last
# being necessary for the result to make sense, it may still work if in
# incorrect order)
# UNION ALL can be used to include duplicates.
# ORDER BY can be used after the last statement to order the results.
command = '''
SELECT
    id
    ,name
FROM konosuba

UNION

SELECT
    id
    ,reward
FROM quest

UNION

SELECT
    quest_id
    ,char_id
FROM quest_map
;
'''
df = pd.read_sql_query(command, engine)
print('union:')
print(df)
print()
