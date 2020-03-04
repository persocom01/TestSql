# Demonstrates the top and order keywords.
import json
import mariadb as mdb
import pandas as pd

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

# The TOP keyword does not exist in mariadb. LIMIT is used instead.
# In mysql, TOP is placed between select and the column name.
# TOP or LIMIT return the topmost valid rows in the table by index, not column
# values.
# TOP x PERCENT returns x percentage of rows instead of a fixed number.
# LIMIT x, y returns x rows from the top after skipping y rows.
# LIMIT x OFFSET y is alternative syntax.
command = '''
SELECT *
FROM konosuba
WHERE age LIKE '1%'
LIMIT 3;
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('limit:')
print(df)
print()

command = '''
SELECT *
FROM konosuba
WHERE age LIKE '1%'
LIMIT 3, 3;
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('limit with offset:')
print(df)
print()

# ORDER can be ASC (small to big) or DESC (big to small).
# Ordering by mutiple columns will order by the first column then the second.
command = '''
SELECT *
FROM konosuba
WHERE age LIKE '1%'
ORDER BY age DESC,name ASC
LIMIT 5;
'''
print('order:')
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

# Demonstrates custom ordering syntax:
# (CASE col
# WHEN row_value1 THEN rank1
# WHEN row_value2 THEN rank2
# ELSE rank3 END)
command = '''
SELECT *
FROM konosuba
WHERE sex = 'F'
ORDER BY
(CASE race
    WHEN 'god' THEN 1
    WHEN 'human' THEN 2
    WHEN 'lich' THEN 3
    ELSE 100
END)
LIMIT 10;
'''
print('custom order:')
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

# Without a column name argument case has to take column names as conditions.
command = '''
SELECT *
FROM konosuba
WHERE sex = 'F'
ORDER BY
(CASE
    WHEN age IS NULL THEN name
    ELSE age
END)
LIMIT 10;
'''
print('custom order:')
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
