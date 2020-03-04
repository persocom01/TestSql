# Demonstrates the group keyword.
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

# The main reason to use GROUP BY is to apply an aggregate function to the
# group. The aggregate functions available are:
# COUNT = len()
# SUM
# AVG
# MIN
# MAX
command = '''
SELECT *,count(*),SUM(age),AVG(age),MIN(age),MAX(age)
FROM konosuba
WHERE isekai = false
GROUP BY race
LIMIT 10;
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
print('order')
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print(df)
print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
