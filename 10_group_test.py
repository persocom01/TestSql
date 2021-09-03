# Demonstrates the group keyword.
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

command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')

# One reason to use GROUP BY is to apply an aggregate function to the group.
# Note also that although you may select non aggregate columns, SELECT * will
# only return the first row in the group.
# The aggregate functions available are:
# COUNT = len()
# SUM
# AVG
# MIN
# MAX
command = '''
SELECT *,count(*),SUM(age),AVG(age),MIN(age),MAX(age)
FROM konosuba
WHERE isekai = false
GROUP BY class
ORDER BY count(*) DESC
LIMIT 10;
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('group by:')
print(df)
print()

# The second reason to use GROUP BY is to use the HAVING keyword, which is
# basically WHERE but applied to aggregate group functions.
command = '''
SELECT class,count(*),SUM(age),AVG(age),MIN(age),MAX(age)
FROM konosuba
WHERE isekai = false
GROUP BY class
HAVING count(*) = 1
ORDER BY MAX(age) DESC
LIMIT 10;
'''
cursor.execute(command)
df = pd.DataFrame(cursor.fetchall())
print('having count > 1:')
print(df)
print()

cursor.close()
mdb_con.commit()
mdb_con.close()
print('commands executed.')
