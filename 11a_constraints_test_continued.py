# Demonstrates table constraints. There are 6 possible constraints:
# NOT NULL
# UNIQUE
# PRIMARY KEY
# FOREIGN KEY
# CHECK
# DEFAULT
import json
import mariadb as mdb
import pandas as pd
import pleiades as ple

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

cz = ple.CZ(cursor)

# PRIMARY KEY is a column or set of columns that uniquely identifies each row
# in a table. It is typically the first column in the table, but can be
# a set of multiple columns in cases of mapping tables. Such keys are known as
# composite primary keys. There is some debate whether each table should just
# have a single column primary key.
# PRIMARY KEY can be set at table creation  or after in the same manner as
# UNIQUE. However, in the case of PRIMARY KEY, where is no need to bother with
# custom CONSTRAINT names since PRIMARY KEY can easily be removed without
# knowing its CONSTRAINT name.
command = '''
ALTER TABLE quest_map
ADD PRIMARY KEY(quest_id,char_id);
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('quest_map'))
print('primary key:')
print(df)
print()

# Demonstrates dropping of PRIMARY KEY.
command = '''
ALTER TABLE quest_map
DROP PRIMARY KEY;
'''
cursor.execute(command)
# df = pd.DataFrame(cz.show_columns('quest_map'))
# print(df)
# print()

# AUTO_INCREMENT allows one to insert VALUES into the table without defining
# the value of a column, which is auto incremented by 1 each time. There can
# only be one AUTO_INCREMENT column and it must either be the PRIMARY or
# FOREIGN KEY. AUTO_INCREMENT is used on mysql and mariadb. oracle, PostgreSQL
# and sql server use variations of the IDENTITY instead.
# To remove AUTO_INCREMENT, you need to first use ALTER TABLE to DROP PRIMARY
# KEY before using ALTER TABLE CHANGE or MODIFY on the datatype.
column = 'id'
tablename = 'konosuba'
for i, col in enumerate(cz.show_columns(tablename)):
    if col[0] == column:
        col_index = i
dtype = cz.show_columns(tablename)[col_index][1]
command = f'ALTER TABLE {tablename} MODIFY {column} {dtype} NOT NULL AUTO_INCREMENT;'
cursor.execute(command)
df = pd.DataFrame(cz.show_columns(tablename))
print('auto_increment:')
print(df)
print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
