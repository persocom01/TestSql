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

# CHANGE, MODIFY and RENAME are used to modify columns in an existing table.
# CHANGE is the stringest command, which allows renaming, changing dtype, and
# reordering the column.
# MODIFY does what change does without renaming or reordering.
# RENAME only renames the column.
# The CHANGE family will not return an error even if the result is the same as
# the input.
# NOT NULL prevents null values from being entered into a column. Primary keys
# are often NOT NULL by default, but it may be safer to set it anyway. Setting
# a column to NOT NULL requires altering the entire column dtype, which can be
# troublesome. It appears that existing NULL values will be converted to 0 for
# numbers, '' for strings if they are not imputed with non-NULL values before
# changing the column datatype. This may not be true if sql is set to strict
# mode. However, it is recommended to impute NULLS manurally before adding
# the NOT NULLS constraint to a column using:
# UPDATE konosuba
# SET age = 0
# WHERE age is NULL;
column = 'age'
tablename = 'konosuba'
for i, col in enumerate(cz.show_columns(tablename)):
    if col[0] == column:
        col_index = i
dtype = cz.show_columns(tablename)[col_index][1]
command = f'ALTER TABLE {tablename} MODIFY {column} {dtype} NULL;'
cursor.execute(command)
df = pd.DataFrame(cz.show_columns(tablename))
print(df)
print()
df = pd.DataFrame(cz.select_from(tablename).ex())
print(df)
print()

# A primary key is a column or set of columns that uniquely identifies each row
# in a table. It is typically the first column in the table, but can be
# a set of multiple columns in cases of mapping tables. Such keys are known as
# composite primary keys. There is some debate whether each table should just
# have a single column primary key.
command = '''
ALTER TABLE quest_map
ADD PRIMARY KEY(quest_id,char_id);
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('quest_map'))
print(df)
print()

command = '''
ALTER TABLE quest_map
DROP PRIMARY KEY;
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('quest_map'))
print(df)
print()

column = 'id'
tablename = 'konosuba'
for i, col in enumerate(cz.show_columns(tablename)):
    if col[0] == column:
        col_index = i
dtype = cz.show_columns(tablename)[col_index][1]
command = f'ALTER TABLE {tablename} MODIFY {column} {dtype} NOT NULL AUTO_INCREMENT;'
cursor.execute(command)
df = pd.DataFrame(cz.show_columns(tablename))
print(df)
print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
