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

# UNIQUE is similar to PRIMARY KEY in that it only allows unique values in a
# column. However, UNIQUE allows a column to have null values, unlike PRIMARY
# KEY. Furthermore, in oracle and sql server, UNIQUE cannot have multiple null
# values. UNIQUE columns can be set during table creation or added as column
# constraints afterward. The CONSTRAINT keyword can be used to set a custom
# constraint name during table creation or when added as a column property.
# Without which,the database will automatically assign it a generated name.
# This may be fine in most cases, but it would be troublesome if there is a
# need to drop constraint in future.
# CONSTRAINT cannot be used when adding a column using ALTER TABLE. However,
# the contraint with a custom name can simply be added to the column after it
# is created.
command = '''
ALTER TABLE konosuba
ADD email VARCHAR(50) UNIQUE;
'''
cursor.execute(command)
# Demonstrates using CONSTRAINT to add UNIQUE to the name column. It is
# possible to add CONSTRAINT to a set of columns instead of just one. This
# functions in a manner similar to  composite primary keys.
command = '''
ALTER TABLE konosuba
ADD CONSTRAINT u_k_name
UNIQUE(name);
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('konosuba'))
print('unique:')
print(df)
print()

# Deleting a CONSTRAINT is easy so long as its name is known.
command = '''
ALTER TABLE konosuba
DROP CONSTRAINT u_k_name;
'''
cursor.execute(command)
command = '''
ALTER TABLE konosuba
DROP COLUMN email;
'''
cursor.execute(command)

# CHANGE, MODIFY and RENAME are used to modify columns in an existing table
# while ALTER TABLE is used.
# CHANGE is the strongest command, which allows renaming, changing dtype, and
# reordering the column.
# MODIFY does what change does without renaming or reordering.
# RENAME only renames the column.
# The CHANGE family will not return an error even if the result is the same as
# the input.
# NOT NULL prevents null values from being entered into a column. PRIMARY KEY
# is often NOT NULL by default, but it may be safer to set it anyway. Setting
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
command = f'ALTER TABLE {tablename} MODIFY {column} {dtype} NOT NULL;'
# cursor.execute(command)
# df = pd.DataFrame(cz.show_columns(tablename))
# print('not null:')
# print(df)
# print()
# df = pd.DataFrame(cz.select_from(tablename).ex())
# print(df)
# print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
