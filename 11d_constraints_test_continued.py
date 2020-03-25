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

# CHECK is a CONSTRAINT that allows for data validation of sql tables by
# checking if the data fulfils certain criteria.
# Like UNIQUE, it can be added during table creation while defining column
# datatypes or after. It can also be added to columns using ALTER TABLE.
# After CHECK has been set, an error will be returned if a value that does not
# meet the criteria is added or updated to the column. The CHECK criteria does
# not have to be the same column as the column whose datatype was just defined.
# However, it makes things confusing, and it still works as if you set CHECK
# on the column defined in the criteria anyway.
# If attempting to set CHECK on an existing table, and error will be returned
# if any existing value in the column does not meet the criteria.
command = '''
CREATE TABLE testtable(
    length DOUBLE
    ,breath DOUBLE
    ,area DOUBLE DEFAULT(length*breath)
);
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('testtable'))
print('check:')
print(df)
print()

command = '''
INSERT INTO testtable(length,breath)
VALUES
    (2, 3)
    ,(3, 4)
;
'''
cursor.execute(command)
df = pd.DataFrame(cz.select_from('testtable'))
print('check:')
print(df)
print()

cursor.close()
db.commit()
db.close()
print('commands executed.')
