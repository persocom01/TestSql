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

    id int CHECK(id > 0 and id < 100),

    id2 int,
    CHECK(id2 > 2),

    id3 int,
    CONSTRAINT c_tt_id3
    CHECK(id3 > 3)
);
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('testtable'))
print('check:')
print(df)
print()

# Deleting a CONSTRAINT is easy so long as its name is known.
command = '''
ALTER TABLE testtable
DROP CONSTRAINT c_tt_id3
;
'''
cursor.execute(command)
# Demonstrates using ALTER TABLE to add CHECK CONSTRAINT to a table.
command = '''
ALTER TABLE testtable
ADD CONSTRAINT c_tt_id3
CHECK(id3 > 3)
;
'''
cursor.execute(command)

insert_value = 200
command = f'INSERT INTO testtable(id) VALUES({insert_value});'
try:
    cursor.execute(command)
except mdb.IntegrityError:
    print('mariadb.IntegrityError: CONSTRAINT `testtable.id` failed for `testdb`.`testtable`')

command = '''
DROP TABLES testtable;
'''
cursor.execute(command)

cursor.close()
db.commit()
db.close()
print('commands executed.')
