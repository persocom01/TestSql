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
from sqlalchemy import create_engine
import pleiades as ple

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# Official mdb connector.
mdb_con = mdb.connect(**cfg)
cursor = mdb_con.cursor()

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}/{dbname}"
engine = create_engine(engine_string, pool_pre_ping=True, pool_recycle=300)
con = engine.connect()

command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')

cz = ple.CZ(engine)

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

    id INT CHECK(id > 0 and id < 100)

    ,id2 INT
    ,CHECK(id2 > 2)

    ,id3 INT
    ,CONSTRAINT c_tt_id3
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

# Test criteria here.
insert_value = 200
colname = 'id'
command = f'INSERT INTO testtable({colname}) VALUES({insert_value});'
print(f'inserting value {insert_value} in column {colname}:')
try:
    cursor.execute(command)
    print('value successfully inserted.')
    print()
except mdb.IntegrityError:
    print('mariadb.IntegrityError: CONSTRAINT `testtable.id` failed for `testdb`.`testtable`')
    print()

print(cz.del_tables('testtable'))
print()

cursor.close()
mdb_con.commit()
mdb_con.close()
print('commands executed.')
