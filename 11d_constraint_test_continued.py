# Demonstrates table constraints. There are 6 possible constraints:
# NOT NULL
# UNIQUE
# PRIMARY KEY
# FOREIGN KEY
# CHECK
# DEFAULT
import json
from sqlalchemy import create_engine
import pleiades as ple

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}/{dbname}"
engine = create_engine(engine_string, pool_pre_ping=True, pool_recycle=300)
con = engine.connect()

cz = ple.CZ(engine)

# DEFAULT is used to define a default value for a column when no value is
# specified. Formulas or functions can be defined as default values. Default
# values are inserted into a column every time a row is added to it without
# specifying values for DEFAULT columns.
command = '''
CREATE TABLE testtable(
    length DOUBLE
    ,breath DOUBLE
    ,area DOUBLE DEFAULT(length * breath)
    ,timestamp DATETIME
);
'''
con.execute(command)

# Default can also be added via ALTER TABLE.
command = '''
ALTER TABLE testtable
MODIFY timestamp DATETIME DEFAULT NOW()
;
'''
con.execute(command)
print('default:')
print(cz.show_columns('testtable'))
print()

# Demonstrates how DEFAULT works when values are inserted into the table.
command = '''
INSERT INTO testtable(length, breath)
VALUES
    (2, 3)
    ,(3, 4)
;
'''
con.execute(command)
print('default at work:')
print(cz.select_from('testtable').ex())
print()

# Demonstrates dropping DEFAULT from a column.
# The official way to do it is:
# ALTER TABLE testtable
# ALTER COLUMN timestamp DROP DEFAULT
# ;
# However, when tested, for some reason it didn't work with the timestamp
# column. On testing, the datetime datatype seems to have something to do with
# it. To be safe, modifying the whole column works better.
command = '''
ALTER TABLE testtable
MODIFY timestamp DATETIME
;
'''
con.execute(command)
print('drop default:')
print(cz.show_columns('testtable'))
print()

command = '''
INSERT INTO testtable(length, breath)
VALUES
    (4, 5)
;
'''
con.execute(command)
print('no default timestamp:')
print(cz.select_from('testtable').ex())
print()

print(cz.del_tables('testtable'))
