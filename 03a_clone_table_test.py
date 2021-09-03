# Demonstrates cloning a table.
import json
import mariadb as mdb
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
engine = create_engine(engine_string)
con = engine.connect()

cz = ple.CZ(engine)

# Select database to be used. Can also be set in server.cfg instead.
command = f'USE {dbname};'
print(cz.use_db(dbname, printable=True))
try:
    cursor.execute(command)
    print(f'database {dbname} selected.')
except mdb.ProgrammingError:
    print('database does not exist')
print()

# Demonstrates creating a table.
command = '''
CREATE TABLE konosuba_copy AS
SELECT *
FROM konosuba
;
'''
print(cz.clone_table('konosuba', printable=True))
try:
    cursor.execute(command)
    print(f'table cloned.')
except mdb.ProgrammingError as err:
    print(err)
print()

print(cz.show_tables())
print()
