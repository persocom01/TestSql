# Demonstrates deletion of databases.
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
engine_string = 'mysql+pymysql://' + cfg['user'] + ':' + password + '@' + cfg['host']
engine = create_engine(engine_string)
con = engine.connect()

cz = ple.CZ(engine)

# Delete database.
command = f'DROP DATABASE {dbname};'
print(cz.del_db(dbname, printable=True))
try:
    cursor.execute(command)
    print('database deleted.')
    print()
except mdb.DatabaseError:
    print('database does not exist.')
    print()

# Show databases.
command = 'SHOW DATABASES;'
print(cz.show_db(printable=True))
cursor.execute(command)
dbs = cursor.fetchall()
print(dbs)
print()
