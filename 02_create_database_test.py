# Demonstrates creation of databases.
import json
import mariadb as mdb
from sqlalchemy import create_engine
import pleiades as ple

# SQL is case insensitive.
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

# Create database.
command = f'CREATE DATABASE {dbname};'
print(cz.mk_db(dbname, printable=True))
try:
    cursor.execute(command)
except mdb.DatabaseError:
    print('database exists')
    print()

# Show databases.
command = 'SHOW DATABASES;'
print(cz.show_db(printable=True))
cursor.execute(command)
dbs = cursor.fetchall()
print(dbs)
print()

# End.
cursor.close()
mdb_con.commit()
mdb_con.close()
print('commands executed.')
