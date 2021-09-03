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
# sqlalchemy's engine strings are written in the form:
# dialect[+driver]://user:password@host:port/dbname[?key=value..]
# Leave out /dbname if not connecting to a db.
# It seems to use the database's appropriate default port if not specified.
# In this case the default port is 3306.
# How to write the engine string for various flavors of sql are detailed here:
# https://docs.sqlalchemy.org/en/14/core/engines.html
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}"
engine = create_engine(engine_string)
con = engine.connect()

cz = ple.CZ(engine)

# Create database.
command = f'CREATE DATABASE {dbname} CHARACTER SET utf8 COLLATE utf8_general_ci;'
print(cz.mk_db(dbname, printable=True))
try:
    cursor.execute(command)
    print('database created.')
except mdb.DatabaseError as err:
    print(err)
print()

# Show databases.
command = 'SHOW DATABASES;'
print(cz.show_db(printable=True))
cursor.execute(command)
dbs = cursor.fetchall()
print(dbs)
print()

# The mariadb connector autocommits by default, but if you use:
# mdb_con.autocommit = False
# you need to wite the following code after every script:
# mdb_con.commit()
# mdb_con.close()
