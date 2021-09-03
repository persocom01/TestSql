# Demonstrates creation of databases in PostgreSQL. Aside from inor differences
# in SQL syntax, one of the main differences is the need to set
# execution_options of the connection to 'AUTOCOMMIT' when executing write
# commands.
import json
from sqlalchemy import create_engine
from sqlalchemy import exc

# SQL is case insensitive.
dbname = 'food_order2'
cfg_path = './PostgreSQL/server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
# sqlalchemy's engine strings are written in the form:
# dialect[+driver]://user:password@host:port/dbname[?key=value..]
# Leave out /dbname if not connecting to a db.
# It seems to use the database's appropriate default port if not specified.
# In this case the default port is 5432.
engine_string = f"postgresql+psycopg2://{cfg['user']}:{password}@{cfg['host']}"
engine = create_engine(engine_string)
con = engine.connect()
# Needed to use certain commands in PostgreSQL.
con.execution_options(isolation_level='AUTOCOMMIT')

# Create database.
command = f'CREATE DATABASE {dbname} ENCODING="utf8";'
try:
    con.execute(command)
    print('database created.')
except exc.ProgrammingError as err:
    print(err)
print()

# Show databases.
# Unlike mysql, PostgreSQL has no SHOW DATABASES command.
command = 'SELECT datname FROM pg_database;'
dbs = con.execute(command).fetchall()
print(dbs)
print()
