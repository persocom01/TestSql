# Demonstrates deletion of databases.
import json
from sqlalchemy import create_engine
from sqlalchemy import exc

dbname = 'food_order2'
cfg_path = './PostgreSQL/server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"postgresql+psycopg2://{cfg['user']}:{password}@{cfg['host']}"
engine = create_engine(engine_string)
con = engine.connect()
con.execution_options(isolation_level='AUTOCOMMIT')

# Delete database.
command = f'DROP DATABASE {dbname};'
try:
    con.execute(command)
    print('database deleted.')
except exc.ProgrammingError as err:
    print(err)
print()

# Show databases.
command = 'SELECT datname FROM pg_database;'
dbs = con.execute(command).fetchall()
print(dbs)
print()
