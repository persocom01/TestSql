# Demonstrates writing of files into a database.
# The process involves creating tables in the database and loading the data
# into it.
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
engine = create_engine(engine_string, pool_pre_ping=True, pool_recycle=300)
con = engine.connect()

cz = ple.CZ(engine)

# Select database to be used. Can also be set in server.cfg instead.
command = f'USE {dbname};'
print(cz.use_db(dbname, printable=True))
try:
    cursor.execute(command)
    print(f'database {dbname} selected.')
    print()
except mdb.ProgrammingError as err:
    print(err)
    print()

# Demonstrates creating a table.
# The timestamp data type is special in that the first one of its kind in a
# table is automatically made to update itself with the current timestamp
# whenever the table is updated. The full line to create a timestamp column is
# as follows:
# column_name TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE CURRENT_TIMESTAMP,
command = '''
CREATE TABLE client(
    id INT
    ,name VARCHAR(50)
    ,PRIMARY KEY(id)
);
'''
pkey = 'id'
print(cz.csv_table('./data/client.csv', pkey=pkey, printable=True))
# try:
#     cursor.execute(command)
#     print(f'table created.')
# except mdb.ProgrammingError as err:
#     print(err)
print()

# Demonstrates writing data to a table.
command = '''
INSERT INTO client(id, name)
VALUES
    (1, 'Pizza Booth')
    ,(2, 'McDaniels')
;
'''
print(cz.csv_insert('./data/client.csv', printable=True))
# try:
#     cursor.execute(command)
#     print(f'table values inserted.')
# except mdb.IntegrityError as err:
#     print(err)
print()

# Convenience function to load files into database.
file_paths = './data/*.csv'
print(cz.csvs_into_database(file_paths, pkeys=[
      'id', 'id', 'id', 'id', '', 'id', 'id']))
# Show created tables. The sql query is SHOW TABLES
print(cz.show_tables())
print()

# Drop tables.
tables = ['client', 'konosuba', 'partner', 'quest_map',
          'quest', 'staff_request', 'timesheet_entry']
# print(cz.del_tables(tables, printable=True))
# print(cz.del_tables(tables))
# print(cz.show_tables())
print()
