# Demonstrates writing of files into a database.
# The process involves creating tables in the database and loading the data
# into it.
import mariadb as mdb
import json
import pleiades as ple

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    server_config = json.load(f)
db = mdb.connect(**server_config)
cursor = db.cursor()

# Select database to be used. Can also be set in server_config instead.
command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')


cz = ple.CZ(cursor)

# Demonstrates creating a table.
print('clone table:')
print(cz.clone_table('konosuba', printable=True))
print(cz.show_tables())
print()
