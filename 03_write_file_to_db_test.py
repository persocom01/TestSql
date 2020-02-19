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
print('create table:')
print(cz.csv_table('./data/client.csv', 'id', printc=True))
print()

# Convenience function to load files into database.
file_paths = './data/*.csv'
print(cz.csvs_into_database(file_paths, ['id', 'id', 'id', 'id', 'id']))
# Show created tables. The sql query is SHOW TABLES
print(cz.show_tables())

# Drop tables.
command = '''
DROP TABLES client,konosuba,partner,staff_request,timesheet_entry
'''
cursor.execute(command)
print(cz.show_tables())

cursor.close()
db.commit()
db.close()
print('commands executed.')
