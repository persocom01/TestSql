# Demonstrates creation of databases.
import mariadb as mdb
import json

# SQL is case insensitive.
dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    server_config = json.load(f)
db = mdb.connect(**server_config)
cursor = db.cursor()

# Create database.
command = f'CREATE DATABASE {dbname};'
try:
    cursor.execute(command)
except mdb.DatabaseError:
    print('database exists')

# Show databases.
command = f'SHOW DATABASES;'
cursor.execute(command)
dbs = cursor.fetchall()
print('databases:', dbs)

# End.
cursor.close()
db.commit()
db.close()
print('commands executed.')
