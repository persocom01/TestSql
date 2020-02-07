# Demonstrates creation og databases.
import mariadb as mdb

mysql_config = {
    'user': 'root',
    # 'password': '1234',
    'host': '127.0.0.1',
    'port': 3306,
}

db = mdb.connect(**mysql_config)
cursor = db.cursor()

command = '''
CREATE DATABASE testDB;
'''
try:
    cursor.execute(command)
except mdb.DatabaseError:
    print('database exists')

command = '''
SHOW DATABASES;
'''
cursor.execute(command)
dbs = cursor.fetchall()
print('databases:', dbs)

cursor.close()
db.commit()
db.close()

print('commands executed.')
