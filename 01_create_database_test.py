import mysql.connector as mariadb

mysql_config = {
    'user': 'root',
    # 'password': '1234',
    'host': '127.0.0.1',
    'port': 3306,
}

db = mariadb.connect(**mysql_config)
cursor = db.cursor()

command = '''
CREATE DATABASE testDB;
'''
cursor.execute(command)

command = '''
SHOW DATABASES;
'''
cursor.execute(command)

cursor.close()
db.commit()
db.close()

print('commands executed.')
