import mysql.connector as mariadb

mysql_config = {
    'user': 'root',
    # 'password': '1234',
    'host': '127.0.0.1',
    'port': 3306,
    'database': 'capstone'
}

db = mariadb.connect(**mysql_config)
cursor = db.cursor()

command = '''
SELECT first_name,last_name FROM employees WHERE first_name=%s
'''

cursor.execute(command)

cursor.close()
db.commit()
db.close()

print('command executed.')
