# Demonstrates the where, and and or logical operators.
from sqlalchemy import create_engine
import pleiades as ple

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

# Demonstrates the basic update datement.
# If where is not specified all rows will be updated with the same value.
command = '''
UPDATE konosuba
SET age = 17000
WHERE id = 1;
'''
con.execute(command)

cz = ple.CZ(engine=engine)

print(cz.select_from('konosuba').ex())
print()

# Demonstrates mass updating of a table from a file.
command = cz.csv_insert('./data/konosuba.csv', updatekey='id', printc=True)
print('command:')
print(command)
print()
con.execute(command)

print(cz.select_from('konosuba').ex())
print()
