# Demonstrates the update command.
from sqlalchemy import create_engine
import pleiades as ple

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

# Demonstrates the basic update datement.
# If where is not specified all rows will be updated with the same value.
# This can be used to copy a column onto another using SET col2 = col1.
command = '''
UPDATE konosuba
SET age = 17000
WHERE id = 1;
'''
con.execute(command)

cz = ple.CZ(engine, alchemy=True)

print(cz.select_from('konosuba').ex())
print()

# Demonstrates mass updating of a table from a file.
command = cz.csv_insert('./data/konosuba.csv', updatekey='id', printable=True)
print('command:')
print(command)
print()
con.execute(command)

print(cz.select_from('konosuba').ex())
print()
