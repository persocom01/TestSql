# Demonstrates the delete command.
from sqlalchemy import create_engine
import pleiades as ple

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

cz = ple.CZ(engine=engine)

print('before:')
print(cz.select_from('konosuba').ex())

# Demonstrates the basic delete datement.
# If where is not specified all rows in the table will be deleted.
command = '''
DELETE
FROM konosuba
WHERE id = 1;
'''
con.execute(command)

print('after')
print(cz.select_from('konosuba').ex())
print()
