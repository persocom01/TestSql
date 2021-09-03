# Demonstrates the update command.
import json
from sqlalchemy import create_engine
import pleiades as ple

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}/{dbname}"
engine = create_engine(engine_string)
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

cz = ple.CZ(engine)

print(cz.select_from('konosuba').ex())
print()

# Demonstrates mass updating of a table from a file.
command = cz.csv_insert('./data/konosuba.csv', pkey='id', printable=True)
print('command:')
print(command)
print()
con.execute(command)

print(cz.select_from('konosuba').ex())
print()
