# Demonstrates the delete command.
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

cz = ple.CZ(engine)

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
