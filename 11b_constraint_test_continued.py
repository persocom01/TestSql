# Demonstrates table constraints. There are 6 possible constraints:
# NOT NULL
# UNIQUE
# PRIMARY KEY
# FOREIGN KEY
# CHECK
# DEFAULT
import json
import mariadb as mdb
import pandas as pd
from sqlalchemy import create_engine
import pleiades as ple

dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    cfg = json.load(f)

# Official mdb connector.
mdb_con = mdb.connect(**cfg)
cursor = mdb_con.cursor()

# sqlalchemy connector.
try:
    password = cfg['password']
except KeyError:
    password = ''
engine_string = f"mysql+pymysql://{cfg['user']}:{password}@{cfg['host']}/{dbname}"
engine = create_engine(engine_string)
con = engine.connect()

command = f'USE {dbname};'
try:
    cursor.execute(command)
except mdb.ProgrammingError:
    print('database does not exist')

cz = ple.CZ(engine)

# FOREIGN KEY is a CONSTRAINT that enforces referential integrity between
# tables. The table with the FOREIGN KEY is the child and the reference is the
# parent. The FOREIGN KEY of the child typically references the PRIMARY KEY of
# the parent. Aside from referencing, two options, ON DELETE and ON UPDATE can
# be set to determine what happens to the FOREIGN KEY when the parent is
# deleted or updated. The options are CASCADE, SET NULL and RESTRICT.
# The options are set to RESTRICT by default, which makes it impossible for the
# parent to be deleted or updated as long as the child exists.
# CASCADE sets the child equal to whatever the parent is set to. Trying to
# delete the parent when ON UPDATE is set to cascade will still return an error
# because ON DELETE will override ON UPDATE for delete.
# SET NULL sets the child to NULL.
# Unlike UNIQUE, FOREIGN KEY can only be be set at table creation after column
# datatypes are defined or after using ALTER TABLE and practically the same
# syntax.
command = '''
ALTER TABLE quest_map
ADD CONSTRAINT fk_qm_qid
    FOREIGN KEY(quest_id)
        REFERENCES quest(id)
        ON DELETE restrict
        ON UPDATE restrict
,ADD CONSTRAINT fk_qm_cid
    FOREIGN KEY(char_id)
        REFERENCES konosuba(id)
        ON DELETE restrict
        ON UPDATE cascade
;
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('quest_map'))
print('foreign key:')
print(df)
print()

# Demonstrates the effect of cascade.
command = '''
UPDATE konosuba
SET id = 999
WHERE id = 1;
'''
cursor.execute(command)
df = pd.DataFrame(cz.select_from('quest_map').ex())
print('cascade:')
print(df)
print()

command = '''
UPDATE konosuba
SET id = 1
WHERE id = 999;
'''
cursor.execute(command)
df = pd.DataFrame(cz.select_from('quest_map').ex())
print('undo cascade:')
print(df)
print()

# Demonstrates dropping of a foreign key. Note that the key property of the
# column will still remain MUL after removal of the constraint. It is unknown
# if this has any impact.
command = '''
ALTER TABLE quest_map
DROP CONSTRAINT fk_qm_qid
,DROP CONSTRAINT fk_qm_cid
;
'''
cursor.execute(command)
df = pd.DataFrame(cz.show_columns('quest_map'))
print(df)
print()

cursor.close()
mdb_con.commit()
mdb_con.close()
print('commands executed.')
