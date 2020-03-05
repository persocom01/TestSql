from sqlalchemy import create_engine
import json
import mariadb as mdb
import pandas as pd
import pleiades as ple

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

cz = ple.CZ(engine, alchemy=True)

command = '''
ALTER TABLE quest_map
DROP PRIMARY KEY;
'''
# con.execute(command))
print(cz.show_columns('quest_map'))
print()
