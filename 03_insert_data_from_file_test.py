# Demonstrates creation of databases.
import mariadb as mdb
import json

# SQL is case insensitive.
dbname = 'testDB'
cfg_path = './server.cfg'

with open(cfg_path, 'r') as f:
    server_config = json.load(f)
db = mdb.connect(**server_config)
cursor = db.cursor()

LOAD DATA LOCAL INFILE 'source.csv' INTO target_db.target_table FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n'
