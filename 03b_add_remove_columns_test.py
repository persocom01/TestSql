# Demonstrates cloning a table.
import json
import mariadb as mdb
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
engine_string = 'mysql+pymysql://' + cfg['user'] + ':' + password + '@' + cfg['host'] + '/' + dbname
engine = create_engine(engine_string)
con = engine.connect()

cz = ple.CZ(engine)

# Select database to be used. Can also be set in server.cfg instead.
command = f'USE {dbname};'
print(cz.use_db(dbname, printable=True))
try:
    cursor.execute(command)
    print(f'database {dbname} selected.')
except mdb.ProgrammingError:
    print('database does not exist')
print()

# Demonstrates dropping a column.
command = '''
ALTER TABLE konosuba_copy
DROP COLUMN IF EXISTS age
;
'''
print(cz.del_columns('konosuba_copy', 'age', printable=True))
cursor.execute(command)
print('column dropped.')
print()

command = '''
CREATE PROCEDURE MIMIC.map_icd9_to_icd10_am()
BEGIN
    IF NOT EXISTS (
        SELECT NULL
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = 'DIAGNOSES_ICD_copy'
        AND table_schema = 'MIMIC'
        AND column_name = 'ICD10_AM_CODE'
        ) THEN
        ALTER TABLE DIAGNOSES_ICD_copy ADD ICD10_AM_CODE VARCHAR(50);
    END IF;
END
'''
print(cz.insert_columns('konosuba_copy', 'client', cols='name', where='M', printable=True))
print(cursor.execute(command))

command = '''
CREATE PROCEDURE MIMIC.test(CREATE PROCEDURE MIMIC.map_icd9_to_icd10_am(IN target VARCHAR(255)))
BEGIN
    SET @table_name = target;
    SET @command = concat('SELECT * FROM ',@table_name);

    PREPARE stmt FROM @command;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END
'''
print(cz.insert_columns('konosuba_copy', 'client', cols='name', where='M', printable=True))
print(cursor.execute(command))

command = '''
CREATE PROCEDURE MIMIC.map_icd9_pcs_to_icd10_pcs(IN target VARCHAR(255))
BEGIN
    SET @table_name = target;
    SET @command = concat('ALTER TABLE ',@table_name,' ADD ICD10_CODE VARCHAR(10800);');
    PREPARE stmt FROM @command;

    IF NOT EXISTS (
        SELECT NULL
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = @table_name
        AND table_schema = 'MIMIC'
        AND column_name = 'ICD10_CODE'
        ) THEN
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    SET @command = concat('UPDATE ',@table_name,' t JOIN MIMIC.D_ICD_PROCEDURES_copy s ON s.ICD9_CODE = t.ICD9_CODE SET t.ICD10_CODE = s.ICD10_CODE;');
    PREPARE stmt FROM @command;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END
'''
print(cz.insert_columns('konosuba_copy', 'client', cols='name', where='M', printable=True))
print(cursor.execute(command))

print(cz.select_from('konosuba_copy').ex())
print()
