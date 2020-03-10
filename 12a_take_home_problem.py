# Workmate Data Analyst take home problem. The given data are found in the csv
# files client, partner, staff_request and timesheet_entry in the data folder.
from sqlalchemy import create_engine
import pandas as pd
import pleiades as ple

pd.set_option('display.max_columns', 100)
pd.set_option('display.max_rows', 300)

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

cz = ple.CZ(engine, alchemy=True)

# Problem 1.
command = '''
SELECT
    c.name as 'client_name'
    ,COUNT(*) as 'total_timesheet_entries'
FROM client c
INNER JOIN staff_request sr ON sr.client_id = c.id
INNER JOIN timesheet_entry te ON te.staff_request_id = sr.id
GROUP BY c.name
ORDER BY COUNT(*)
;
'''
df = pd.read_sql_query(command, engine)
print(df)
print()

# Problem 2.
command = '''
SELECT
    c.name as 'client_name'
    ,DATE_FORMAT(STR_TO_DATE(clock_in_time, '%%m/%%d/%%y %%h:%%i %%p'), '%%Y-%%m') as 'month'
    ,COUNT(*) as 'total_timesheet_entries'
FROM client c
INNER JOIN staff_request sr ON sr.client_id = c.id
INNER JOIN timesheet_entry te ON te.staff_request_id = sr.id
GROUP BY clock_in_time
ORDER BY COUNT(*)
;
'''
df = pd.read_sql_query(command, engine)
print(df)
print()
