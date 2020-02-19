# Demonstrates the where, and and or logical operators.
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

# Comparison operators in sql:
# =
# !=
# <> same as !=
# >
# !> not greater than
# <
# !< not smaller than
# >=
# <=

command = '''
SELECT name,race,age
FROM konosuba
WHERE age > 18;
'''
df = pd.read_sql_query(command, engine)
print(df.head())
print()
