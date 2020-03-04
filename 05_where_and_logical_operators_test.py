# Demonstrates the keyword where and the logical operators that can be used
# with it.
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('mysql+pymysql://root:@localhost/testDB')
con = engine.connect()

# Comparison operators:
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
SELECT name,class,race,age
FROM konosuba
WHERE age > 18;
'''
df = pd.read_sql_query(command, engine)
print('age > 18:')
print(df.head())
print()

# Note that BOOLEAN columns don't need a value = 1 or value = True, but it does
# still work if written that way.
# Brackets are optional in this case, but makes the logic clearer, as AND has
# higher priority over OR.
# For string values, escape ' by doubling it. For instance, 'McDonald''s'
command = '''
SELECT name,class,race,age
FROM konosuba
WHERE (NOT isekai AND race = 'human') OR class = 'adventurer';
'''
df = pd.read_sql_query(command, engine)
print('not isekai AND human OR class = adventurer:')
print(df.head())
print()

# Demonstrates logical operators.
command = '''
SELECT name,class,race,age
FROM konosuba
WHERE age BETWEEN 18 AND 999;
'''
df = pd.read_sql_query(command, engine)
print('age between 18 and 999:')
print(df.head())
print()

# IS NOT NULL returns non nulls.
command = '''
SELECT *
FROM konosuba
WHERE class IS NULL;
'''
df = pd.read_sql_query(command, engine)
print('is null:')
print(df)
print()

# ALL and ANY accept SELECT subqueries. The difference being ANY often
# references its own column while ALL normally doesn't. (it does in this case)
command = '''
SELECT *
FROM konosuba
WHERE age > ALL
    (
        SELECT age
        FROM konosuba
        WHERE class = 'thief'
    );
'''
df = pd.read_sql_query(command, engine)
print('age > all thieves:')
print(df)
print()

# ANY and IN. IN (value2,value2) can be written as col=value1 OR col=value2.
command = '''
SELECT *
FROM konosuba
WHERE class = ANY
    (
        SELECT class
        FROM konosuba
        WHERE class IN ('priest','wizard','crusader')
    );
'''
df = pd.read_sql_query(command, engine)
print('any priest, wizard or crusader:')
print(df)
print()

# EXISTS is often used to find rows in different tables that share a common
# key. NOT EXISTS is used to find the opposite.
# It should be noted that EXISTS (SELECT NULL) will always evaluate to True.
command = '''
SELECT *
FROM konosuba AS k
WHERE EXISTS
    (
        SELECT char_id
        FROM quest_map AS qm
        WHERE qm.char_id = k.id
    );
'''
df = pd.read_sql_query(command, engine)
print('any priest, wizard or crusader:')
print(df)
print()
