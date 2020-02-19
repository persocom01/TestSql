# CZ deals with databases.
# She pastes 1 yen stickers on things she likes.


class CZ:

    def __init__(self, cursor=None):
        self.cursor = cursor
        self.dtype_dic = {
            'int64': 'INT',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME'
        }

    def get_db(self, printc=False):
        command = '''
        SELECT DATABASE()
        '''
        if printc:
            return command
        else:
            self.cursor.execute(command)
            return self.cursor.fetchone()[0]

    def use_db(self, db, printc=False):
        command = f'USE {db};'
        self.cursor.execute(command)
        if printc:
            return command
        else:
            self.cursor.execute(command)
            return f'database {db} selected.'

    def unuse_db(self, printc=False, _db='2arnbzheo2j0gygk'):
        command = f'CREATE DATABASE {_db};'
        command += f'\nUSE {_db};'
        command += f'\nDROP DATABASE {_db};'
        if printc:
            return command
        else:
            self.cursor.execute(command)
            return 'database deselected.'

    def csv_table(self, file, pkey=None, printc=False, nrows=100):
        from pathlib import Path
        import pandas as pd
        from math import ceil
        # The file name will be used as the table name.
        tablename = Path(file).stem
        # pandas is used to impute datatypes.
        df = pd.read_csv(file, nrows=nrows)
        df_dtypes = [x for x in df.dtypes.apply(lambda x: x.name)]
        sql_dtypes = []
        command = f'CREATE TABLE {tablename}('
        # pandas dtypes are converted to sql dtypes to create the table.
        for i, col in enumerate(df.columns):
            if df_dtypes[i] in self.dtype_dic:
                sql_dtypes.append(self.dtype_dic[df_dtypes[i]])
            else:
                # Determine VARCHAR length.
                char_length = ceil(df[col].map(len).max() / 50) * 50
                sql_dtypes.append(f'VARCHAR({char_length})')
            if pkey and col == pkey:
                command = command + f'\n{col} {sql_dtypes[i]} NOT NULL,'
            else:
                command = command + f'\n{col} {sql_dtypes[i]},'
        if pkey:
            command += f'\nPRIMARY KEY ({pkey})\n);'
        else:
            command = command[:-1] + '\n);'
        if printc:
            return command
        else:
            self.cursor.execute(command)
            return f'table {tablename} created.'

    def csv_insert(self, file, tablename=None, printc=False):
        import pandas as pd
        if tablename is None:
            from pathlib import Path
            tablename = Path(file).stem
        df = pd.read_csv(file)
        rows = [x for x in df.itertuples(index=False, name=None)]
        cols = ','.join(df.columns)
        command = f'INSERT INTO {tablename}({cols}) VALUES'
        for r in rows:
            command += f'\n{r},'
        command = command[:-1] + ';'
        if printc:
            return command
        else:
            self.cursor.execute(command)
            return f'data loaded into table {tablename}.'

    def csvs_into_database(self, file_paths, pkeys=None):
        import glob
        files = glob.glob(file_paths)
        has_incomplete_pkeys = False
        if pkeys:
            if isinstance(pkeys, str):
                pkeys = [pkeys]
            else:
                for i, file in enumerate(files):
                    try:
                        self.csv_table(file, pkeys[i])
                    except IndexError:
                        has_incomplete_pkeys = True
                        self.csv_table(file)
                    self.csv_insert(file)
        else:
            for file in files:
                self.csv_table(file)
                self.csv_insert(file)
        return_statement = f'files written to database {self.get_db()}.'
        if has_incomplete_pkeys:
            return_statement = 'not all tables have primary keys.\n' + return_statement
        return return_statement

    def show_tables(self, all=False):
        if all:
            command = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"
        else:
            command = '''
            SHOW TABLES
            '''
        self.cursor.execute(command)
        return self.cursor.fetchall()
