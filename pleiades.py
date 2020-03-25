# CZ deals with databases.
# She pastes 1 yen stickers on things she likes.


class CZ:

    def __init__(self, cursor=None, alchemy=False):
        self.cursor = cursor
        self.dtype_dic = {
            'int64': 'INT',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME'
        }
        self.alchemy = alchemy
        self.tabspace = 4

    class SQL:
        '''
        The SQL object allows a SQL statement to be extended with methods like
        where() before being executed with .ex()

        params:
            alchemy     when set to true, assumes that cursor is an sqlalchemy
                        engine. This results in some sql commands being
                        returned as a DataFrame.
        '''

        def __init__(self, command, cursor=None, alchemy=False, tabspace=4):
            self.alchemy = alchemy
            self.command = command
            self.cursor = cursor
            self.tabspace = tabspace

        def ex(self, p=False):
            command = self.command
            if p or self.cursor is None:
                return command
            if self.alchemy:
                import pandas as pd
                df = pd.read_sql_query(command, self.cursor)
                return df
            else:
                self.cursor.execute(command)
                return self.cursor.fetchall()

        def where(self, condition):
            command = self.command
            command = command[:-1] + f'WHERE {condition}\n;'
            self.command = command
            return self

    def get_db(self, printable=False):
        command = 'SELECT DATABASE();'
        if printable or self.cursor is None:
            return command
        if self.alchemy:
            return self.cursor.connect().execute(command).fetchone()[0]
        else:
            self.cursor.execute(command)
            return self.cursor.fetchone()[0]

    def use_db(self, db, printable=False):
        command = f'USE {db};'
        if printable or self.cursor is None:
            return command
        if self.alchemy:
            self.cursor.connect().execute(command)
        else:
            self.cursor.execute(command)
        return f'database {db} selected.'

    def unuse_db(self, printable=False, _db='2arnbzheo2j0gygkteu9ltxtabmzldvb'):
        command = f'CREATE DATABASE {_db};'
        command += f'\nUSE {_db};'
        command += f'\nDROP DATABASE {_db};'
        if printable or self.cursor is None:
            return command
        if self.alchemy:
            self.cursor.connect().execute(command)
        else:
            self.cursor.execute(command)
        return 'database deselected.'

    def select_from(self, table, cols=None):
        tab = ' ' * self.tabspace
        command = 'SELECT'
        if cols:
            if isinstance(cols, str):
                command += f' {cols}\n'
            else:
                command += f'\n{tab}'
                for col in cols:
                    command += f'{col}\n{tab},'
                command = command[:-(self.tabspace+1)]
        else:
            command += f' *\n'
        command += f'FROM {table}\n;'
        return self.SQL(command, cursor=self.cursor, alchemy=self.alchemy)

    def csv_table(self, file, pkey=None, printable=False, nrows=100):
        from pathlib import Path
        import pandas as pd
        from math import ceil
        # The file name will be used as the table name.
        tablename = Path(file).stem
        # pandas is used to impute datatypes.
        df = pd.read_csv(file, nrows=nrows)
        df_dtypes = [x for x in df.dtypes.apply(lambda x: x.name)]
        df = df.fillna('')
        sql_dtypes = []
        tab = ' ' * self.tabspace
        command = f'CREATE TABLE {tablename}(\n{tab}'
        # pandas dtypes are converted to sql dtypes to create the table.
        for i, col in enumerate(df.columns):
            if df_dtypes[i] in self.dtype_dic:
                sql_dtypes.append(self.dtype_dic[df_dtypes[i]])
            else:
                # Determine VARCHAR length.
                char_length = ceil(df[col].map(len).max() / 50) * 50
                sql_dtypes.append(f'VARCHAR({char_length})')
            if pkey and col == pkey:
                command = command + \
                    f'{col} {sql_dtypes[i]} NOT NULL\n{tab},'
            else:
                command = command + f'{col} {sql_dtypes[i]}\n{tab},'
        if pkey:
            command += f'PRIMARY KEY({pkey})\n{tab},'
        command = command[:-(self.tabspace+1)] + ');'
        if printable or self.cursor is None:
            return command
        if self.alchemy:
            self.cursor.connect().execute(command)
        else:
            self.cursor.execute(command)
        return f'table {tablename} created.'

    def csv_insert(self, file, updatekey=None, postgre=False, tablename=None, printable=False):
        '''
        Convenience function that uploads file data into a premade database
        table.

        params:
            updatekey   given the table's primary key, the function updates all
                        values in the table with those from the file except the
                        primary key.
            postgre     set to True if working on a PostgreSQL database.
            tablename   if None, tablename = filename.
            printable   returns the SQL command that would have been executed
                        as a printable string.
        '''
        import pandas as pd
        from re import sub
        if tablename is None:
            from pathlib import Path
            tablename = Path(file).stem
        df = pd.read_csv(file)
        rows = [x for x in df.itertuples(index=False, name=None)]
        cols = ','.join(df.columns)
        tab = ' ' * self.tabspace
        command = f'INSERT INTO {tablename}({cols})\nVALUES\n{tab}'
        for r in rows:
            # Fix null values.
            pattern = r"([^\w'])nan([^\w'])"
            replacement = r'\1NULL\2'
            fixed_r = sub(pattern, replacement, f'{r}')
            command += f'{fixed_r}\n{tab},'
        if updatekey:
            if postgre:
                command = command[:-(self.tabspace+1)] + \
                    f'ON CONFLICT ({updatekey}) DO UPDATE SET\n{tab}'
                for c in df.columns:
                    if c != updatekey:
                        command += f'{c}=excluded.{c}\n{tab},'
            else:
                command = command[:-(self.tabspace+1)] + \
                    f'ON DUPLICATE KEY UPDATE\n{tab}'
                for c in df.columns:
                    if c != updatekey:
                        command += f'{c}=VALUES({c})\n{tab},'
        command = command[:-(self.tabspace+1)] + ';'
        if printable or self.cursor is None:
            return command
        if self.alchemy:
            self.cursor.connect().execute(command)
        else:
            self.cursor.execute(command)
        return f'data loaded into table {tablename}.'

    def csvs_into_database(self, file_paths, pkeys=None, printable=False):
        '''
        Convenience function that uploads a folder of files into a database.
        params:
            pkeys       accepts a list of primary keys to be assigned to each
                        table to be created as a list. Must be given in file
                        alphabetical order as the tables will be created in
                        alphabetical order. The list can be incomplete, but any
                        table not assigned a primary key should have '' as its
                        corresponding list item. Note that this alphabetical
                        order can be different from atom file order if _ is
                        used.
            printable   returns a printable of files that would be be inserted
                        into the database.
        '''
        import glob
        files = glob.glob(file_paths)
        if printable:
            return files
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

    def del_tables(self, tables, printable=False):
        tab = ' ' * self.tabspace
        command = f'DROP TABLES'
        if isinstance(tables, str):
            command += f' {tables};'
            return_string = tables
        else:
            command += f'\n{tab}'
            for table in tables:
                command += f'{table}\n{tab},'
            command = command[:-(self.tabspace+1)] + ';'
            return_string = ', '.join(tables)
        if printable or self.cursor is None:
            return command
        if self.alchemy:
            self.cursor.connect().execute(command)
        else:
            self.cursor.execute(command)
        return f'table(s) {return_string} deleted.'

    def show_columns(self, table, all=False):
        if all:
            command = f"SHOW ALL COLUMNS FROM {table};"
        else:
            command = f"SHOW COLUMNS FROM {table};"
        if self.alchemy:
            import pandas as pd
            df = pd.read_sql_query(command, self.cursor)
            return df
        else:
            self.cursor.execute(command)
            return self.cursor.fetchall()

    def show_tables(self, all=False):
        if all:
            command = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
        else:
            command = 'SHOW TABLES;'
        if self.alchemy:
            import pandas as pd
            df = pd.read_sql_query(command, self.cursor)
            return df
        else:
            self.cursor.execute(command)
            return self.cursor.fetchall()
