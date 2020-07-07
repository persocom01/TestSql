# CZ deals with databases.
# She primarily functions to make the use of sqlalchemy easier.
# She pastes 1 yen stickers on things she likes.


class CZ:

    def __init__(self, engine=None, database=None):
        self.engine = engine
        self.database = database
        self.dtype_dic = {
            'int64': 'INT',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME'
        }
        self.tabspace = 4

    # This function is meant to be used on boto3.resource objects.
    def get_keys(self, bucket_name, prefix='/', suffix=None, delimiter='/'):
        import re
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        bucket = self.engine.Bucket(bucket_name)
        keys = [_.key for _ in bucket.objects.filter(Prefix=prefix)]
        for key in keys:
            if suffix:
                if not re.search(suffix, key):
                    keys.remove(key)
            if key[-1:] == delimiter:
                keys.remove(key)
        return keys

    # This function is meant to be used on boto3.resource objects.
    def download_files(self, bucket_name, keys, savein=''):
        import re
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            filename = re.search(r'/([^/]+)$', key, re.I)[1]
            file_path = savein + filename
            bucket = self.engine.Bucket(bucket_name)
            bucket.download_file(key, file_path)
        return f'{len(keys)} files downloaded.'

    class SQL:
        '''
        The SQL object allows a SQL statement to be extended with methods like
        where() before being executed with .ex()
        '''

        def __init__(self, command, engine=None, tabspace=4):
            self.command = command
            self.engine = engine
            self.tabspace = tabspace

        def ex(self, p=False):
            command = self.command
            if p or self.engine is None:
                return command
            import pandas as pd
            df = pd.read_sql_query(command, self.engine)
            return df

        def where(self, condition):
            command = self.command
            command = command[:-1] + f'WHERE {condition}\n;'
            self.command = command
            return self

    def mk_db(self, db, printable=False):
        from sqlalchemy.exc import ProgrammingError
        command = f'CREATE DATABASE {db};'
        if printable or self.engine is None:
            return command
        try:
            self.engine.connect().execute(command)
            return f'database {db} created.'
        except ProgrammingError:
            return f'database {db} already exists.'

    def show_db(self, printable=False):
        command = f'SHOW DATABASES;'
        if printable or self.engine is None:
            return command
        import pandas as pd
        df = pd.read_sql_query(command, self.engine)
        return df

    def del_db(self, db, printable=False):
        command = f'DROP DATABASE {db};'
        if printable or self.engine is None:
            return command
        self.engine.connect().execute(command)
        return f'database {db} deleted.'

    # Returns the name of the currently selected db.
    def current_db(self, printable=False):
        command = 'SELECT DATABASE();'
        if printable or self.engine is None:
            return command
        return self.engine.connect().execute(command).fetchone()[0]

    def use_db(self, db, printable=False):
        command = f'USE {db};'
        if printable or self.engine is None:
            return command
        if self.alchemy:
            self.engine.connect().execute(command)
        else:
            self.engine.execute(command)
        return f'database {db} selected.'

    def unuse_db(self, printable=False, _db='2arnbzheo2j0gygkteu9ltxtabmzldvb'):
        command = f'CREATE DATABASE {_db};'
        command += f'\nUSE {_db};'
        command += f'\nDROP DATABASE {_db};'
        if printable or self.engine is None:
            return command
        if self.alchemy:
            self.engine.connect().execute(command)
        else:
            self.engine.execute(command)
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
        return self.SQL(command, engine=self.engine, alchemy=self.alchemy)

    def csv_clean_colnames(self, file, sep=''):
        '''
        Given the path to a csv file, this function cleans the column names by
        converting removing leading and trailing white spaces, converting all
        letters to lowercase, replacing all remaining whitespaces with
        underscores, removing brackets, forward slashes, and other special
        characters. The csv file is then replaced with a copy of itself with
        the cleaned column names.

        params:
            file        path of file wholse column names are to be cleaned.
            sep         The character(s) used to replace brackets and special
                        characters.
        '''
        import re
        import csv
        import os

        def remove_special_characters(text, sep=sep):
            pattern = r'[^a-zA-Z0-9!"#$%&\'()*+, -./:; <= >?@[\]^_`{|}~]'
            return re.sub(pattern, sep, text)

        # Opens the csv file and writes the cleaned version to a .tmp file.
        tempfile = file + '.tmp'
        with open(file, 'r') as infile, open(tempfile, 'w', newline='') as outfile:
            r = csv.reader(infile, delimiter=',', quotechar='"')
            colnames = next(r)
            colnames = [remove_special_characters(x.strip().lower().replace(' ', '_').replace('(', sep).replace(')', sep).replace('/', sep)) for x in colnames]

            w = csv.writer(outfile)
            w.writerow(colnames)
            for i, row in enumerate(r):
                if i > 0:
                    w.writerow(row)

        # Delete original and replace it with the cleaned file.
        os.remove(file)
        os.rename(tempfile, file)

    def csv_table(self, file, tablename=None, pkey=None, nrows=100000, printable=False, **kwargs):
        from pathlib import Path
        import pandas as pd
        from math import ceil
        if self.alchemy and printable is False:
            return 'csv_insert creates the necessary tables with sqlalchemy.'
        # The file name will be used as the table name if not provided.
        if tablename is None:
            tablename = Path(file).stem
        # pandas is used to impute datatypes.
        df = pd.read_csv(file, nrows=nrows, **kwargs)
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
        if printable or self.engine is None:
            return command
        self.engine.execute(command)
        return f'table {tablename} created.'

    def csv_insert(self, file, updatekey=None, postgre=False, tablename=None, chunksize=None, sizelim=1073741824, printable=False, **kwargs):
        '''
        Convenience function that uploads file data into a premade database
        table.

        params:
            file        path of file to be uploaded.
            updatekey   given the table's primary key, the function updates all
                        values in the table with those from the file except the
                        primary key. If sqlalchemy is used, tables values are
                        updated by default, but the primary key is set using
                        this value.
            postgre     set to True if working on a PostgreSQL database. Only
                        relevant if not using sqlalchemy.
            tablename   if None, tablename = filename.
            chunksize   determines the number of rows read from the csv file to
                        insert into the database at a time. This is
                        specifically meant to deal with memory issues. As such,
                        when chunksize != None and printable == True, the
                        commands will be written to chunk_insert.txt instead of
                        being returned for printing.
            sizelim     determines the file size, in bytes, before a default
                        chunksize of 10000 is imposed if chunksize is not
                        already specified.
            printable   returns the SQL command that would have been executed
                        as a printable string. It doesn't work well past a few
                        thousand rows or so.
            **kwargs    Other arguments to be passed on to pandas read_csv.
        '''
        from pathlib import Path
        from re import sub
        from sqlalchemy.exc import InternalError
        import pandas as pd
        if tablename is None:
            tablename = Path(file).stem
        # Automatically set chunksize if file exceeds sizelim.
        if Path(file).stat().st_size >= sizelim and chunksize is None:
            chunksize = 100000

        def individual_insert(df, tablename=None):
            rows = [x for x in df.itertuples(index=False, name=None)]
            cols = ', '.join(df.columns)
            for r in rows:
                command = f'INSERT INTO {tablename}({cols}) VALUES '
                # Fix null values.
                pattern = r"([^\w'])nan([^\w'])"
                replacement = r'\1NULL\2'
                fixed_r = sub(pattern, replacement, f'{r}')
                command += f'{fixed_r}'
                try:
                    self.engine.execute(command)
                except InternalError:
                    continue

        def alchemy_insert(df, updatekey=None, tablename=None):
            try:
                df.to_sql(tablename, self.engine, index=False, if_exists='append')
            except InternalError:
                individual_insert(df, tablename=tablename)
            if updatekey:
                command = f'ALTER TABLE {tablename} ADD PRIMARY KEY({updatekey});'
                self.engine.execute(command)

        def engine_insert(df, tablename=None, updatekey=None, postgre=False, printable=False):
            rows = [x for x in df.itertuples(index=False, name=None)]
            cols = ', '.join(df.columns)
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
            command = command[:-(self.tabspace+1)] + ';\n'
            if printable or self.engine is None:
                return command
            self.engine.execute(command)

        if chunksize:
            reader = pd.read_csv(file, chunksize=chunksize, **kwargs)

            for chunk in reader:
                df = pd.DataFrame(chunk)
                if self.alchemy and printable is False:
                    alchemy_insert(df, updatekey=updatekey,
                                   tablename=tablename)
                if printable or self.engine is None:
                    with open('chunk_insert.txt', 'a') as f:
                        f.write(engine_insert(
                            df, updatekey=updatekey, postgre=postgre, tablename=tablename, printable=printable))
                else:
                    engine_insert(df, updatekey=updatekey, postgre=postgre,
                                  tablename=tablename, printable=printable)

            if printable is None and self.engine:
                return f'data loaded into table {tablename}.'

        else:
            df = pd.read_csv(file, **kwargs)

            if self.alchemy and printable is False:
                alchemy_insert(df, updatekey=updatekey, tablename=tablename)
                return f'data loaded into table {tablename}.'

            if printable or self.engine is None:
                return engine_insert(df, updatekey=updatekey, postgre=postgre, tablename=tablename, printable=printable)

            engine_insert(df, updatekey=updatekey, postgre=postgre,
                          tablename=tablename, printable=printable)
            return f'data loaded into table {tablename}.'

    def csvs_into_database(self, file_paths, tablename=None, clean_colnames=False, pkeys=None, printable=False, **kwargs):
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
            **kwargs    optional arguments passed to pandas' read_csv function.
                        na_values can be specified, keep_default_na=False,
                        low_memory=False are useful arguments.
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
                if tablename:
                    for i, file in enumerate(files):
                        if clean_colnames:
                            self.csv_clean_colnames(file)
                        if i == 0:
                            self.csv_table(files[i], tablename=tablename, updatekey=pkeys[i], **kwargs)
                        try:
                            self.csv_insert(file, tablename=tablename, updatekey=pkeys[i], **kwargs)
                        except TypeError or IndexError:
                            self.csv_insert(file, tablename=tablename, **kwargs)
                else:
                    for i, file in enumerate(files):
                        if clean_colnames:
                            self.csv_clean_colnames(file)
                        try:
                            self.csv_table(file, updatekey=pkeys[i], **kwargs)
                            self.csv_insert(file, updatekey=pkeys[i], **kwargs)
                        except TypeError or IndexError:
                            has_incomplete_pkeys = True
                            self.csv_table(file, **kwargs)
                            self.csv_insert(file, **kwargs)
        else:
            if tablename:
                for i, file in enumerate(files):
                    if clean_colnames:
                        self.csv_clean_colnames(file)
                    if i == 0:
                        self.csv_table(files[i], tablename=tablename, **kwargs)
                    self.csv_insert(file, tablename=tablename, **kwargs)
            else:
                for file in files:
                    if clean_colnames:
                        self.csv_clean_colnames(file)
                    self.csv_table(file, **kwargs)
                    self.csv_insert(file, **kwargs)
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
        if printable or self.engine is None:
            return command
        if self.alchemy:
            self.engine.connect().execute(command)
        else:
            self.engine.execute(command)
        return f'table(s) {return_string} deleted.'

    def show_columns(self, table, all=False):
        if all:
            command = f"SHOW ALL COLUMNS FROM {table};"
        else:
            command = f"SHOW COLUMNS FROM {table};"
        if self.alchemy:
            import pandas as pd
            df = pd.read_sql_query(command, self.engine)
            return df
        else:
            self.engine.execute(command)
            return self.engine.fetchall()

    def show_tables(self, all=False):
        if all:
            command = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
        else:
            command = 'SHOW TABLES;'
        if self.alchemy:
            import pandas as pd
            df = pd.read_sql_query(command, self.engine)
            return df
        else:
            self.engine.execute(command)
            return self.engine.fetchall()
