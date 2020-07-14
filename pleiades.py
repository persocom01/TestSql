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
        command = f'CREATE DATABASE {db};'
        if printable or self.engine is None:
            return command
        from sqlalchemy.exc import ProgrammingError
        try:
            self.engine.connect().execute(command)
            return f'database {db} created.'
        except ProgrammingError as err:
            return err

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
        from sqlalchemy.exc import InternalError
        try:
            self.engine.connect().execute(command)
            return f'database {db} deleted.'
        except InternalError as err:
            return err

    # Returns the name of the currently selected db.
    def current_db(self, printable=False):
        command = 'SELECT DATABASE();'
        if printable or self.engine is None:
            return command
        return self.engine.connect().execute(command).fetchone()[0]

    def use_db(self, db=None, printable=False):
        if db is None:
            db = self.database
        command = f'USE {db};'
        if printable or self.engine is None:
            return command
        self.engine.connect().execute(command)
        return f'database {db} selected.'

    def unuse_db(self, printable=False, _db='2arnbzheo2j0gygkteu9ltxtabmzldvb'):
        command = f'CREATE DATABASE {_db};'
        command += f'\nUSE {_db};'
        command += f'\nDROP DATABASE {_db};'
        if printable or self.engine is None:
            return command
        self.engine.connect().execute(command)
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
        if self.database:
            table = self.database + '.' + table
        command += f'FROM {table}\n;'
        return self.SQL(command, engine=self.engine)

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
            colnames = [remove_special_characters(x.strip().lower().replace(' ', '_').replace(
                '(', sep).replace(')', sep).replace('/', sep)) for x in colnames]
            w = csv.writer(outfile)
            w.writerow(colnames)
            for row in r:
                w.writerow(row)

        # Delete original and replace it with the cleaned file.
        os.remove(file)
        os.rename(tempfile, file)

    def csv_table(self, file, table=None, pkey=None, nrows=100000, printable=False, **kwargs):
        '''
        Creates an empty table based on data from a file. Normally unnecessary
        as pandas .to_sql() creates the table automatically, but could be
        useful when that doesn't work.
        params:
            file        file the table datatypes will be based on.
            table       if None, table = filename.
            pkeys       the table names to pass on when defining the PRIMARY
                        KEYs of the table. If a list is passed, a composite
                        primary key will be defined.
            nrows       determines the number of rows read by pandas when
                        imputing the table datatypes. A large value results in
                        unnecessary data being read. A small value may result
                        in incorrect table datatype values.
            printable   returns the SQL command that would have been executed
                        as a printable string.
            **kwargs    Other arguments to be passed on to pandas read_csv.
        '''
        import pandas as pd
        from pathlib import Path
        from math import ceil
        from sqlalchemy.exc import InternalError
        # pandas is used to impute datatypes.
        df = pd.read_csv(file, nrows=nrows, **kwargs)
        tab = ' ' * self.tabspace
        # The file name will be used as the table name if not provided.
        if table is None:
            table = Path(file).stem
        if self.database:
            table = self.database + '.' + table

        def get_sql_dtypes(df):
            sql_dtype_dict = {}
            df_dtypes = [x for x in df.dtypes.apply(lambda x: x.name)]
            df = df.fillna('')
            # pandas dtypes are converted to sql dtypes to create the table.
            for i, col in enumerate(df.columns):
                if df_dtypes[i] in self.dtype_dic:
                    sql_dtype_dict[col] = self.dtype_dic[df_dtypes[i]]
                else:
                    # Determine VARCHAR length.
                    char_length = ceil(df[col].map(len).max() / 50) * 50
                    sql_dtype_dict[col] = f'VARCHAR({char_length})'
            return sql_dtype_dict

        command = f'CREATE TABLE {table}(\n{tab}'
        sql_dtype_dict = get_sql_dtypes(df)
        for col, sql_dtype in sql_dtype_dict.items():
            command = command + f'{col} {sql_dtype}\n{tab},'
        if pkey:
            if isinstance(pkey, str):
                pkey = [pkey]
            pkey = ', '.join(pkey)
            command += f'PRIMARY KEY({pkey})\n{tab},'
        command = command[:-(self.tabspace+1)] + ');'
        if printable or self.engine is None:
            return command
        try:
            self.engine.connect().execute(command)
            return f'table {table} created.'
        except InternalError as err:
            return err

    def csv_insert(self, file, table=None, updatekey=None, postgre=False, chunksize=None, sizelim=1073741824, printable=False, **kwargs):
        '''
        Convenience function that uploads file data into a database.
        params:
            file        path of file to be uploaded.
            updatekey   given the table's PRIMARY KEY, the function updates all
                        values in the table with those from the file except the
                        primary key. If a new table is created, updatekey is
                        set as the table's primary key.
            postgre     set to True if working on a PostgreSQL database. Only
                        relevant if not using sqlalchemy.
            table       if None, table = filename.
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
        from sqlalchemy.exc import IntegrityError
        import pandas as pd
        if table is None:
            table = Path(file).stem
        # Automatically set chunksize if file exceeds sizelim.
        if Path(file).stat().st_size >= sizelim and chunksize is None:
            chunksize = 100000
        if self.database:
            self.csv_table(file, table=table, pkey=updatekey)
            table = self.database + '.' + table

        def individual_insert(df, table=None):
            rows = [x for x in df.itertuples(index=False, name=None)]
            cols = ', '.join(df.columns)
            tab = ' ' * self.tabspace
            for r in rows:
                command = f'INSERT INTO {table}({cols}) VALUES '
                # Fix null values.
                pattern = r"([^\w'])nan([^\w'])"
                replacement = r'\1NULL\2'
                fixed_r = sub(pattern, replacement, f'{r}')
                command += f'{fixed_r}\n'
                if updatekey:
                    if postgre:
                        command = command[:-(self.tabspace+1)] + \
                            f'ON CONFLICT ({updatekey}) DO UPDATE SET\n{tab}'
                        for c in df.columns:
                            if c != updatekey:
                                command += f'{c}=excluded.{c}\n{tab},'
                    else:
                        command += f'ON DUPLICATE KEY UPDATE\n{tab}'
                        for c in df.columns:
                            if c not in updatekey:
                                command += f'{c}=VALUES({c})\n{tab},'
                    command = command[:-(self.tabspace+1)] + ';'
                try:
                    self.engine.connect().execute(command)
                except (InternalError, IntegrityError):
                    continue

        def alchemy_insert(df, updatekey=None, table=None):
            try:
                df.to_sql(table, self.engine, index=False, if_exists='append')
            except (InternalError, IntegrityError):
                individual_insert(df, table=table)
            if updatekey:
                try:
                    command = f'ALTER TABLE {table} ADD PRIMARY KEY({updatekey});'
                    self.engine.connect().execute(command)
                except InternalError as err:
                    return err

        def mass_insert(df, table=None, updatekey=None, postgre=False):
            rows = [x for x in df.itertuples(index=False, name=None)]
            cols = ', '.join(df.columns)
            tab = ' ' * self.tabspace
            command = f'INSERT INTO {table}({cols})\nVALUES\n{tab}'
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
            return command

        if chunksize:
            reader = pd.read_csv(file, chunksize=chunksize, **kwargs)
            for chunk in reader:
                df = pd.DataFrame(chunk)
                if printable:
                    with open('chunk_insert.txt', 'a') as f:
                        f.write(mass_insert(df, updatekey=updatekey,
                                            postgre=postgre, table=table))
                else:
                    alchemy_insert(df, updatekey=updatekey, table=table)
            if printable:
                return 'sql commands written to chunk_insert.txt'
            else:
                return f'data loaded into table {table}.'

        else:
            df = pd.read_csv(file, **kwargs)
            if printable:
                return mass_insert(df, updatekey=updatekey, postgre=postgre, table=table)
            alchemy_insert(df, updatekey=updatekey, table=table)
            return f'data loaded into table {table}.'

    def csvs_into_database(self, file_paths, table=None, clean_colnames=False, pkeys=None, **kwargs):
        '''
        Convenience function that uploads a folder of files into a database.
        params:
            file_paths      a string passed to the glob module which determines
                            what files to to upload. Normally in the format
                            './folder/*.extension'
            table       the table to upload the files into. Use when all
                            files are to be uploaded into a SINGLE TABLE.
            clean_colnames  standardizes and gets rid of potentially
                            problematic characters in column names. Warning:
                            replaces the column names in the original file.
            pkeys           accepts a list of PRIMARY KEYs to be assigned to
                            each table to be created. Must be given in file
                            alphabetical order as the files will be read in
                            alphabetical order. The list can be incomplete, but
                            any table not assigned a primary key should have ''
                            as its corresponding list item. Note that
                            alphabetical on atom can be different from the way
                            python processes the files if _ is in the file
                            name.
            **kwargs        optional arguments passed to pandas read_csv
                            function. na_values can be specified,
                            keep_default_na=False, low_memory=False are useful
                            arguments.
        '''
        import glob
        files = glob.glob(file_paths)
        has_incomplete_pkeys = False
        if pkeys:
            if isinstance(pkeys, str):
                pkeys = [pkeys]
            if table:
                for i, file in enumerate(files):
                    if clean_colnames:
                        self.csv_clean_colnames(file)
                    try:
                        self.csv_insert(file, table=table,
                                        updatekey=pkeys[i], **kwargs)
                    except TypeError or IndexError:
                        has_incomplete_pkeys = True
                        self.csv_insert(file, table=table, **kwargs)
            else:
                for i, file in enumerate(files):
                    if clean_colnames:
                        self.csv_clean_colnames(file)
                    try:
                        self.csv_insert(file, updatekey=pkeys[i], **kwargs)
                    except TypeError or IndexError:
                        has_incomplete_pkeys = True
                        self.csv_insert(file, **kwargs)
        else:
            if table:
                for file in files:
                    if clean_colnames:
                        self.csv_clean_colnames(file)
                    self.csv_insert(file, table=table, **kwargs)
            else:
                for file in files:
                    if clean_colnames:
                        self.csv_clean_colnames(file)
                    self.csv_insert(file, **kwargs)

        return_statement = f'files written to database {self.current_db()}.'
        if has_incomplete_pkeys:
            return_statement = 'not all tables have primary keys.\n' + return_statement
        return return_statement

    def show_tables(self, all=False, printable=False):
        if self.database:
            if all:
                command = f"SELECT TABLE_NAME FROM {self.database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
            else:
                command = f'SHOW TABLES FROM {self.database};'
        else:
            if all:
                command = f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE';"
            else:
                command = 'SHOW TABLES;'
        if printable or self.engine is None:
            return command
        import pandas as pd
        df = pd.read_sql_query(command, self.engine)
        return df

    def clone_table(self, target, new_table=None, cols=None, where=None, printable=False):
        from sqlalchemy.exc import InternalError
        if new_table is None:
            new_table = target + '_copy'
        command = f'CREATE TABLE {new_table} AS\n'
        if self.database:
            target = self.database + '.' + target
        if cols:
            if isinstance(cols, str):
                cols = [cols]
            cols = ', '.join(cols)
            command += f'SELECT {cols}\n'
        else:
            command += f'SELECT *\n'
        command += f'FROM {target}\n'
        if where:
            command += f'WHERE {where}\n;'
        else:
            command = command + ';'
        if printable or self.engine is None:
            return command
        try:
            self.engine.connect().execute(command)
        except InternalError as err:
            return err
        return f'table {target} cloned into table {new_table}.'

    def del_tables(self, tables, printable=False):
        from sqlalchemy.exc import InternalError
        tab = ' ' * self.tabspace
        command = 'DROP TABLES'
        if isinstance(tables, str):
            tables = [tables]
        if self.database:
            for i, table in enumerate(tables):
                tables[i] = self.database + '.' + table
        if len(tables) == 1:
            command += f' {tables};'
            return_string = tables
        else:
            command += f'\n{tab}'
            for table in tables:
                command += f'{table}\n{tab},'
            command = command[:-(self.tabspace+1)] + ';'
        if printable or self.engine is None:
            return command
        try:
            self.engine.connect().execute(command)
        except InternalError as err:
            return err
        return_string = ', '.join(tables)
        return f'table(s) {return_string} deleted.'

    def show_columns(self, table, all=False, printable=False):
        if self.database:
            table = self.database + '.' + table
        if all:
            command = f'SHOW ALL COLUMNS FROM {table};'
        else:
            command = f'SHOW COLUMNS FROM {table};'
        if printable or self.engine is None:
            return command
        import pandas as pd
        df = pd.read_sql_query(command, self.engine)
        return df

    def insert_columns(self, to_table, from_table, cols=None, where=None, printable=False):
        from sqlalchemy.exc import InternalError
        if self.database:
            to_table = self.database + '.' + to_table
        command = f'INSERT INTO {to_table}\n'
        if cols is None:
            cols = '*'
        elif isinstance(cols, str):
            cols = [cols]
            cols = ', '.join(cols)
        else:
            cols = ', '.join(cols)
        command += f'SELECT {cols}\nFROM {from_table}\n'
        if where:
            command += f'WHERE {where}\n;'
        else:
            command += ';'
        if printable or self.engine is None:
            return command
        try:
            self.engine.connect().execute(command)
        except InternalError as err:
            return err
        return_string = ', '.join(cols)
        return f'column(s) {return_string} inserted into {to_table} from {from_table}.'

    def del_columns(self, table, cols, if_exists=True, printable=False):
        from sqlalchemy.exc import InternalError
        if self.database:
            table = self.database + '.' + table
        command = f'ALTER TABLE {table}\n'
        if isinstance(cols, str):
            cols = [cols]
        if if_exists:
            drop_statement = 'DROP COLUMN IF EXISTS'
        else:
            drop_statement = 'DROP COLUMN'
        for col in cols:
            command += f'{drop_statement} {col}\n'
        command += ';'
        if printable or self.engine is None:
            return command
        try:
            self.engine.connect().execute(command)
        except InternalError as err:
            return err
        return_string = ', '.join(cols)
        return f'column(s) {return_string} deleted.'
