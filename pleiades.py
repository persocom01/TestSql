# CZ deals with databases.
# She pastes 1 yen stickers on things she likes.


class CZ:

    def __init__(self, cursor):
        self.cursor = cursor
        self.dtype_dic = {
            'int64': 'INT',
            'float64': 'DOUBLE',
            'bool': 'BOOLEAN',
            'datetime64': 'DATETIME'
        }

    def csv_table(self, file, pkey=None, printc=False, nrows=100):
        from pathlib import Path
        import pandas as pd
        from math import ceil
        # The filename will be used as the table name.
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
            command = command + f'\n{col} {sql_dtypes[i]},'
        if pkey:
            command = command + f'\nPRIMARY KEY ({pkey})\n);'
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
            command = command + f'\n{r},'
        command = command[:-1] + ';'
        if printc:
            return command
        else:
            self.cursor.execute(command)
            return f'data loaded into table {tablename}.'

    def csvs_into_database(self, file_paths, pkeys=None):
        import glob
        for i, file in enumerate(glob.glob(file_paths)):
            self.csv_table(file, pkeys[i])
            self.csv_insert(file)
