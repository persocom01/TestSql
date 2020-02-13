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

    def create_table_from_csv(self, file, primary_key=None, print_only=False, nrows=100):
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
                # The length of the string datatype is determined by the
                # maximum length of strings in the column.
                char_length = ceil(df[col].map(len).max() / 50) * 50
                sql_dtypes.append(f'VARCHAR({char_length})')
            command = command + f'\n{col} {sql_dtypes[i]},'
        if primary_key:
            command = command + f'\nPRIMARY KEY ({primary_key})\n);'
        else:
            command = command[:-1] + '\n);'
        if print_only:
            return command
        else:
            self.cursor.execute(command)
            return f'table {tablename} created.'

    def csv_into_database(self, file_paths, primary_keys=None):
        import glob
        for i, file in enumerate(glob.glob(file_paths)):
            self.create_table_from_csv(file)
