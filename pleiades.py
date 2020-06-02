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

    # This function is meant to be used on boto3.resource objects.
    def get_keys(self, bucket_name, prefix='/', suffix=None, delimiter='/'):
        import re
        prefix = prefix[1:] if prefix.startswith(delimiter) else prefix
        bucket = self.cursor.Bucket(bucket_name)
        keys = [_.key for _ in bucket.objects.filter(Prefix=prefix)]
        for key in keys:
            if suffix:
                if not re.search(suffix, key):
                    keys.remove(key)
            if key[-1:] == delimiter:
                keys.remove(key)
        return keys

    def download_files(self, bucket_name, keys, savein=''):
        import re
        if isinstance(keys, str):
            keys = [keys]
        for key in keys:
            filename = re.search(r'/([^/]+)$', key, re.I)[1]
            file_path = savein + filename
            bucket = self.cursor.Bucket(bucket_name)
            bucket.download_file(key, file_path)
        return f'{len(keys)} files downloaded.'

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

    def csv_table(self, file, pkey=None, nrows=100000, printable=False, **kwargs):
        from pathlib import Path
        import pandas as pd
        from math import ceil
        if self.alchemy and printable is False:
            return 'csv_insert creates the necessary tables with sqlalchemy.'
        # The file name will be used as the table name.
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
        if printable or self.cursor is None:
            return command
        self.cursor.execute(command)
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
        import pandas as pd
        from re import sub
        if tablename is None:
            tablename = Path(file).stem
        # Automatically set chunksize if file exceeds sizelim.
        if Path(file).stat().st_size >= sizelim and chunksize is None:
            chunksize = 100000

        def alchemy_insert(df, updatekey=None, tablename=None):
            df.to_sql(tablename, self.cursor, index=False, if_exists='replace')
            if updatekey:
                command = f'ALTER TABLE {tablename} ADD PRIMARY KEY({updatekey});'
                self.cursor.execute(command)

        def cursor_insert(df, updatekey=None, postgre=False, tablename=None, printable=False):
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
            if printable or self.cursor is None:
                return command
            self.cursor.execute(command)

        if chunksize:
            reader = pd.read_csv(file, chunksize=chunksize, **kwargs)

            for chunk in reader:
                df = pd.DataFrame(chunk)
                if self.alchemy and printable is False:
                    alchemy_insert(df, updatekey=updatekey,
                                   tablename=tablename)
                if printable or self.cursor is None:
                    with open('chunk_insert.txt', 'a') as f:
                        f.write(cursor_insert(
                            df, updatekey=updatekey, postgre=postgre, tablename=tablename, printable=printable))
                else:
                    cursor_insert(df, updatekey=updatekey, postgre=postgre,
                                  tablename=tablename, printable=printable)

            if printable is None and self.cursor:
                return f'data loaded into table {tablename}.'

        else:
            df = pd.read_csv(file, **kwargs)

            if self.alchemy and printable is False:
                alchemy_insert(df, updatekey=updatekey, tablename=tablename)
                return f'data loaded into table {tablename}.'

            if printable or self.cursor is None:
                return cursor_insert(df, updatekey=updatekey, postgre=postgre, tablename=tablename, printable=printable)

            cursor_insert(df, updatekey=updatekey, postgre=postgre,
                          tablename=tablename, printable=printable)
            return f'data loaded into table {tablename}.'

    def csvs_into_database(self, file_paths, pkeys=None, printable=False, **kwargs):
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
                for i, file in enumerate(files):
                    try:
                        self.csv_table(file, pkeys[i], **kwargs)
                        self.csv_insert(file, pkeys[i], **kwargs)
                    except IndexError:
                        has_incomplete_pkeys = True
                        self.csv_table(file, **kwargs)
                        self.csv_insert(file, **kwargs)
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


# Nabe deals with data cleaning and exploration.


class Nabe:

    def __init__(self):
        self.null_dict = None
        self.steps = '''
        1. df.head()
        2. df.info()
        3. df.isnull().sum() or 1 - df.count() / df.shape[0]
        4. clean
        5. visualize correlations
        '''

    def get_null_indexes(self, df, cols=None):
        '''
        Takes a DataFrame and returns a dictionary of columns and the row
        indexes of the null values in them.
        '''
        # Prevents errors from passing a string instead of a list.
        if isinstance(cols, str):
            cols = [cols]

        null_indexes = []
        null_dict = {}
        if cols is None:
            cols = df.columns
        for col in cols:
            null_indexes = df[df[col].isnull()].index.tolist()
            null_dict[col] = null_indexes
        return null_dict

    # Drops columns with 75% or more null values.
    def drop_null_cols(self, df, null_size=0.75, inplace=False):
        if inplace is False:
            df = df.copy()
        null_table = 1 - df.count() / df.shape[0]
        non_null_cols = [i for i, v in enumerate(null_table) if v < null_size]
        df = df.iloc[:, non_null_cols]
        return df

    # Returns the row index of a column value.
    def get_index(self, df, col_name, value):
        if len(df.loc[df[col_name] == value]) == 1:
            return df.loc[df[col_name] == value].index[0]
        else:
            return df.loc[df[col_name] == value].index

# Lupusregina deals with word processing.


class Lupu:

    def __init__(self):
        # Contractions dict.
        self.contractions = {
            "n't": " not",
            "n’t": " not",
            "'s": " is",
            "’s": " is",
            "'m": " am",
            "’m": " am",
            "'ll": " will",
            "’ll": " will",
            "'ve": " have",
            "’ve": " have",
            "'re": " are",
            "’re": " are"
        }
        self.re_ref = {
            'email': r'([a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+)',
            'link': r'(https?://[^ ]+)',
            'gender_pronoun': [r'[hH]e/[hH]im', '[tT]hey/[tT]hem', '[tT]ey/[tT]em', '[eE]y/[eE]m', '[eE]/[eE]m', '[tT]hon/[tT]hon', '[fF]ae/[fF]aer', '[vV]ae/[vV]aer', '[aA]e/[aA]er', '[nN]e/[nN]ym', '[nN]e/[nN]em', '[xX]e/[xX]em', '[xX]e/[xX]im', '[xX]ie/[xX]em', '[zZ]e/[zZ]ir', '[zZ]ie/[zZ]ir', '[zZ]he/[zZ]hir', '[zZ]e/[hH]ir', '[sS]ie/[sS]ier', '[zZ]ed/[zZ]ed', '[zZ]ed/[zZ]ed', '[cC]e/[cC]ir', '[cC]o/[cC]os', '[vV]e/[vV]is', '[jJ]ee/[jJ]em', '[lL]ee/[lL]im', '[kK]ye/[kK]yr', '[pP]er/[pP]er', '[hH]u/[hH]um', '[bB]un/[bB]un', '[iI]t/[iI]t']
        }
        self.sep = ' '

    # Lowercase.
    def to_lower(self, sentence):
        return sentence.lower()

    # To tokenize is to split the sentence into words.
    def re_tokenize(self, sentence, sep=r'\w+'):
        from nltk.tokenize import RegexpTokenizer
        retoken = RegexpTokenizer(sep)
        words = retoken.tokenize(sentence)
        return words

    # Lemmatizing eliminates things like the s from plurals like apples.
    def lemmatize_sentence(self, sentence):
        from nltk.stem import WordNetLemmatizer
        wnlem = WordNetLemmatizer()
        words = self.re_tokenize(sentence)
        words = [wnlem.lemmatize(word) for word in words]
        # Returns sentence instead of individual words.
        return ' '.join(words)

    # Stemming is a more drastic approach than lemmatizing. It truncates words
    # to get to the root word.
    def stem_sentence(self, sentence):
        from nltk.stem.porter import PorterStemmer
        p_stem = PorterStemmer()
        words = self.re_tokenize(sentence)
        words = [p_stem.stem(word) for word in words]
        # Returns sentence instead of individual words.
        return ' '.join(words)

    def remove_punctuation(self, sentence, sep=None):
        import re
        if sep is None:
            sep = self.sep
        sentence = re.sub(
            r'[!"#$%&\'()*+, -./:; <= >?@[\]^_`{|}~’“”]', sep, sentence)
        return sentence

    def split_camel_case(self, sentence):
        import re
        splitted = re.sub('([A-Z][a-z]+)', r' \1',
                          re.sub('([0-9A-Z]+)', r' \1', sentence)).split()
        return ' '.join(splitted)

    def text_list_cleaner(self, text_list, *args, sep=None, inplace=False):
        '''
        Function made to make chain transformations on text lists easy.

        Maps words when passed functions or dictionaries as *arguments.
        Removes words when passed lists or strings.

        Aside from lists, all *arguments should be made in regex format, as the
        function does not account for spaces or word boundaries by default.
        '''
        import re
        if inplace is False:
            text_list = text_list.copy()
        if sep is None:
            sep = self.sep

        # Prevents KeyError from passing a pandas series with index not
        # beginning in 0.
        try:
            iter(text_list.index)
            r = text_list.index
        except TypeError:
            r = range(len(text_list))

        for i in r:
            for arg in args:
                # Maps text with a function.
                if callable(arg):
                    text_list[i] = arg(text_list[i])
                # Maps text defined in dict keys with their corresponding
                # values.
                elif isinstance(arg, dict):
                    for k, v in arg.items():
                        text_list[i] = re.sub(k, v, text_list[i])
                # Removes all words passed as a list.
                elif not isinstance(arg, str):
                    for a in arg:
                        pattern = r'\b{}\b'.format(a)
                        text_list[i] = re.sub(pattern, sep, text_list[i])
                # For any other special cases.
                else:
                    text_list[i] = re.sub(arg, sep, text_list[i])
        return text_list

    def word_cloud(self, text, figsize=(12.5, 7.5), max_font_size=None, max_words=200, background_color='black', mask=None, recolor=False, export_path=None, **kwargs):
        '''
        Plots a wordcloud.

        Use full_text = ' '.join(list_of_text) to get a single string.
        '''
        import numpy as np
        import matplotlib.pyplot as plt
        from PIL import Image
        from wordcloud import WordCloud, ImageColorGenerator

        fig, ax = plt.subplots(figsize=figsize)
        if mask:
            m = np.array(Image.open(mask))
            cloud = WordCloud(background_color=background_color,
                              max_words=max_words, mask=m, **kwargs)
            cloud.generate(text)
            if recolor:
                image_colors = ImageColorGenerator(mask)
                ax.imshow(cloud.recolor(color_func=image_colors),
                          interpolation='bilinear')
            else:
                ax.imshow(cloud, interpolation='bilinear')
        else:
            cloud = WordCloud(background_color=background_color,
                              max_words=max_words, **kwargs)
            cloud.generate(text)
            ax.imshow(cloud, interpolation='bilinear')
        if export_path:
            cloud.to_file(export_path)
        ax.axis('off')
        plt.show()
        plt.close()
