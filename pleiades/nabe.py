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

    # Drops columns with 75% or more null values.
    def drop_null_cols(self, df, null_size=0.75, inplace=False):
        if inplace is False:
            df = df.copy()
        null_table = 1 - df.count() / df.shape[0]
        non_null_cols = [i for i, v in enumerate(null_table) if v < null_size]
        df = df.iloc[:, non_null_cols]
        return df

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

    # Returns the row index of a column value.
    def get_index(self, df, col_name, value):
        if len(df.loc[df[col_name] == value]) == 1:
            return df.loc[df[col_name] == value].index[0]
        else:
            return df.loc[df[col_name] == value].index

    def ordinal_scale(self, df, cols=None, mapping=None, start_num=0):
        '''
        A convenience mapping function that accepts a DataFrame and returns it
        with the unique values in each column encoded as integers in
        alphabetical order.
        params:
            cols        accepts a list of columns to encode instead of all by
                        default.
            mapping     accepts a dictionary whose keys are the columns to
                        encode and whose values are lists of the order in which
                        values are to be encoded. Useful for values like
                        ['low', 'medium', 'high'] which make no sense when
                        encoded in alphabetical order.
            start_num   allows integer encoding to start with a number other
                        than 0.
        '''
        from sklearn.preprocessing import OrdinalEncoder
        if mapping:
            cols = mapping.keys()
            for col in cols:
                df[col] = df[col].map({k: i+start_num for i, k in enumerate(mapping[col])})
                if df[col].isnull().sum() > 0:
                    print(f'WARNING: not all values in column "{col}" were mapped.')
        else:
            ord = OrdinalEncoder()
            if not cols:
                cols = df.columns
            df[cols] = ord.fit_transform(df[cols])
        return df
