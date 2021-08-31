# Solution handles feature selection and scaling.


class Solution:

    def __init__(self):
        pass

    def cramers_corr(self, df):
        '''
        Takes a DataFrame of categorical variables and returns a DataFrame of
        the correlation matrix based on the Cramers V statistic. Uses
        correction from Bergsma and Wicher, Journal of the Korean Statistical
        Society 42 (2013): 323-328

        Does not require variables to be label encoded before use.
        '''
        import numpy as np
        import pandas as pd
        import scipy.stats as stats
        from itertools import combinations

        def cramers_v(x, y):
            con_table = pd.crosstab(x, y)
            chi2 = stats.chi2_contingency(con_table)[0]
            n = con_table.sum().sum()
            phi2 = chi2/n
            r, k = con_table.shape
            phi2corr = max(0, phi2-((k-1)*(r-1))/(n-1))
            rcorr = r-((r-1)**2)/(n-1)
            kcorr = k-((k-1)**2)/(n-1)
            return np.sqrt(phi2corr/min((kcorr-1), (rcorr-1)))

        cols = df.columns
        n_cols = len(cols)
        corr_matrix = np.zeros((n_cols, n_cols))
        for col1, col2 in combinations(cols, 2):
            i1, i2 = cols.get_loc(col1), cols.get_loc(col2)
            corr_matrix[i1, i2] = cramers_v(df[col1], df[col2])
            corr_matrix[i2, i1] = corr_matrix[i1, i2]
        np.fill_diagonal(corr_matrix, 1.0)
        df_corr_matrix = pd.DataFrame(corr_matrix, index=cols, columns=cols)
        return df_corr_matrix

    def vif_feature_select(self, df, max_score=5.0, inplace=False, printable=False, _drops=None):
        '''
        Takes a DataFrame and returns it after recursively eliminating columns
        with the highest VIF scores until the remainder have VIF scores less
        than max_score.

        params:
            printable   when set to True, the function returns a list of
                        features that would be dropped instead.
        '''
        import numpy as np
        # Avoids overwriting the original DataFrame by default.
        if inplace is False:
            df = df.copy()
        # Creates an empty list for the first iteration.
        if _drops is None:
            _drops = []
            # Check if features contain string values.
            dtypes = [dt for dt in df.dtypes]
            if 'object' in dtypes:
                print('Feature(s) contain string values. Result may be unreliable.')
            # Check if any feature contains all 0s.
            if (df == 0).all().any():
                raise Exception(
                    'Feature(s) contain all 0s. Drop them before use.')
        features = df.columns
        # VIF is the diagonal of the correlation matrix.
        vifs = np.linalg.inv(df.corr().values).diagonal()
        max_vif_index = np.argmax(vifs)
        # Eliminate feature with the highest VIF score and rerun the function.
        if vifs[max_vif_index] >= max_score:
            _drops.append(features[max_vif_index])
            del df[features[max_vif_index]]
            return self.vif_feature_select(df, max_score, inplace, printable, _drops)
        else:
            # Returns a list of features that would be dropped instead of a
            # DataFrame
            if printable:
                return _drops
            else:
                return df
