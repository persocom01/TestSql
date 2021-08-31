# Yuri handles data plots.


class Yuri:

    def __init__(self, y_test=None, y_prob=None):
        self.classes = None

    def auc_score(self, y_test, y_prob):
        '''
        A wrapper on sklearn's roc_auc_score that makes it work even if the
        target is multi-categorical or has not been label encoded.

        The auc_score normally ranges between 0.5 and 1. Less than 0.5 makes
        the model worse than the baseline.

        The function assumes y_prob is ordered in ascending order for the
        target.
        '''
        from sklearn.preprocessing import label_binarize
        from sklearn.metrics import roc_auc_score
        # Gets all unique classes.
        classes = list(set(y_test))
        classes.sort()
        self.classes = classes
        n_classes = len(self.classes)
        is_multi_categorical = n_classes > 2
        # Avoids label_binarize if unnecessary.
        if is_multi_categorical:
            lb_test = label_binarize(y_test, classes=self.classes)
            auc_scores = []
            for i in range(n_classes):
                auc_scores.append(roc_auc_score(lb_test[:, i], y_prob[:, i]))
            # Returns the mean roc auc score.
            return auc_scores / n_classes
        else:
            try:
                y_prob = y_prob[:, 1]
                return roc_auc_score(y_test, y_prob)
            except IndexError:
                print('y_prob needs to have at least 2 columns.')
            except TypeError:
                lb_test = label_binarize(y_test, classes=self.classes)
                return roc_auc_score(lb_test, y_prob)

    def dt_auc_scores(self, X_train, X_test, y_train, y_test, param_grid, tree='dt', **kwargs):
        '''
        Returns the AUROC scores for the 3 most important parameters of a
        decision tree. It is used in conjunction with plot_auc to help
        visualize decision tree parameters.
        '''
        # Set tree model.
        if tree == 'dt':
            from sklearn.tree import DecisionTreeClassifier
            dt_type = DecisionTreeClassifier
        elif tree == 'rf':
            from sklearn.ensemble import RandomForestClassifier
            dt_type = RandomForestClassifier
        elif tree == 'et':
            from sklearn.ensemble import ExtraTreesClassifier
            dt_type = ExtraTreesClassifier
        else:
            raise Exception('unrecognized tree type.')
        # Sets hyperparameter.
        train_auc_scores = []
        test_auc_scores = []
        for key, value in param_grid.items():
            for v in value:
                if key == 'max_depth' or key == 'md':
                    dt = dt_type(max_depth=v, **kwargs)
                elif key == 'min_samples_split' or key == 'mss':
                    dt = dt_type(min_samples_split=v, **kwargs)
                elif key == 'min_samples_leaf' or key == 'msl':
                    dt = dt_type(min_samples_leaf=v, **kwargs)
                else:
                    raise Exception('unrecognized param.')
                dt.fit(X_train, y_train)
                y_prob_train = dt.predict_proba(X_train)
                train_auc_scores.append(self.auc_score(y_train, y_prob_train))
                y_prob = dt.predict_proba(X_test)
                test_auc_scores.append(self.auc_score(y_test, y_prob))
        return [train_auc_scores, test_auc_scores]

    def plot_auc(self, x, auc_scores, lw=2, title=None, xlabel=None, labels=None, fontsize=10, **kwargs):
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(**kwargs)
        for i, scores in enumerate(auc_scores):
            if labels is None:
                labels = range(len(auc_scores))
            ax.plot(x, scores, label=labels[i])
        ax.set_xlabel(xlabel)
        ax.set_ylabel('AUC score')
        ax.set_title(title)
        ax.legend(loc='best')
        plt.rc('font', size=fontsize)
        plt.show()
        plt.close()

    def plot_roc(self, y_test, y_prob, average='macro', mm=False, reverse_classes=False, lw=2, title=None, labels=None, fontsize=10, **kwargs):
        '''
        Plots Receiver Operating Characteristic (ROC) curves for predict_proba
        method for sklearn models.

        This function is built to make plotting of ROC curves for a model with
        multi-categorical targets painless. It takes the one vs all approach
        when plotting the ROC curve for each target class.

        params:
            average 'macro' accepts 3 possible arguments besides None. 'macro',
                    'micro' or 'both'. It determines whether and what kind of
                    mean ROC curve to plot for multi-categorical targets.
            mm      If set to True, makes the function capable of plotting
                    ROC curves of multiple binary target models in the same
                    figure. It will cause the function to treat y_prob as a
                    list of y_probs instead of the y_prob of a single model.
                    mm stands for multi model.
            labels  accepts a dictionary of column values mapped onto class
                    names. If the column values are simply integers, it is
                    possible to just pass a list.

        The function assumes y_prob is ordered in ascending order for the
        target.
        '''
        import numpy as np
        import matplotlib.pyplot as plt
        from sklearn.preprocessing import label_binarize
        from sklearn.metrics import roc_curve, auc
        from itertools import cycle
        from scipy import interp
        # Gets all unique classes.
        classes = list(set(y_test))
        classes.sort()
        self.classes = classes
        is_multi_categorical = len(self.classes) > 2
        lb_test = label_binarize(y_test, classes=self.classes)

        # Initialize graph.
        fig, ax = plt.subplots(**kwargs)

        if is_multi_categorical:

            # Compute ROC curve and ROC area for each class.
            fpr = {}
            tpr = {}
            roc_auc = {}
            for i, k in enumerate(self.classes):
                fpr[k], tpr[k], _ = roc_curve(lb_test[:, i], y_prob[:, i])
                roc_auc[k] = auc(fpr[k], tpr[k])

            if average == 'micro' or average == 'both':
                # Compute micro-average ROC curve and ROC area.
                fpr['micro'], tpr['micro'], _ = roc_curve(
                    lb_test.ravel(), y_prob.ravel())
                roc_auc['micro'] = auc(fpr['micro'], tpr['micro'])

                ax.plot(fpr['micro'], tpr['micro'], ':r',
                        label=f'micro-average ROC curve (area = {roc_auc["micro"]:0.2f})', lw=lw)

            if average == 'macro' or average == 'both':
                # Compute macro-average ROC curve and ROC area.

                # First aggregate all false positive rates.
                all_fpr = np.unique(np.concatenate(
                    [fpr[k] for k in self.classes]))

                # Then interpolate all ROC curves at these points.
                mean_tpr = np.zeros_like(all_fpr)
                for k in self.classes:
                    mean_tpr += interp(all_fpr, fpr[k], tpr[k])

                # Finally average it and compute AUC.
                mean_tpr /= len(self.classes)

                fpr['macro'] = all_fpr
                tpr['macro'] = mean_tpr
                roc_auc['macro'] = auc(fpr['macro'], tpr['macro'])

                ax.plot(fpr['macro'], tpr['macro'], ':b',
                        label=f'macro-average ROC curve (area = {roc_auc["macro"]:0.2f})', lw=lw)

            # Plot ROC curve for each category.
            colors = cycle(['teal', 'darkorange', 'cornflowerblue'])
            if labels is None:
                labels = self.classes
            for k, color in zip(self.classes, colors):
                ax.plot(fpr[k], tpr[k], color=color,
                        label=f'ROC curve of {labels[k]} (area = {roc_auc[k]:0.2f})', lw=lw)

        else:

            def plot_single_roc(lb_test, y_prob, labels, i='target'):
                y_prob = y_prob[:, 1]
                fpr, tpr, _ = roc_curve(lb_test, y_prob)
                roc_auc = auc(fpr, tpr)
                if labels is None:
                    labels = f'class {i}'
                else:
                    labels = labels[i]
                ax.plot(
                    fpr, tpr, label=f'ROC curve of {labels} (area = {roc_auc:0.2f})', lw=lw)

            # Allows plotting of multiple binary target ROC curves in the same
            # figure.
            if mm:
                for i in range(len(y_prob)):
                    plot_single_roc(lb_test, y_prob[i], labels, i)
            else:
                plot_single_roc(lb_test, y_prob, labels)

        # Plot the curve of the baseline model (mean).
        ax.plot([0, 1], [0, 1], 'k--')
        ax.set_xlim([0.0, 1.0])
        ax.set_ylim([0.0, 1.05])
        ax.set_xlabel('False Positive Rate')
        ax.set_ylabel('True Positive Rate')
        ax.set_title(title)
        ax.legend(loc='best')
        plt.rc('font', size=fontsize)
        plt.show()
        plt.close()
