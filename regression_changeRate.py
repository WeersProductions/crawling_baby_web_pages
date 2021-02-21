import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split, ShuffleSplit
from sklearn.model_selection import learning_curve
from sklearn.feature_selection import RFECV

from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import explained_variance_score, median_absolute_error, mean_absolute_error, r2_score, make_scorer

from data_exploration import read_dataset, plot_univariate_distribution
from feature_selection import filter_na, filter_variance, filter_correlation

def scale_features(X):
	scaler = StandardScaler()
	headers = X.columns.values
	X = pd.DataFrame(scaler.fit_transform(X), columns=headers)

	return X

def filter_RFECV(model, X, Y, feature_set):
	selector = RFECV(model, step=1, cv=5)
	selector = selector.fit(X, Y)
	reduced_feature_set = [feature_set[i] for i in range(len(feature_set)) if selector.support_[i]]

	return X[reduced_feature_set]

def just_fit(model, X_train, Y_train, X_test, Y_test):
	model.fit(X_train, Y_train)

	my_r2_score = model.score(X_test, Y_test)
	print("Test R2 score:", my_r2_score)

	Y_pred = model.predict(X_test)

	exvar_score = explained_variance_score(Y_test, Y_pred)
	print("Test Explained Variance score:", exvar_score)
	mae_score = mean_absolute_error(Y_test, Y_pred)
	print("Test Mean Absolute Error score:", mae_score)
	median_score = median_absolute_error(Y_test, Y_pred)
	print("Test Median Absolute Error score:", median_score)

def hyperparameter_tuning(model, name, tuned_params, X_train, Y_train, X_test, Y_test, cv=5, weights=None):

	print("\n"+name, "- hyperparameter tuning")

	my_scorer = make_scorer(r2_score)
	tuned_model = GridSearchCV(model, tuned_params, cv=cv, scoring=my_scorer)
	tuned_model.fit(X_train, Y_train)

	print("\nScores on the development set:\n")
	means = tuned_model.cv_results_['mean_test_score']
	stds = tuned_model.cv_results_['std_test_score']
	for mean, std, params in zip(means, stds, tuned_model.cv_results_['params']):
		print("mean %0.5f, stdev %0.05f for parameters %r" % (mean, std, params))

	print("\nBest parameters on the development set:", tuned_model.best_params_)
	model = tuned_model.best_estimator_

	my_r2_score = model.score(X_test, Y_test)
	print("Test R2 score:", my_r2_score)

	Y_pred = model.predict(X_test)

	exvar_score = explained_variance_score(Y_test, Y_pred)
	print("Test Explained Variance score:", exvar_score)
	mae_score = mean_absolute_error(Y_test, Y_pred)
	print("Test Mean Absolute Error score:", mae_score)
	median_score = median_absolute_error(Y_test, Y_pred)
	print("Test Median Absolute Error score:", median_score)

	return model

def plot_learning_curve(model, title, X, y, ylim=None, cv=3, n_jobs=4, train_sizes=np.linspace(.1, 1.0, 10)):
	# Source: https://scikit-learn.org/stable/auto_examples/model_selection/plot_learning_curve.html#sphx-glr-auto-examples-model-selection-plot-learning-curve-py

    plt.figure(figsize=(4, 3))
    plt.title(title)
    if ylim:
        plt.ylim(*ylim)
    plt.xlabel("Training set size")
    plt.ylabel("R2 score")

    from matplotlib.ticker import FormatStrFormatter
    plt.gca().yaxis.set_major_formatter(FormatStrFormatter('%.2f'))
    plt.gca().yaxis.set_ticks_position("both")

    my_scorer = make_scorer(r2_score)
    train_sizes, train_scores, test_scores = learning_curve(model, X, y, cv=cv, scoring=my_scorer, n_jobs=n_jobs, train_sizes=train_sizes)
    train_scores_mean = np.mean(train_scores, axis=1)
    train_scores_std = np.std(train_scores, axis=1)
    test_scores_mean = np.mean(test_scores, axis=1)
    test_scores_std = np.std(test_scores, axis=1)

    plt.fill_between(train_sizes, train_scores_mean - train_scores_std, train_scores_mean + train_scores_std, alpha=0.2, facecolor='#448CBE')
    plt.plot(train_sizes, train_scores_mean, 's-', markersize=4, color='#448CBE', label="Training")

    plt.fill_between(train_sizes, test_scores_mean - test_scores_std, test_scores_mean + test_scores_std, alpha=0.2, facecolor='#B72633')
    plt.plot(train_sizes, test_scores_mean,  'o-', markersize=4, color='#B72633', label="Cross-validation")

    plt.xticks(fontsize=8)
    plt.yticks(fontsize=8)

    plt.legend(loc="lower right", markerfirst=True, fontsize=9, frameon=False, labelspacing=0.2)
    plt.savefig("learning_curve-"+title+".png", dpi=500, bbox_inches='tight')

# _____________________________________________________________________________

if __name__ == '__main__':

	random_state = 0
	data_size = 700000
	which_model = 'RFR'
	which_features = 'SPN'
	static_page_features = ['pathDepth', 'domainDepth', 'contentLength', 'textSize', 'textQuality', 'internalOutLinks', 'externalOutLinks']
	static_network_features = ['internalInLinks', 'externalInLinks', 'trustRank']
	features_sets = {	'SN': static_network_features, 
						'SP': static_page_features, 
						'SPN': static_page_features + static_network_features
					}
	target = ['changeCount', 'fetchCount']

	hardcoded_models = {
		'RFR': RandomForestRegressor(n_estimators=50, min_samples_leaf=5)
	}
	untuned_models = {
		'RFR': (RandomForestRegressor(n_estimators=50), {'min_samples_leaf': [5, 10, 15], 'max_depth': [30]})
	}

	# Data reading, forming features and target
	XY = read_dataset("../changeRate_dataset.json", 
						['url'] + features_sets[which_features] + target, data_size) # pd.DataFrame
	Y = XY['changeCount'] / (XY['fetchCount'] - 1) # pd.Series
	# plot_univariate_distribution(Y, 9, 'changeRate', 'distribution_changeRate.pdf')
	X = XY.drop(['changeCount', 'fetchCount'], axis='columns')

	# Feature selection
	# X = filter_variance(X)	# this data doesn't need it
	# X = filter_correlation(X)	# this data doesn't need it
	# X = scale_features(X)		# some statistical models need it
	# X = filter_RFECV(hardcoded_models[which_model], X, Y, features_sets[which_features])
	# print(X.columns.values)

	X_train, X_test, Y_train, Y_test = train_test_split(X, Y, train_size=0.7, shuffle=True, random_state=random_state) 

	# (Either) Fit the model
	model = just_fit(hardcoded_models[which_model], X_train, Y_train, X_test, Y_test)

	# (Or) Hypertune parameters
	# model = hyperparameter_tuning(untuned_models[which_model][0], which_model, untuned_models[which_model][1], 
	# 			X_train, Y_train, X_test, Y_test, cv=5)

	# (Optional) Produce a learning curve
	# plot_learning_curve(hardcoded_models[which_model], which_features + " features-target changeRate", 
	#			X, Y, ylim=(0, 1), cv=ShuffleSplit(n_splits=5, test_size=0.25, random_state=random_state))
