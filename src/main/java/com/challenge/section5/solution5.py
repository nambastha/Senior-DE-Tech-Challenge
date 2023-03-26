import pandas as pd
import category_encoders as ce
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
from sklearn.tree import DecisionTreeClassifier
import scikitplot.metrics as skplt
import matplotlib.pyplot as plt
from sklearn import tree
from sklearn.model_selection import GridSearchCV

# Load dataset
data = pd.read_csv('/Users/ambastha/IdeaProjects/Senior-DE-Tech-Challenge/src/main/java/com/challenge/section5/data'
                   '/car.data')

# adding headings to the dataset
# creating a list of necessary headings
header = ['buying', 'maintainance_cost', 'total_doors', 'seater', 'boot_space', 'safety', 'decision']
data.columns = header

print(data)

# lets explore the given dataset
print("data shape: ", data.shape)

# checking for any missing value in the given dataset
print("data is null: ", data.isnull().sum())

# counting the occurrence of each value in decision column
data.decision.value_counts()

# Segregating datasets into features and target datasets

# dropping last column(target label) and selecting rest
X = data.drop('decision', axis=1)

# selecting only target variable
Y = data['decision']

print(X.shape, Y.shape)

# encoding the categorical features
encoder = ce.OrdinalEncoder(cols=['buying', 'maintainance_cost', 'total_doors', 'seater', 'boot_space', 'safety'])
x = encoder.fit_transform(X)

# "safety" column in both the datasets for comparison LOW in original data set is encoded as 1, MED as 2 and High as 3

print("x head: ", x.head())
print("x shape: ", x.shape)

print('Original dataset')
print('\n\n', X.head())
print('\n Encoded dataset')
print('\n', x.head())

# Let's break the dataset into 3 parts Train,Cross-validation and Test datasets. One can simply split the dataset into
# train and test data sets only but splitting into two is prone to data leakage. So, the idea behind splitting the
# data sets into three parts is to avoid data leakage and to make our test data totally unseen from the model.This
# brings more generalization to our model accuracy.

x_1, xtest, y_1, ytest = train_test_split(x, Y, test_size=0.3, random_state=2)
xtrain, x_cv, ytrain, y_cv = train_test_split(x_1, y_1, test_size=0.3, random_state=2)

# Exploring class distribution under train ,cross validation and test dataset
print('Training Dataset', xtrain.shape, ytrain.shape)
print('\n Class label distribution in Training Set\n', ytrain.value_counts())
print('\n***********')
print("\n CrossValidation Dataset", x_cv.shape, y_cv.shape)
print('\nClass label distribution in Cross Validation Set\n', y_cv.value_counts())
print('\n***********')
print("\n Test Dataset", xtest.shape, ytest.shape)
print('\nClass label distribution in Test Set\n', ytest.value_counts())

# There is a class imbalance particularly in cross validation dataset for class label "good". Let us try to reduce
# this imbalance by giving weights to class labels while training our model.

parameters = {'max_depth': list(range(1, 30)),
              'min_samples_leaf': list(range(5, 200, 20)),
              'min_samples_split': list(range(5, 200, 20))
              }

# Using Grid Search for finding the best parameters for our decision tree model & checking for model classification
# accuracy on Cross Validation Dataset

model = GridSearchCV(DecisionTreeClassifier(class_weight='balanced'), parameters, n_jobs=-1, cv=10, scoring='accuracy')
model.fit(xtrain, ytrain)
print(model.best_estimator_)
print("\n", model.best_params_)
print("\n", model.score(x_cv, y_cv))

ypredict = model.predict(x_cv)
accuracy = accuracy_score(y_cv, ypredict, normalize=True) * float(100)
print('\n\n classification report')
print(classification_report(y_cv, ypredict))

# Using optimal value of hyper-parameters that we got via Grid search, predicting the class labels for the trest
# dataset and getting its classification report

clf = tree.DecisionTreeClassifier(class_weight='balanced', max_depth=9, min_samples_leaf=5, min_samples_split=5)
clf.fit(xtrain, ytrain)
ypredict = clf.predict(xtest)
accuracy = accuracy_score(ytest, ypredict, normalize=True) * float(100)
print('\n Accuracy score is', accuracy)
print('\n classification report')
print(classification_report(ytest, ypredict))

