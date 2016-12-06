from utils import *


# Number of revisions and ratio between train:test desired
num_revisions = 10000 
train_test_split = num_revisions * 3 / 4

# Process csv and convert non numerical data
data_chunk = get_data_chunk(num_revisions=num_revisions)
X, Y = process_chunk(data_chunk)

# Split data accordingly and replace unknown values
train_X = fix_unknowns(X[:train_test_split])
train_Y = Y[:train_test_split]

test_X = fix_unknowns(X[train_test_split:])
test_Y = Y[train_test_split:]

model = LogisticRegression()
model = model.fit(train_X, train_Y)

print model.score(test_X, test_Y)
