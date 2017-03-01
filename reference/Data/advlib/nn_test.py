import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import theano
import theano.tensor as T
import numpy as np

X = theano.shared(value=np.asarray([[1, 0], [0, 0], [0, 1], [1, 1]]), name='X')
y = theano.shared(value=np.asarray([[1], [0], [1], [0]]), name='y')
rng = np.random.RandomState(1234)
LEARNING_RATE = 0.01

def layer(n_in, n_out):
    return theano.shared(value=np.asarray(rng.uniform(low=-1.0, high=1.0, size=(n_in, n_out)), dtype=theano.config.floatX), name='W', borrow=True)

W1 = layer(2, 3)
W2 = layer(3, 1)

output = T.nnet.sigmoid(T.dot(T.nnet.sigmoid(T.dot(X, W1)), W2))
cost = T.sum((y - output) ** 2)
updates = [(W1, W1 - LEARNING_RATE * T.grad(cost, W1)), (W2, W2 - LEARNING_RATE * T.grad(cost, W2))]

train = theano.function(inputs=[], outputs=[], updates=updates)
test = theano.function(inputs=[], outputs=[output])

for i in range(1000):
    train()

print(test())


def get_draw_data(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                         port=8161)
    cursor = db.cursor()

    # end = datetime.datetime.now()
    # start = end + datetime.timedelta(days=-200)
    # enddate = end.strftime('%Y-%m-%d')
    # startdate = start.strftime('%Y-%m-%d')
    # startdate = '2015-05-01'
    # enddate = '2016-05-20'

    select_cmd = 'select `close` from `stock_2014` where `stockid` = "' + id + '" and `amount`>0 order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
    # select_cmd = 'select `close` from `stock_2015` where `stockid` = "' + id + '" and `amount`>0 and `date` < "'+date+'"order by `date`'
    select_cmd = 'select `close` from `stock_2015` where `stockid` = "' + id + '" and `amount`>0 order by `date`'
    cursor.execute(select_cmd)
    raw = list(cursor.fetchall())
    close = []
    for each in raw:
        if each[0] != 0:
            close.append(each[0])
# print close
    return close



# np.random.seed(0)
#
# # train_X, train_y = datasets.make_moons(300, noise=0.20)
# train_X = train_X.astype(np.float32)
# train_y = train_y.astype(np.float32)
# num_example=len(train_X)