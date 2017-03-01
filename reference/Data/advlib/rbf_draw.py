import MySQLdb
from scipy import *
from scipy.linalg import norm, pinv
import numpy as np
from matplotlib import pyplot as plt


class RBF:

    def __init__(self, indim, numCenters, outdim):
        self.indim = indim
        self.outdim = outdim
        self.numCenters = numCenters
        self.centers = [random.uniform(-1, 1, indim) for i in xrange(numCenters)]
        self.beta = 8
        self.W = random.random((self.numCenters, self.outdim))

    def _basisfunc(self, c, d):
        print len(d)
        print self.indim
        assert len(d) == self.indim
        return exp(-self.beta * norm(c-d)**2)

    def _calcAct(self, X):
        # calculate activatppions of RBFs
        G = zeros((X.shape[0], self.numCenters), float)
        # print G
        for ci, c in enumerate(self.centers):
            for xi, x in enumerate(X):
                print x
                G[xi, ci] = self._basisfunc(c, x)
        return G

    def train(self, X, Y):
        """ X: matrix of dimensions n x indim
            y: column vector of dimension n x 1 """

        # choose random center vectors from training set
        rnd_idx = random.permutation(X.shape[0])[:self.numCenters]
        self.centers = [X[i,:] for i in rnd_idx]

        # print "center", self.centers
        # calculate activations of RBFs
        G = self._calcAct(X)
        # print G

        # calculate output weights (pseudoinverse)
        self.W = dot(pinv(G), Y)

    def test(self, X):
        """ X: matrix of dimensions n x indim """
        G = self._calcAct(X)
        Y = dot(G, self.W)
        return Y


def draw(id):
    db = MySQLdb.connect("572b2568442c7.sh.cdb.myqcloud.com", "cdb_outerroot", "software2015", "test",
                                     port=8161)
    cursor = db.cursor()
    cursor.execute('select `close`, `date` from `stock_2014` where `amount` >0  and `stockid` = "' + id + '"')
    raw = list(cursor.fetchall())
    base = []
    for each in raw:
        base.append(each[0])
    cursor.execute('select `close`, `date` from `stock_2015` where `amount` >0  and `stockid` = "' + id + '"')
    raw = list(cursor.fetchall())
    actual = []
    for each in raw:
        actual.append(each[0])
    predict = []
    X = []
    j = 1
    for each in actual:
        X.append(j)
        j += 1
        trainX = []
        trainY = []
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
            # sum = 0
            # # for every in tempX:
            # #     sum += every
            # for k in range(0, len(tempX)):
            #     # tempX[k] = float(tempX[k]) / float(sum)
            #     if tempX[k] >= 0:
            #         tempX[k] = 1
            #     else:
            #         tempX[k] = 0
            # tempY = float(tempY) / float(sum)
            trainX.append(tempX)
            trainY.append(tempY)
        testX = base[-10:]
        # sum = 0
        # for every in testX:
        #     sum += every
        # for i in range(0, len(testX)):
        #     testX[i] = float(testX[i]) / float(sum)
        # predict.append(get_predict(trainX, trainY, testX))
        rbf = RBF(10, 10, 1)
        # print np.array(trainX).T
        # print np.array([trainY])
        rbf.train(np.array(trainX).T, np.array(trainY))
        z = rbf.test(testX)
        predict.append(z)
        base.append(each)

    for x in range(0, 14):
        base.append(predict[-1])
        X.append(j)
        j += 1
        trainX = []
        trainY = []
        for x in range(0, len(base) - 10):
            tempX = base[x:x + 10]
            tempY = base[x + 10]
            # sum = 0
            # for every in tempX:
            #     sum += every
            # for k in range(0, len(tempX)):
            #     tempX[k] = float(tempX[k]) / float(sum)
            # tempY = float(tempY) / float(sum)
            trainX.append(tempX)
            trainY.append(tempY)
        testX = base[-10:]
        rbf = RBF(10, 20, 1)
        rbf.train(np.array(trainX).T, np.array(trainY))
        z = rbf.test(testX)
        predict.append(z)
        # sum = 0
        # for every in testX:
        #     sum += every
        # for i in range(0, len(testX)):
        #     testX[i] = float(testX[i]) / float(sum)

        # predict.append(get_predict(trainX, trainY, testX))

    print actual
    print predict
    print X
    # x = mgrid[-1:1:complex(0,n)].reshape(n, 1)
    # # set y and add random noise
    # y = sin(3*(x+0.5)**3 - 1)
    # # y += random.normal(0, 0.1, y.shape)
    # print x
    # print y
    # # rbf regression
    # rbf = RBF(1, 10, 1)
    # rbf.train(x, y)
    # z = rbf.test(x)
    #
    # # plot original data
    # plt.figure(figsize=(12, 8))
    # plt.plot(x, y, 'k-')
    #
    # # plot learned model
    # plt.plot(x, z, 'r-', linewidth=2)
    #
    # # plot rbfs
    # # plt.plot(rbf.centers, zeros(rbf.numCenters), 'gs')
    #
    # for c in rbf.centers:
    #     # RF prediction lines
    #     cx = arange(c-0.7, c+0.7, 0.01)
    #     cy = [rbf._basisfunc(array([cx_]), array([c])) for cx_ in cx]
    #     plt.plot(cx, cy, '-', color='gray', linewidth=1)
    #
    # plt.xlim(-1.2, 1.2)
    # plt.show()
draw('sh600000')