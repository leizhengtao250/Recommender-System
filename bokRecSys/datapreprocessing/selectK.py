import numpy as np
from sklearn.cluster import KMeans
from scipy.spatial.distance import cdist
import matplotlib.pyplot as plt

def getAppropriateK(X,classes):
    '''
    我们给我们的图书做Kmeans分类时,选择合适的K值
    :param X:
    :param classes:
    :return: prok
    '''
    K = range(1, classes) # 假设可能聚成 1~classes[20] 类
    lst = []
    for k in K:
        kmeans = KMeans(n_clusters = k)
        kmeans.fit(X) # X为2133 * 100 的大矩阵
        # 计算对应 K 值时最小值列表和的平均值
        lst.append(sum(np.min(cdist(X,kmeans.cluster_centers_,'euclidean'), axis = 1)) / X.shape[0])
        # cdist(X, kmeans.cluster_centers_, 'euclidean') 求 X 到各质心
        #cluster_centers_之间的距离平方和， 维度为(150, k), 'euclidean'表示使用
        #欧式距离计算
        # np.min(cdist(X,kmeans.cluster_centers_, 'euclidean'), axis = 1) 计
        #算每一行中的最小值
        # sum(np.min(cdist(X,kmeans.cluster_centers_, 'euclidean'), axis = 1))
        #计算每一轮 K 值下最小值列表的和
        # print(lst)
    plt.plot(K, lst, 'bo-')
    plt.title('Elbow method')
    plt.xlabel("K")
    plt.ylabel("Cost function")
    plt.show()