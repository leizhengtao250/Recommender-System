'''
基于用户的推荐系统
通过sim_user 表 聚类,对user进行聚类，user 表新增列 userclass
'''
from sklearn.cluster import KMeans
from DatabaseOp import connDb
from datapreprocessing import selectK
import pandas as pd
from DatabaseOp import df2mysql as dbop

def initDb():
    con = connDb.connRecbooksys()
    cur =con.cursor()
    cur.execute('use recommendbooksys')
    return cur

def userTable():
    '''
    :return:
    '''
    cur = initDb()
    cur.execute('select * from user')
    data = cur.fetchall()
    user = pd.DataFrame(data,columns=['userid','username','userlink'])
    return user

def clustersim_user():
    cur = initDb()
    cur.execute('select * from sim_user')
    data = cur.fetchall()
    sim_user = pd.DataFrame(data,columns=['userid',
                               'bc0','bc1','bc2','bc3','bc4','bc5','bc6','bc7',
                          'bcavgsc0','bcavgsc1','bcavgsc2','bcavgsc3','bcavgsc4','bcavgsc5','bcavgsc6','bcavgs17c7'])
    # sim_user.index = sim_user['userid']
    # sim_user = sim_user.iloc[:,1:]
    #print(sim_user)
    # print('------------------------------------------------------')
    # 采用‘肘’方法得出对用户聚类合适的K值
    #selectK.getAppropriateK(sim_user.iloc[:,1:],20) #得出k=12比较合适


    kmeans = KMeans(n_clusters=12).fit(sim_user.iloc[:,1:])
    pred = kmeans.predict(sim_user.iloc[:,1:])
    # print(type(pred),len(pred),pred)
    sim_user['userclass'] =pred

    user_class = sim_user.loc[:,['userid','userclass']]
    # print('------------------------------------------------------')
    # print(user_class)
    return user_class

def adduserclass(user,user_class):
    # user 219
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)
    user =pd.merge(user_class,user,on='userid',how='left')
    user = user.dropna(how='any',axis=0)
    return user

if __name__ == '__main__':
    user = userTable()
    user_class = clustersim_user()
    user = adduserclass(user,user_class)
    dbop.storeRecommendBookSys(user,'user')