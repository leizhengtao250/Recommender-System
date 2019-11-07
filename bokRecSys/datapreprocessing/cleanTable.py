'''
清洗user,book,user_book,book_labels,user_book_score五张表
    清洗逻辑为：
        1. 对book_labels表清洗规则如下：
            1.1 再重复做一次去除四个汉字以上的标签
            1.2 再做一次化繁体为简体
            1.3 去掉字典（词袋）中不存在的标签
        2. 对book表清洗规则如下：
            2.1 基于1的清洗工作,去除没有标签的book
        3. 对于user_book表的清洗规则如下：
            3.1 去除用户读过书中没有标签的图书，即为去除不在book表中的bookid
        4. 对user表的清洗规则如下：
            4.1 基于3的清洗工作后，有些用户存在"没有"读过书(读过的书没有标签，或已被清洗掉),去除这些用户,
            即user表中的userid必须要在user_book表中的userid集合中。
        5. 对user_book_score表的清洗规则如下：
            5.1 去除userid 不在user表中的userid的集合中
            5.2 去除bookid 不在book表中的bookid的集合中

清洗完成后,新建数据库 recmendbookSys ,分别创建五张表 user,book,user_book,book_labels,user_book_score
'''

import synonyms as syn
import pymysql
import pandas as pd
import numpy as np
import re
from tool import Traditional2simple as trans
from DatabaseOp import connDb as con
from DatabaseOp import df2mysql as dbop
from sklearn.cluster import KMeans
from datapreprocessing import selectK

def dbInit():
    # 连接数据库
    conn=con.connection()
    cur = conn.cursor()
    #先选中一轮预清洗后的recsys本地数据库
    cur.execute('use recsys')
    return cur


def clean1_1(book_labels):
    # 1.1 再重复做一次去除四个汉字以上的标签
    print('book_label 1.1 清洗开始:',book_labels.shape,'---------------------')
    retainindex = []
    pattern = re.compile("^[\u4e00-\u9fa5]{2,4}$")
    for i in range(book_labels.shape[0]):
        if re.match(pattern,book_labels.iloc[i,1]):
           retainindex.append(i)
    book_labels = book_labels.iloc[retainindex]
    book_labels = book_labels.reset_index(drop=True)
    print('book_label 1.1 清洗结束:', book_labels.shape, '---------------------')
    return book_labels


def clean1_2(book_labels):
    print('book_label 1.2 清洗开始:',book_labels.shape,'---------------------------')
    # 1.2 再做一次化繁体为简体
    for i in range(book_labels.shape[0]):
        book_labels.iloc[i,1] = trans.Traditional2Simplified(book_labels.iloc[i,1])
    print('book_label 1.2 清洗结束:', book_labels.shape, '---------------------------')
    return book_labels


def clean1_3(book_labels):
    print('book_label 1.3 清洗开始:', book_labels.shape, '---------------------------')
    # 1.3 去掉字典（词袋）中不存在的标签
    indexbyWordBag = []
    labelsvector = []
    for i in range(book_labels.shape[0]):
        try:
            labelvector = syn.v(book_labels.iloc[i, 1])
            # print(labelvector,len(labelvector))
            # 若词袋中存在该词，保留index
            labelsvector.append(labelvector)
            indexbyWordBag.append(i)
        except KeyError as err:
            # 若词袋中不存在该词，删除
            continue
    book_labels = book_labels.iloc[indexbyWordBag]
    book_labels = book_labels.reset_index(drop=True)
    labelvector = pd.DataFrame(labelsvector)
    book_labels = pd.concat([book_labels,labelvector],ignore_index=True,axis=1)
    print('book_label 1.3 清洗结束:', book_labels.shape, '---------------------------') # 10118,102
    return book_labels


def cleanbook_lablesTable():
    #得到数据库游标
    cur=dbInit()
    cur.execute('select * from book_labels')
    data =cur.fetchall()
    book_labels=pd.DataFrame(data,columns=['bookid','booklabel'])
    print('--------------------------book_label开始清洗:',book_labels.shape,'-------------------------------------')
    # 先去重复记录
    book_labels = book_labels.loc[book_labels.duplicated(['bookid','booklabel']) == False]
    print('--------------------------book_label去重复记录后:', book_labels.shape, '---------------------------------')
    # 1.1 再重复做一次去除四个汉字以上的标签
    book_labels = clean1_1(book_labels)

    # 1.2 繁体化简体
    book_labels = clean1_2(book_labels)

    # 1.3 去掉字典（词袋）中不存在的标签
    book_labels = clean1_3(book_labels)

    return book_labels

def getAllbooksid(book_labels):
    booksid = list(book_labels.groupby('bookid').groups.keys())
    # print(len(booksid)) #2133本有标签的图书
    return booksid

def getAllbookVector(book_labels):
    # 得出图书id列表
    bookvector = book_labels.groupby('bookid').mean()
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)
    # print(bookvector)
    # print(len(bookvector))
    # print(bookvector)
    return bookvector


def cleanbookTable(booksid):
    #得到数据库游标
    cur = dbInit()
    cur.execute('select * from book')
    data = cur.fetchall()
    book = pd.DataFrame(data,columns=['bookid','bookname','bookauthor','bookcover'])
    print('-------------------book表清洗前:',book.shape,'-----------------------------')
    # 对book表去重，即为一条记录存了多次
    book = book.loc[book.duplicated('bookid') == False] # 去重后为2149
    book = book.reset_index(drop=True)
    print('book 表去重复记录后:',book.shape,'------------------------------------------')
    # 去除没有标签的图书
    book = pd.merge(booksid,book,on='bookid',how='left')  # 去重后图书为2133
    print('book 表去除没有标签的图书后:',book.shape,'--------------------------------------')
    book = book.dropna(how='any',axis=0)
    print('book 表去除没有标签的图书产生的nan:',book.shape,'-------------------------------') # 2099*4
    return book

def addbookclass(book, bookclass):
    '''
    在book表新加列 bookclass 表示图书的分类
    :param book:
    :param bookclass:
    :return:
    '''
    bookclass = pd.DataFrame(bookclass,columns=['bookclass'])
    book = pd.concat([book,bookclass],axis=1)

    return book

def cleanuser_bookTable(booksid):
    '''
    清洗user_book表
    :return:
    '''
    cur = dbInit()
    cur.execute('select * from user_book')
    data = cur.fetchall()
    user_book = pd.DataFrame(data, columns=['userid', 'bookid']) # 未开始清洗前 2268 row
    print('--------------user_book清洗前', user_book.shape, '----------------------------')
    user_book = user_book.loc[user_book.duplicated(['userid','bookid']) ==False]
    print('-----user_book 表去重复记录后----:',book.shape,' ------------------------------')
    user_book = pd.merge(booksid, user_book, on='bookid', how='left') # 清洗后 2262 row
    print('--------------user_book表去除不合理的userid后', user_book.shape, '--------------')
    user_book = user_book.dropna(how='any',axis=0)
    # user_book = user_book.fillna('No')
    # delindex = []
    # for i in range(user_book.shape[0]):
    #     if user_book.iloc[i,1] == 'No':
    #         delindex.append(i)
    # user_book = user_book.drop(delindex)
    # user_book = user_book.reset_index(drop=True)
    print('--------------user_book去userid产生的nan后', user_book.shape, '---------------------') # 2216*3


    return user_book

def cleanuserTable(user_book):
    # 得到非重复的userid
    usersid = user_book.groupby('userid').groups.keys() # 清洗后219个不重复的userid
    usersid = pd.DataFrame(usersid,columns=['userid'])
    cur = dbInit()
    cur.execute('select * from user')
    data = cur.fetchall()
    user = pd.DataFrame(data,columns=['userid','username','userlink'])
    print('-----user 表清洗前----', user.shape, '------------------------------')
    # 先对user表去重，一条记录存了两次或者多次
    user = user.loc[user.duplicated('userid') == False]
    print('-----user 表去重复记录后----:',user.shape,'--------------------------')
    user = pd.merge(usersid,user,on='userid',how='left')
    print('----user 表去不合理的userid后',user.shape,'--------------------------')

    # user = user.fillna('No')
    # delindex = []
    # for i in range(user.shape[0]):
    #     if user.iloc[i,1] == 'No':
    #         delindex.append(i)
    # user = user.drop(delindex)
    user = user.dropna(how='any',axis= 0)
    user = user.reset_index(drop=True)
    print('--------------user 表去userid产生的nan 后', user.shape, '-------------') #219*3
    return user

def cleanuser_book_scorelablesTable(user,book):
    # 得到usersid
    usersid = user['userid']
    # 得到booksid
    booksid = book['bookid']

    cur = dbInit()
    cur.execute('select * from user_book_score')
    data = cur.fetchall()
    user_book_score = pd.DataFrame(data,columns=['userid','bookid','score'])

    print('--------清洗前的user_book_score',user_book_score.shape,'----------------------')
    # 先去掉重复的记录
    user_book_score = user_book_score.loc[user_book_score.duplicated(['userid','bookid']) == False]
    print('--------user_book_score去重复记录后：', user_book_score.shape, '----------------------')

    # 去掉user_book_score 不包含在user 表中的userid 集合中的中的userid
    user_book_score = pd.merge(usersid,user_book_score,on='userid',how='left')
    print('--------user_book_score去不合理的userid后：', user_book_score.shape, '----------------------')
    # 去掉某行
    # user_book_score = user_book_score.fillna('NO')
    # delindex = []
    # for i in range(user_book_score.shape[0]):
    #     if user_book_score.iloc[i,1] == 'No':
    #         delindex.append(i)
    # user_book_score = user_book_score.drop(delindex)
    # user_book_score = user_book_score.reset_index(drop=True)

    user_book_score = user_book_score.dropna(how='any',axis=0)
    print('--------user_book_score去userid产生的nan 后：', user_book_score.shape, '----------------------')
    user_book_score = user_book_score.reset_index(drop=True)
    user_book_score = pd.merge(booksid,user_book_score,on='bookid',how='left')
    print('--------user_book_score去不合理的bookid后：', user_book_score.shape, '----------------------')
    user_book_score = user_book_score.dropna(how='any',axis=0)
    print('--------user_book_score去bookid产生的nan 后：', user_book_score.shape, '----------------------')
    return user_book_score



if __name__ == '__main__':

    # 1. 对book_labels 表进行清洗
    book_labels=cleanbook_lablesTable()
    print('----------------------------book_labels 清洗结束',book_labels.shape,'------------------------------------')
    # 对book_labels 表格调整格式
    v=['v'+str(i) for i in range(100)]
    v.insert(0,'booklable')
    v.insert(0,'bookid')
    book_labels.columns = v
    # 存入recommendbookSys 数据库中去
    dbop.storeRecommendBookSys(book_labels,'book_labels')

    # 得到清洗干净 book_labels中的 bookid
    booksid = getAllbooksid(book_labels) # list类型
    booksid = pd.DataFrame(booksid,columns=['bookid']) # 转为DataFrame类型


    # 2.对book 表进行清洗
    book = cleanbookTable(booksid)
    print('---------------------------------book 表清洗结束:',book.shape,'-------------------------------------------')
    #存入recommendbooksys 数据库中去
    dbop.storeRecommendBookSys(book,'book')

    # 得到每本图书的图书向量（bookvector)
    book_vector = getAllbookVector(book_labels)
    #生成一张新表 book_vector 表,存入recommendbooksys数据库中去
    dbop.storeRecommendBookSys(book_vector,'book_vector')

    # # 通过Kmeans算法给图书分类
    # # 先通过"肘"方法得到一个合适的K值
    selectK.getAppropriateK(book_vector,20)
    # # 通过“肘”方法我们发现 K 取8 比较合适

    # 根据图书向量对图书做聚类，n_clusters 通过‘肘’算法取8比较合适
    kmeans = KMeans(n_clusters=8, max_iter=500).fit(book_vector)
    bookclass = kmeans.predict(book_vector)
    # 将图书聚类的结果添加到book 表中去(再book表中添加新列 bookclass)
    book = addbookclass(book,bookclass)
    print('--------book 表更新修改结束:-------------',book.shape,'------------------------')
    dbop.storeRecommendBookSys(book, 'book')

    # 3.对user_book 表进行清洗
    user_book = cleanuser_bookTable(booksid)
    print('----------------------------user_book 清洗结束', user_book.shape, '------------------------------------')
    dbop.storeRecommendBookSys(user_book, 'user_book')

    # 4. 对user表的清洗规则如下:
    # 4.1 基于3的清洗工作后，有些用户存在
    # 读过书(读过的书没有标签，或已被清洗掉), 去除这些用户,
    # 即user表中的userid必须要在user_book表中的userid集合中。
    user = cleanuserTable(user_book)
    print('------------------------------user 表清洗结束',user.shape, '--------------------------------------')
    dbop.storeRecommendBookSys(user, 'user')

    # 5. 对user_book_score表的清洗规则如下：
    # 5.1 去除userid,不在user表中的userid的集合中
    # 5.2去除bookid,不在book表中的bookid的集合中
    user_book_score =cleanuser_book_scorelablesTable(user, book)
    print('------------------------------user_book_score清洗结束', user_book_score.shape, '-----------------------------')
    dbop.storeRecommendBookSys(user_book_score,'user_book_score')








