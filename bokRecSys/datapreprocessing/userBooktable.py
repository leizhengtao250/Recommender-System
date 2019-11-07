import pandas as pd
import os
from DatabaseOp import df2mysql as dbop
'''
处理李佳的user_book.txt文件，存入mysql数据库
'''
def delNoReadBook(user_book):
    #deluserid 记录没读过书的用户
    deluserid = []
    for i in range(user_book.shape[0]):
       if len(user_book['booksid'][i]) == 0:
           deluserid.append(i)
    user_book = user_book.drop(deluserid,axis=0)
    user_book = user_book.reset_index(drop=True)
    return user_book

def regularTable(user_book):
    '''
    再将数据个数转化为 一个userid 对应多了 booksid,即将booksid拆开
    user_book useid varchar(30) bookid varchar(30)
    :param user_book:
    :return: user_book
    '''
    user_bookdf = pd.DataFrame(columns=['userid','bookid'])
    for i in range(user_book.shape[0]):
        item_userid =[user_book.iloc[i,0]]*len(user_book.iloc[i,1])
        itemuser = pd.DataFrame(zip(item_userid,user_book.iloc[i,1]),columns=['userid','bookid'])
        user_bookdf = user_bookdf.append(itemuser)
    user_book = user_bookdf
    user_book = user_book.reset_index(drop=True)
    # pd.set_option('display.max_columns', None)
    # # 显示所有行
    # pd.set_option('display.max_rows', None)
    # print(user_book)
    return user_book

def read_txt():
    path = r'C:\Users\chend\Desktop\dataset\doubanbook-lijia\user-book.txt'
    with open(path,encoding='utf-8-sig') as f:
        txt = f.readlines()
    usesid = []
    booksid =[]
    for line in txt:
        line = eval(line)
        usesid.append(line['用户id'])
        booksid.append(line['图书id'])
    user_book = pd.DataFrame(zip(usesid,booksid),columns=['userid','booksid'])

    # 删除没读过书的用户
    user_book=delNoReadBook(user_book)

    #user_booK格式调整
    user_book =regularTable(user_book)

    return user_book

if __name__ == "__main__":

    user_book = read_txt()
    print(user_book.shape)
    dbop.store(user_book,'user_book')