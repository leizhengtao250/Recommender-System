# 王辉数据的预处理
'''
excel2txt.py 对王辉的excel 做处理，并存入mysql数据库中
表一 : user
user:userid,username,userlink
表二 : book
bookid,bookname,bookauthor,bookcover
表三 : book_labels
bookid,booklabels 其中booklabes为列表结构 存到mysql中个格式为varchar
表四 : user_book
userid,booksid 其中booksid 为列表结构
表五 : user_book_score
userid,bookid,score
'''
import numpy as np
import pandas as pd
import os
# from zhtools.langconv import *
from DatabaseOp import df2mysql as dbop
import re
from zhtools.langconv import *
from tool import Traditional2simple as trans


def delnanDataFrame(table):
    '''
    删除一张表中的“空行”(图书没标签，用户没看过书)
    :param table:
    :return:
    '''
    delid=[]
    delindex=[]
    for i in range(table.shape[0]):
        if table.iloc[i,1:].isna().all():
            delid.append(str(table.iloc[i,0]))
            delindex.append(i)
    table = table.drop(delindex)
    table = table.reset_index(drop=True)
    print('删除的用户或图书id有：')
    print(delid)
    return table


def handleUser(user):
    '''
    王辉user表
    :param user:
    :return:
    '''
    user = user.dropna(how='any', axis=0)  # DataFrame
    user.columns = ['userid', 'username', 'userlink']
    return user


def handleBook(book):
    # 对于book我们需要给他加上bookcover字段
    book = book.dropna(how='any', axis=0)
    path = r'C:\Users\chend\Desktop\dataset\doubanbook-wanghui\covers'
    os.chdir(path)
    # 得到所有的图片名称
    filenames = os.listdir(path)  # list类型
    indexlist = []
    coverpathlist = []
    for i in range(book.shape[0]):
        # print(book.iloc[i,0])
        name = str(book.iloc[i, 0]) + '.jpg'
        if name in filenames:
            indexlist.append(i)
            coverpath = path + '\\' + name
            coverpathlist.append(coverpath)
    # 删除没有封面的图书
    book = book.iloc[indexlist]
    book = book.reset_index(drop=True)
    # 加新的一列 bookcover
    book['bookcovver'] = coverpathlist
    pd.set_option('display.max_columns', None)
    # 显示所有行
    pd.set_option('display.max_rows', None)
    book.columns = ['bookid', 'bookname', 'bookauthor', 'bookcover']
    return book


def handleUser_book(user_book):
    user_bookdf=pd.DataFrame(columns=['userid','bookid'])

    user_book.columns = ['userid', 'bookid1', 'bookid2', 'bookid3', 'bookid4',
                         'bookid5', 'bookid6,', 'bookid7', 'bookid8',
                         'bookid9', 'bookid10', 'bookid11', 'bookid12',
                         'bookid13', 'bookid14', 'bookid15']
    # 先对user_book做依次清洗,删除没有阅读图书的用户，返回deleteuserid=[]
    user_book = delnanDataFrame(user_book)
    bookid = []
    userid = []
    #删除标签为nan的的图书标签
    user_book = user_book.fillna('No')
    for i in range(user_book.shape[0]):
        item_booksid = []
        item_userid = []
        for j in range(1, 15):
            if user_book.iloc[i, j] != 'No':
                item_booksid.append(str(user_book.iloc[i, j]))
        item_userid = [str(user_book.iloc[i,0])]*len(item_booksid)
        itemdf = pd.DataFrame(zip(item_userid,item_booksid),columns=['userid','bookid'])
        user_bookdf = user_bookdf.append(itemdf)

    user_book = user_bookdf
    user_book = user_book.reset_index(drop=True)

    return user_book




def handleBook_labels(book_labels):
    # 删除没有标签的图书
    book_labels = delnanDataFrame(book_labels)
    book_labels=book_labels.fillna('No')
    #删除图书的标签为nan的标签
    book_labelsdf = pd.DataFrame(columns=['bookid','booklabel'])
    for i in range(book_labels.shape[0]):
        item_booklabels = []
        item_bookid = []
        for j in range(1, 9):
            if book_labels.iloc[i, j] != 'No':
                item_booklabels.append(str(book_labels.iloc[i, j]))
        item_bookid=[str(book_labels.iloc[i,0])]*len(item_booklabels)
        itemdf = pd.DataFrame(zip(item_bookid,item_booklabels),columns=['bookid','booklabel'])
        book_labelsdf = book_labelsdf.append(itemdf,ignore_index=True)
    book_labels=book_labelsdf
    # 对book_labels 做清洗,只保留四个汉字一下的标签
    dfindex=[]
    for i in range(book_labels.shape[0]):
        pattern=re.compile("^[\u4e00-\u9fa5]{2,4}$")
        if re.match(pattern,str(book_labels.iloc[i,1])):
            #换繁体字为简体字
            book_labels.iloc[i,1] = trans.Traditional2Simplified(book_labels.iloc[i,1])
            dfindex.append(i)
    book_labels = book_labels.iloc[dfindex]
    book_labels = book_labels.reset_index(drop=True)
    return book_labels



def handleUser_book_score(user_book_score,user_book):
    #user_book_score表结构
    #userid varchar(30) bookid vachar(30) score int
    user_book_scoredf = pd.DataFrame(columns=['userid','bookid','score'])
    user_book_score = user_book_score.fillna('No')
    for i in range(user_book_score.shape[0]):
        item_user=[]
        item_score = []
        item_booksid = []
        for j in range(1,16):
            if user_book_score.iloc[i,j] != 'No':
                item_score.append(user_book_score.iloc[i,j])
                item_booksid.append(user_book.iloc[i,j])
        item_user = [user_book_score.iloc[i,0]]*len(item_score)
        itemdf = pd.DataFrame(zip(item_user,item_booksid,item_score),columns=['userid','bookid','score'])
        user_book_scoredf = user_book_scoredf.append(itemdf)

    user_book_score = user_book_scoredf
    user_book_score = user_book_score.reset_index(drop=True)
    return user_book_score


def excel2txt():
    # 对王辉的excel的表格转成txt
    path = r'C:\Users\chend\Desktop\dataset\doubanbook-wanghui\doubanbook-wanghui.xlsx'
    txt = pd.read_excel(path, header=0, sheet_name=None, dtype='str')
    txt = list(txt.values())

    user = handleUser(txt[0])
    book = handleBook(txt[1])
    user_book = handleUser_book(txt[2])
    book_labels = handleBook_labels(txt[3])
    user_book_score = handleUser_book_score(txt[4],txt[2])
    print('-----------------------------------------------------------------------------')
    print(user.shape,book.shape,user_book.shape,book_labels.shape,user_book_score.shape)
    print('-----------------------------------------------------------------------------')
    dbop.store(user, 'user')
    dbop.store(book,'book')
    dbop.store(book_labels,'book_labels')
    dbop.store(user_book,'user_book')
    dbop.store(user_book_score,'user_book_score')

if __name__ == '__main__':
    excel2txt()




























