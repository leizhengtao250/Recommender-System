import pandas as pd
import os
import re
from DatabaseOp import df2mysql as dbop

def delNoitemInfo(table):
    '''
    删除没有标签的书或删除没阅读过书的用户或者删除阅读过书却没有评分的用户
    我们需要记录删除的书的bookid,或 删除的用户的userid
    :param table:
    :return:
    '''
    delindex = []
    delid = []
    for i in range(table.shape[0]):
        if len(table.iloc[i,1]) == 0:
            delindex.append(i)
            delid.append(table.iloc[i,0])
    print(delindex)
    print(delid)
    table = table.drop(delindex)
    table = table.reset_index(drop=True)

    # 需要更新user表或者book表
    # UpdateTable(table,)
    return table

def regularTable(table):
    tabledf = pd.DataFrame(columns=['userid','bookid','score'])
    for i in range(table.shape[0]):
        item_userid =[table.iloc[i,0]]*len(table.iloc[i,1])
        itemdf = pd.DataFrame(zip(item_userid,table.iloc[i,1],table.iloc[i,2]),columns=['userid','bookid','score'])
        tabledf=tabledf.append(itemdf)
    table = tabledf
    table = table.reset_index(drop=True)
    return table

def read_txt():
    path = r'C:\Users\chend\Desktop\dataset\doubanbook-lijia1\user-score.txt'
    with open(path,encoding='utf-8-sig') as f:
        txt = f.readlines()
    usesid = []
    booksid = []
    booksscore = []
    for line in txt:
        line = eval(line)
        usesid.append(line['用户id'])
        booksid.append(line['图书id'])
        booksscore.append(line['图书评分'])
    user_book_score=pd.DataFrame(zip(usesid,booksid,booksscore),columns=['userid','booksid','booksscore'])

    # 显示所有列与行
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)

    # 去除没有读过书的的用户，没给评分
    user_book_score = delNoitemInfo(user_book_score)

    # 表格结构调整
    user_book_score = regularTable(user_book_score)

    return user_book_score

if __name__ == "__main__":
    txt_path = r'C:\Users\chend\Desktop\dataset\doubanbook-lijia1\user-score.txt'
    user_book_score = read_txt()
    print(user_book_score.shape)
    dbop.store(user_book_score,'user_book_score')