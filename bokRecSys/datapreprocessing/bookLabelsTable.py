import pandas as pd
import os
from DatabaseOp import df2mysql as dbop
import re
from tool import Traditional2simple as trans

def delNolabelBook(book_labels):
    delindex = []
    for i in range(book_labels.shape[0]):
        if len(book_labels.iloc[i,1]) == 0:
            delindex.append(i)
    print(delindex)
    book_labels = book_labels.drop(delindex)
    book_labels = book_labels.reset_index(drop=True)

    return book_labels

def regularTable(book_labels):
    book_labelsdf = pd.DataFrame(columns=['bookid','booklabel'])
    for i in range(book_labels.shape[0]):
        item_bookid = []
        item_bookid = [book_labels.iloc[i,0]]*len(book_labels.iloc[i,1])
        itemdf = pd.DataFrame(zip(item_bookid,book_labels.iloc[i,1]),columns=['bookid','booklabel'])
        book_labelsdf = book_labelsdf.append(itemdf)
    book_labels = book_labelsdf
    book_labels = book_labels.reset_index(drop=True)
    return book_labels

def cleanData(book_labels):
    '''
    一轮清洗标签，只保留小于或等于四个汉字的标签,
    繁体字化简体字
    :param book_labels:
    :return: book_labels
    '''
    pattern = re.compile("^[\u4e00-\u9fa5]{2,4}$")
    dfindex = []
    for i in range(book_labels.shape[0]):
        if re.match(pattern,book_labels.iloc[i,1]):
            book_labels.iloc[i,1]=trans.Traditional2Simplified(book_labels.iloc[i,1])
            dfindex.append(i)
    book_labels = book_labels.iloc[dfindex]
    book_labels = book_labels.reset_index(drop=True)
    return book_labels

def read_txt():
    path = r'C:\Users\chend\Desktop\dataset\doubanbook-lijia\book-labels.txt'
    with open(path,encoding='utf-8-sig') as f:
        txt = f.readlines()
    booksid = []
    booklabels = []

    # booklabelsdf = pd.DataFrame(columns=['bookid','booklabels'])
    for line in txt:
        line = eval(line)
        booksid.append(line['图书id'])
        booklabels.append(line['图书标签'])
    book_labels = pd.DataFrame(zip(booksid,booklabels),columns=['bookid','booklabels'])
    # 去除没有标签的图书
    book_labels = delNolabelBook(book_labels)

    # 对book_labels个格式进行调整
    book_labels = regularTable(book_labels)

    # 显示所有列与行
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)

    # 清洗标签，只保留四个一下汉字,且化繁体为简体
    book_labels = cleanData(book_labels)
    # print(book_labels)
    return book_labels

if __name__ == '__main__':
    book_labels=read_txt()
    print(book_labels.shape)
    dbop.store(book_labels,'book_labels')