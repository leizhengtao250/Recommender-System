'''
addcoverlink.py 是对李佳的文件做处理，功能是对每本书加一个封面绝对路径
'''
import os
import pandas as pd
from DatabaseOp import df2mysql as dbop

def addcover(book):
    book = book.dropna(how='any', axis=0)
    path = r'C:\Users\chend\Desktop\dataset\doubanbook-lijia\pictures'
    os.chdir(path)
    # 得到所有的图片名称
    filenames = os.listdir(path)  # list类型
    indexlist = []
    coverpathlist = []
    for i in range(book.shape[0]):
        name = str(book.iloc[i, 0]) + '.jpg'
        if name in filenames:
            indexlist.append(i)
            coverpath = path + '\\' + name
            coverpathlist.append(coverpath)
    # 删除没有封面的图书
    book = book.iloc[indexlist]
    book = book.reset_index(drop=True)
    book['bookcovver'] = coverpathlist
    # pd.set_option('display.max_columns', None)
    # # 显示所有行
    # pd.set_option('display.max_rows', None)
    book.columns = ['bookid', 'bookname', 'bookauthor', 'bookcover']
    # booktable.append(book)
    return book

if __name__ == '__main__':

    path=r'C:\Users\chend\Desktop\dataset\doubanbook-lijia\book.txt'
    # 从txt文件中读入DataFrame
    with open(path,encoding='utf-8') as f:
        txt=f.readlines()
    # print(txt)
    booksid=[]
    booksname=[]
    booksauthor=[]
    for line in txt:
        line=eval(line)
        booksid.append(line['图书id'])
        booksname.append(line['图书名称'])
        booksauthor.append(line['图书作者'])
    book=pd.DataFrame(zip(booksid,booksname,booksauthor),columns=['bookid','bookname','bookauthor'])
    # print(book)
    book = addcover(book)
    print('--------------------------------------------------------------------------------------')
    # pd.set_option('display.max_columns', None)
    # 存入数据库book表
    print(book.shape)
    dbop.store(book,'book')
