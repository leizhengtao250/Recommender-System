from sklearn.cluster import KMeans
from zhtools.langconv import *
import synonyms
import numpy as np
import pandas as pd
import re


# 繁体字转为简体字的API
def Traditional2Simplified(sentence):

    '''
    将sentence中的繁体字转为简体字
    :param sentence: 待转换的句子
    :return: 将句子中繁体字转换为简体字之后的句子
    '''
    sentence = Converter('zh-hans').convert(sentence)
    return sentence

def readData():

    '''
    从txt文件中，读入book-labels，存入DataFrame中
    '''

    path=r'C:\Users\chend\Desktop\dataset\doubanbook-lijia\book-labels.txt'
    with open(path,encoding='utf-8') as f:
        txt=f.readlines()
    bookid=[]
    booklabels=[]
    for line in txt:
        line=eval(line)
        bookid.append(line['图书id'])
        booklabels.append(line['图书标签'])
    book_labels_table=pd.DataFrame(data=zip(bookid,booklabels),columns=['bookid','booklabels'])
    print(book_labels_table)
    return book_labels_table


def delteNanRow(book_labels_table):
    # 去掉没有标签的图书,返回图书id

    # 带删除的booK,该图书没有标签，显然是脏数据
    deletebookid = []

    # 0. 先去掉没有图书标签的行
    for index in range(book_labels_table.shape[0]):
        if len(book_labels_table['booklabels'][index]) == 0:
            deletebookid.append(index)
    # 删除图书的空标签
    book_labels_table = book_labels_table.drop(deletebookid)
    # 重新依次从0设置索引
    book_labels_table = book_labels_table.reset_index(drop=True)
    print(book_labels_table)
    print(book_labels_table.shape)
    return book_labels_table



def cleanData(book_labels_table):
    '''
       #对图书标签进行预处理,清洗规则如下:
       0. 只保留中文文字描述的标签
       1. 去掉四个汉字以上的标签
       2. 去掉没有标签的图书,返回图书id,存入deletebookid=[]
       3. 去掉一本书对应的重复标签
       :param book_labels_table:
       :return: book_labels_table
       '''
    #先去除没有标签的图书
    book_labels_table=delteNanRow(book_labels_table)
    # 存放图书-向量的图书id列表和图书标签向量列表

    print('--------------------下面开始去除四个以上的汉字和不在词典中的汉字---------------------------')

    bookidlist=[]
    booklabelsvector=[]

    # 1. 去掉四个字以上的标签(只保留标签长度为2-4个汉字的),且将繁体标签转换为简体标签,去掉重复的标签
    print('去掉四个字以上的标签(只保留标签长度为2-4个汉字的),且将繁体标签转换为简体标签,去掉重复的标签')
    for i in range(book_labels_table.shape[0]):
        #item_book_label存放清洗后的label
        item_book_label =[]
        for j in range(len(book_labels_table.iloc[i,1])):
            pattern=re.compile("^[\u4e00-\u9fa5]{2,4}$")
            if re.match(pattern, book_labels_table.iloc[i,1][j]):
                #化繁体为简体
                # print('化繁体为简体')
                book_labels_table.iloc[i,1][j]=Traditional2Simplified(book_labels_table.iloc[i,1][j])
                item_book_label.append(book_labels_table.iloc[i,1][j])
        #去重复的词
        item_book_label=set(item_book_label)
        book_labels_table.iloc[i,1]=list(item_book_label)

    #经过一轮清洗后，可能会出现没有标签的图书，去除没有标签的图书
    print('经过一轮清洗后，可能会出现没有标签的图书，去除没有标签的图书')
    book_labels_table=delteNanRow(book_labels_table)

    # 第二轮去除我们词袋中没有出现的词
    print('第二轮去除我们词袋中没有出现的词')
    for i in range(book_labels_table.shape[0]):
        # item_book_labelVector存放清洗后标签对应的词向量
        item_book_label=[]
        item_book_labelVector = []
        # ind_list用来更新book_labels,有些一轮清洗的留下来的词可能并不在我们的词袋中，我们也需要删除它
        #注意:这样又会引出一个新的问题——可能会再次出现一本书没有标签,我们需要再做依次删除空行操作
        for j in range(len(book_labels_table.iloc[i, 1])):
            try:
                wordvector = synonyms.v(book_labels_table.iloc[i, 1][j])
            except KeyError as err:
                #若词袋中不存在该词，删除
                continue
            #若词袋中存在该词,先存入该词，接着存入该词向量
            item_book_label.append(book_labels_table.iloc[i, 1][j])
            item_book_labelVector.append(wordvector)
        #更新每本书对应的标签
        book_labels_table.iloc[i,1]=item_book_label

        # 得到该本书的图书id,建立图书id-图书标签向量表
        # print('得到该本书的图书id,建立图书id-图书标签向量表')
        if len(book_labels_table.iloc[i,1]) != 0:
            bookidlist.append(book_labels_table.iloc[i,0])
            booklabelsvector.append(item_book_labelVector)

    book_labels_table=delteNanRow(book_labels_table)
    book_labels_vector_table=pd.DataFrame(zip(bookidlist,booklabelsvector))
    book_labels_vector_table.columns=['bookid','book_labels_vector']

    print(book_labels_table)
    #至此得到图书id-图书标签向量表


    print(book_labels_vector_table)
    print('入库')
    book_labels_table.index=book_labels_table['bookid']
    del book_labels_table['bookid']
    print(book_labels_table)
    book_labels_vector_table.index=book_labels_vector_table['bookid']
    del book_labels_vector_table['bookid']
    print(book_labels_vector_table)
    return book_labels_table,book_labels_vector_table


def store2txt(tables):
    book_labels_table, book_labels_vector_table = tables
    booklabelspath = r'C:\Users\chend\Desktop\cleanDataset\book-lables.txt'
    booklabelsvectorpath = r'C:\Users\chend\Desktop\cleanDataset\book-lablesvector.txt'

    print(type(book_labels_table))
    print(type(book_labels_vector_table))
    book_labels_table.to_csv(booklabelspath, sep='\t', index=True)
    book_labels_vector_table.to_csv(booklabelsvectorpath, sep='\t', index=True)
    # with open(booklablespath, encoding='utf-8') as bl:
    #     bl.writelines(book_labels_table)
    # with open(booklabelsvectorpath, encoding='utf-8') as blv:
    #     blv.writelines(book_labels_vector_table)


if __name__=='__main__':
    book_labels_table=readData()
    tables=cleanData(book_labels_table)
    store2txt(tables)
    # toVect(table)




















# wordBag=[
#     '计算机','IT','救赎','日本','治愈','温暖','清朝','小说','经典','散文','英国',
#     '童话','散文','名著','儿童',
#     '杂文','鲁迅','张爱玲','当代文学',
#     '村上春树','诗歌','诗词','港台','钱锺书',
#     '漫画','推理','青春','东野圭吾','悬疑',
#     '韩寒','郭敬明','校园','魔幻',
#     '历史','心理学','哲学','社会学','传记','艺术','建筑',
#     '国学','人物传记','绘画','戏剧','西方哲学','二战',
#     '战争','军事','佛教','考古','美术',
#     '爱情','成长','生活','旅行','心理','励志','美食',
#     '游记','健康','情感','人际关系','手工','养生',
#     '经济学','管理','经济','商业','金融','投资','营销',
#     '理财','创业','股票','广告','科普','互联网',
#     '编程','科技','通信','算法','神经网络','程序'
#     ]


# book_labels=pd.DataFrame(txt)
# print(book_labels)
# book_labels=pd.DataFrame(txt)
# print(book_labels)


# noexitlabelsvector=['科幻']
# labels=['计算机','IT','救赎','日本','治愈','温暖','清朝','小说','经典','散文','英国',
#         '童话','散文','名著','儿童',
#         '杂文','鲁迅','张爱玲','当代文学',
#         '村上春树','诗歌','诗词','港台','钱锺书',
#         '漫画','推理','青春','东野圭吾','悬疑',
#         '韩寒','郭敬明','校园','魔幻',
#         '历史','心理学','哲学','社会学','传记','艺术','建筑',
#         '国学','人物传记','绘画','戏剧','西方哲学','二战',
#         '战争','军事','佛教','考古','美术',
#         '爱情','成长','生活','旅行','心理','励志','美食',
#         '游记','健康','情感','人际关系','手工','养生',
#         '经济学','管理','经济','商业','金融','投资','营销',
#         '理财','创业','股票','广告','科普','互联网',
#         '编程','科技','通信','算法','神经网络','程序'
#         ]
# labelsVec=[]
# synonyms.display(labels[0])
# for index,value in enumerate(labels):
#     print(index,',',value)
#     print('-------------------------------------------')
#     # synonyms.display(value)
#     vec=synonyms.v(value)
#     labelsVec.append(vec)
#     print('-------------------------------------------')

# print(labelsVec)
# kmeans=KMeans(n_clusters=8).fit(labelsVec)
# print(kmeans.labels_)

# book1=['温暖', '推理', '治愈']
# book2=['人性', '救赎', '成长']
# book3=['朱元璋', '历史', '明朝']
# book4=['互联网','IT','计算机','管理','投资']

# 度量book1 ,book2 , book3 的相似性
# 第一步得到图书向量=labe1+labe2+labelm/m

# 对图书聚类（利用余弦定理计算图书之间的相似度），存入数据库


#对用户进行聚类









