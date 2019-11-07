'''
0. 从mysql中取出各个表中的数据,对标签进行最终清洗，只保留词袋中出现的标签，
1. 对数据做3NF处理，建立主键,外键，
2. 建立标签向量表
3. 建立图书向量表，基于此对图书做kMeans聚类，将图书分为n类（超参数n估计再5-15的范围内）
4. 基于3中的聚类，再booK表中加入一个新列，[bookcategories int] 表明此时属于那一类
5. 用户聚类规则：
    5.1.先遍历用户读过各类书的本书，建立用户-图书类型向量
    5.2.根据用户对已读图书的评分，建立用户对一类书评分的均分向量
    5.3.联合5.1和5.2组成用户向量，对用户聚类
6. 基于5中的聚类，再user表中加入一个新列，[usercategories int]表明此用户属于哪一类

表一 : user
user:userid varhcar(30),username varchar(30),userlink varchar(100)
表二 : book
bookid varchar(30),bookname varchar(30),bookauthor varchar(30),bookcover varchar(100)
表三 : book_labels
bookid varchar(30),booklabels varchar(100) 其中booklabes为列表结构 存到mysql中个格式为varchar
表四 : user_book
userid varchar(30),booksid varchar(30) 其中booksid 为列表结构
表五 : user_book_score
userid varchar(30),bookid varchar(30),score int
'''