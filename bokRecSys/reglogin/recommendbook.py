#当用户注册成功时（新用户）,和老用户登录时,推荐图书id
'''
0. 得到用户的图书聚类和用户聚类，推荐同类同类用户中的图书
1. 每隔一段时间全局的更新 图书聚类 和 用户聚类，更加精准推荐
'''
from DatabaseOp import connDb

def initDb():
    #连数据库
    con = connDb.connRecbooksys()
    cur =con.cursor()
    cur.execute('use recommendbooksys')
    return cur

def recommnedbook(userid):
    '''
    # 用户登录，得到useid,返回推荐的图书id,booksid = []
    :param userid:
    :return: booksid
    '''
    '''
    推荐图书的流程：
    1. 基于用户的协同过滤
        1.1 根据useid 找到此user 属于哪一类userclass
        1.2 找出该类的所有用户
        1.3 找出该类用户所读的所用的书,推荐给登录用书没读过的书 （基于用户推荐）
    2. 基于内容的协同过滤
        2.1 根据userid 找到此用户所读过的图书 属于的bookclass
        2.2 推荐相同bookclass中该user没读过的图书
    '''
    cur = initDb()
    sql = "select userclass from user where userid = '%s' "
    cur.execute(sql % userid)
    #得到登录用户的userclass
    userclass = cur.fetchall()
    sql = "select userid from user where userclass = '%s' "
    cur.execute(sql % userclass[0][0])
    # 得到与用户同类的所有用户userid
    usersid = cur.fetchall()
    # 找出该类用户所读的所用的书,推荐给登录用书没读过的书 （基于用户推荐)
    books = []
    for userid in usersid:
        sql = "select bookid,bookname,bookcover,bookauthor,bookclass from user_booktype where userid = '%s'"
        cur.execute(sql % userid[0])
        books.append(cur.fetchall())
    print(books,len(books))

    resbooks =[]

    print('--------------------------------------------')
    for userbook in books:
        for book in userbook:
            resbooks.append(book[0])
    print('--------------------------------------------')
    print(resbooks)
    print(len(resbooks))


    recbooks =[]
    recbooksid = []

    for book in books:
        try:
            recbooks.append(book[0])
        except Exception as e:
            continue
    for book in recbooks[1:]:
        recbooksid.append(book[0])
    print(recbooks)
    print(recbooksid)
    #返回推荐的图书和图书id
    return recbooks,recbooksid

if __name__ == "__main__":
    userid='2162455'
    recommnedbook(userid)





