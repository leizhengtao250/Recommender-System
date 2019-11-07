# 存入数据库
# 表一 : user
# user:userid varhcar(30),username varchar(30),userlink varchar(100)
# 表二 : book
#bookid varchar(30),bookname varchar(30),bookauthor varchar(30),bookcover varchar(100)
# 表三 : book_labels
#bookid varchar(30),booklabels varchar(100) 其中booklabes为列表结构 存到mysql中个格式为varchar
# 表四 : user_book
#userid varchar(30),booksid varchar(30) 其中booksid 为列表结构
# 表五 : user_book_score
#userid varchar(30),bookid varchar(30),score int
from sqlalchemy import create_engine


def store(df,tablename):
    '''
    直接将dataframe 存入mysql
    :param df:
    :param tablename:
    :return:
    '''
    recsysinfo = {
        'host': '127.0.0.1',
        'port': '3306',
        'db': 'recsys',
        'user': 'root',
        'password': 'cd7089028'
    }
    engine = create_engine(str(r"mysql+pymysql://%s:" + '%s' + r"@%s:%s" + "/%s?charset=UTF8MB4") % (recsysinfo['user'],
                                                                                                     recsysinfo['password'],
                                                                                                     recsysinfo['host'],
                                                                                                     recsysinfo['port'],
                                                                                                     recsysinfo['db']))
    try:
        df.to_sql(tablename, con=engine, if_exists='append', index=False)
    except Exception as e:
        print(e)

def storeRecommendBookSys(df,tablename):
    dbinfo = {
        'host': '127.0.0.1',
        'port': '3306',
        'db': 'recommendbooksys',
        'user': 'root',
        'password': 'cd7089028'
    }
    engine = create_engine(str(r"mysql+pymysql://%s:" + '%s' + r"@%s:%s" + "/%s?charset=UTF8MB4") % (dbinfo['user'],
                                                                                                     dbinfo['password'],
                                                                                                     dbinfo['host'],
                                                                                                     dbinfo['port'],
                                                                                                     dbinfo['db']))
    try:
        df.to_sql(tablename, con=engine, if_exists='replace', index=False)
    except Exception as e:
        print(e)