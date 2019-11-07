'''
连接本地recsys数据库
'''
import pymysql
def connection():
    dbinfo = {
        'host': '127.0.0.1',
        'port': 3306,
        'db': 'recsys',
        'user': 'root',
        'password': 'cd7089028'
    }
    try:
       conn = pymysql.connect(**dbinfo)
    except Exception as err:
        print(err)
    print('连接成功')
    return conn

def connRecbooksys():
    # 连接数据库
    dbinfo = {
        'host': '127.0.0.1',
        'port': 3306,
        'db': 'recommendbookSys',
        'user': 'root',
        'password': '123456'
     }
    try:
        conn = pymysql.connect(**dbinfo)
    except Exception as e:
        print(e)
    print('连接成功')
    return conn


