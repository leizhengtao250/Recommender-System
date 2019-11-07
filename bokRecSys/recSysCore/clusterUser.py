'''
对用户做聚类,并在user 表中新增一列 userclass
'''
import pandas as pd
from DatabaseOp import connDb as con
from DatabaseOp import  df2mysql as dbop
import numpy as np
def initDb():
    #连接recommendbooksys 数据库,返回游标
    conn = con.connRecbooksys()
    cur = conn.cursor()
    cur.execute('use recommendbooksys')
    return cur

def user_booktypeTable():
    # pd.set_option('display.max_columns', None)
    # pd.set_option('display.max_rows', None)

    cur = initDb()
    cur.execute('select * from user_book_scoretype')
    data = cur.fetchall()
    user_book_scoretype = pd.DataFrame(data,columns=['userid','bookid','bookclass','score'])
    user_book_scoretype =user_book_scoretype.dropna(axis=0,how='any')


    simUsertable = pd.DataFrame(columns=['userid','bc0','bc1','bc2','bc3','bc4','bc5','bc6','bc7',
                          'bcavgsc0','bcavgsc1','bcavgsc2','bcavgsc3','bcavgsc4','bcavgsc5','bcavgsc6','bcavgs17c7'])

    usersid = user_book_scoretype.groupby('userid').groups.keys()

    for userid in list(usersid):
        print(userid)
        userinfo = [0]*17
        count= [0]*8
        userinfo[0]=userid
        sql ="select bookclass,score from user_book_scoretype where userid = '%s' "
        cur.execute(sql % userid)
        bookclassdata = cur.fetchall()
        for i,j in bookclassdata:
            count[i] = count[i]+1
            userinfo[int(i)+1] = userinfo[int(i)+1]+1
            userinfo[int(i)+9] = userinfo[int(i)+9]+j
        for i,k in enumerate(count):
            if k != 0:
                userinfo[i+9] = round(userinfo[i+9]/k,2)
        userinfo = pd.DataFrame(userinfo).T
        userinfo.columns = simUsertable.columns
        simUsertable = pd.concat([simUsertable,userinfo])

    print(simUsertable)
    return simUsertable


if __name__ =='__main__' :
    sim_user = user_booktypeTable()
    dbop.storeRecommendBookSys(sim_user,'sim_user')








