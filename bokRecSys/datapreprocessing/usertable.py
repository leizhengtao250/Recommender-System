import pandas as pd
from DatabaseOp import df2mysql as dbop
'''
处理李佳的user.txt文件，存入mysql
'''
def read_txt():
    path=r'C:\Users\chend\Desktop\dataset\doubanbook-lijia\user.txt'
    usersid = []
    usersname = []
    userslink = []
    with open(path,encoding='UTF-8-sig') as f:
        txt = f.readlines()
    # print(txt)
    for line in txt:
        line = eval(line)
        print(line)
        usersid.append(line['userid'])
        usersname.append(line['username'])
        userslink.append(line['userlink'])
    user = pd.DataFrame(zip(usersid,usersname,userslink),columns=['userid','username','userlink'])
    return user


if __name__ == "__main__":
    user = read_txt()
    print(user.shape)
    dbop.store(user,'user')