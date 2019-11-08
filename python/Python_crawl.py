from xlwt import *
import requests
from bs4 import BeautifulSoup
import re
header={'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36'}
filenew=Workbook(encoding='utf-8')
table=filenew.add_sheet('data')
def getUser(urls):
    userlist = []
    for url in urls:
        wb_data = requests.get(url,headers=header)
        soup = BeautifulSoup(wb_data.text, 'html.parser')
        usernames = soup.select('span.comment-info > a')
        for username in usernames:
            data1 = {
                '用户id':username.get('href').split('/')[4],
                '用户名': list(username.stripped_strings)[0],
                '用户主页链接': username.get('href')
            }
            with open('F:/douban/user.txt', 'a+', encoding='utf-8') as file:
                file.write(str(data1) + "\n")
            userlist.append(data1['用户主页链接'])
    return userlist

def getBook(userlist):
    for user in userlist:
        userid=user.split('/')[4]
        print(userid)
        userbookid=[]
        userbookstar=[]
        user_data=requests.get(user)
        usersoup=BeautifulSoup(user_data.text,'html.parser')
        books=usersoup.select('div#book span.pl>a')
        if not books:
            continue
        bookurl1=books[-1].get('href')
        u0 = bookurl1.split('/')[0]
        u1 = bookurl1.split('/')[4]
        u2 = bookurl1.split('/')[2]
        u3 = bookurl1.split('/')[3]
        bookurl2 = u0 + '/'  + '/' + u2 + '/' + u3 +'/'+ u1 + '/' + 'reviews'
        bookurls=[bookurl2+"?start="+str(n) for n in range(0,20,5)]
        for bookurl in bookurls:
            authors=[]
            book_data=requests.get(bookurl)
            booksoup=BeautifulSoup(book_data.text,'html.parser')
            titles=booksoup.select('div.ilst>a')
            imgs=booksoup.select('img[width="100px"]')
            newurls = booksoup.select('div.ilst>a')
            labellistall=[]
            for title in titles:
                userbookid.append(title.get('href').split('/')[4])

            for newurl in newurls:
                realbookurl = newurl.get('href')
                wb_data = requests.get(realbookurl)
                soup = BeautifulSoup(wb_data.text, 'html.parser')
                author1=soup.select('div#info>span>a')
                author2 = soup.select('div#info>a')
                if author1:
                    author = author1[0].get_text().replace(' ','')
                    author=author.replace('\n','')

                elif author2:
                    author = author2[0].get_text().replace(' ','')
                    author = author.replace('\n', '')
                else:
                    author="不详"
                authors.append(author)

                labellist = []
                booklabel = soup.select('a.tag')

                for i in range(len(booklabel)):
                    label=list(booklabel[i].stripped_strings)
                    labellist+=label
                labellistall.append(labellist)
            stars=booksoup.findAll(name='span',attrs={"class":re.compile(r'^allstar')})
            starlist=[]
            for star in stars:
                starlist.append(star.get('class')[0][7])
                userbookstar+=star.get('class')[0][7]


            booklistall=[]
            for title, img,labellist,star,author in zip(titles, imgs,labellistall,starlist,authors):
                data = {
                    '1': title.get('title'),
                    '2': img.get('src'),
                    '3':author,
                    '4':title.get('href').split('/')[4],
                    '5': labellist,
                    '6':star

                }
                
                with open('F:/douban/bookall.txt', 'a+', encoding='utf-8') as fileall:
                    fileall.write(str(data) + "\n")
                filename = data['图书id'] + "、" + data['图书名称'][0] + ".jpg"
                pic = requests.get(data['图片'])
                with open('F:/douban/pictures/' + filename, 'wb') as photo:
                    photo.write(pic.content)
                booklist = []
                for i in len(data):
                    booklist.append(data[str(i + 1)])
                booklistall.append(booklist)
            for i, p in enumerate(booklistall):
                for j, q in enumerate(p):
                    table.write(i, j, q)
            filenew.save(data.xlsx)

            for title,author in zip(titles,authors):
                data1={
                    '图书id':title.get('href').split('/')[4],
                    '图书名称':title.get('title'),
                    '图书作者':author
                }
                print(data1)
                with open('F:/douban/book.txt', 'a+', encoding='utf-8') as file1:
                    file1.write(str(data1) + "\n")

            for labellist,title in zip(labellistall,titles):
                data2={
                    '图书id':title.get('href').split('/')[4],
                    '图书标签':labellist

                }
                print(data2)
                with open('F:/douban/book_label.txt', 'a+', encoding='utf-8') as file2:
                    file2.write(str(data2) + "\n")
        data3 = {
            '用户id': userid,
            '图书id': userbookid,

                }
        print(data3)
        with open('F:/douban/user_book.txt', 'a+', encoding='utf-8') as file3:
            file3.write(str(data3) + "\n")
        data4 = {
            '用户id': userid,
            '图书评分': userbookstar
            }
        print(data4)
        with open('F:/douban/user_star.txt', 'a+', encoding='utf-8') as file4:
            file4.write(str(data4) + "\n")

        data5 = {
            '用户id': userid,
            '图书id': userbookid,
            '图书评分': userbookstar
        }
        print(data5)
        with open('F:/douban/user_book_star.txt', 'a+', encoding='utf-8') as file5:
            file5.write(str(data5) + "\n")

if __name__=="__main__":
    urls=["https://book.douban.com/subject/1090043/comments/hot?p="+str(n) for n in range(1,10)]
    userlist=getUser(urls)
    getBook(userlist)

