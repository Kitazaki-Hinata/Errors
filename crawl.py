import ssl
import sqlite3
import re
from urllib.request import urlopen
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


ctx = ssl.create_default_context()
ctx.check_hostname = False  # 设置hostname为false
ctx.verify_mode = ssl.CERT_NONE  #  关闭验证，让爬虫抓取网页

# create an sql sheet to store data
db = sqlite3.connect("weburl.sqlite")
cursor = db.cursor()

cursor.executescript('''
    CREATE TABLE IF NOT EXISTS Connect(
        input_id INTEGER,
        output_id INTEGER);
    
    CREATE TABLE IF NOT EXISTS Webs(
        id INTEGER UNIQUE,
        web TEXT);
    
    CREATE TABLE IF NOT EXISTS Urls(
        id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT,
        url TEXT,
        html TEXT,
        error INTEGER,
        old_rank INTEGER,
        new_rank INTEGER)''')
db.commit()

# check if there's any url haven't been crawled
cursor.execute("SELECT url FROM Urls WHERE html is NULL and error IS NULL ORDER BY RANDOM() LIMIT 1")
url_existed = cursor.fetchone()
if url_existed is not None:
    print("Starting from existed url in the database...")
else:   # if there's not url that can be crawled, enter a new url
    url_new = input("Enter a new main url:")
    print(url_new)
    if re.fullmatch(r"\s*", url_new): url_new = r"http://python-data.dr-chuck.net"
    if url_new.endswith("/"): url_new = url_new[:-1]
    parsed_url = urlparse(url_new)
    if parsed_url.path.endswith(".html") or parsed_url.path.endswith(".htm"):
        slash_position = url_new.rfind("/")   # 返回/的位置
        url_new = url_new[:slash_position]
    db.commit()

# add new url into Webs table
    if len(url_new) > 1:
        cursor.execute("INSERT OR IGNORE INTO Webs(web) VALUES (?)", (url_new, ))
        cursor.execute("INSERT OR IGNORE INTO Urls(url, html, new_rank) VALUES(?, NULL, 1.0)", (url_new,))
        db.commit()

# crawl all urls in Webs table
cursor.execute("SELECT web FROM Webs")
webs_collection = []
for i in cursor:
    webs_collection.append(str(i[0]))
print(webs_collection)

# build a loop, where enter times of loop
num_of_loop = 0
while True:
    if num_of_loop < 1:
        try:
            time_loop = int(input("Enter total times of the loop"))
            num_of_loop = time_loop
        except:
            print("Please enter a valid integer!")
            continue



    num_of_loop = num_of_loop - 1

    cursor.execute("SELECT url, id FROM Urls WHERE html IS NULL AND error IS NULL ORDER BY RANDOM() LIMIT 1")
    try:
        url_visit = cursor.fetchone()
        from_id = url_visit[0]
        url = url_visit[1]
    except:
        print("No existed url can be crawled!")
        num_of_loop = 0       # stop loop
        break

    # delete duplicated url
    cursor.execute("DELETE FROM Connect WHERE input_id = ?", (from_id, ))
    db.commit()
    try:
        url_open = urlopen(url, context = ctx)
        url_context = url_open.read()
        if url_open.getcode() != 200:  # 如果server出错，返回错误代码0，无法访问
            print(f"Error: server return error code: {url_open.getcode()}")
            cursor.execute("UPDATE Urls SET error = 0 WHERE url = ?", (url,))
            db.commit()
            continue
        if "text/html" != url_open.info().get_content_type():   # if page is not a html file, return no
            print(f"Error: {url} is not a html file!")
            cursor.execute("DELETE FROM Urls WHERE url = ?", (url, ))
            cursor.execute("UPDATE Urls SET error = 1 WHERE url  = ?", (url, ))
            db.commit()

    except:
        print("Error! Try again!")
        cursor.execute("UPDATE Urls SET error = 2 WHERE url = ?", (url,))
        continue

    # change into html and soup
    soup = BeautifulSoup(url_context, "lxml")
    tags = soup("a")  # filter all sentence with tag "a"
    count = 0
    for tag in tags:
        href = tag.get("href", None)  # get the href context of the page
        if href is None: continue
        up = urlparse(href)

        # 判断#位置，即片段标识符，然后去除标识符后面的内容
        judge = href.find("#")
        if "#" in href and judge > 0:
            href = href[:judge]
        if len(up.scheme) < 1:  # 如果没有http的前缀协议，则加上协议
            href = urljoin(url, href) # 将url和href（缺少前缀协议）拼接成完整的url
        if href.endswith("jpg") or href.endswith("png") or href.endswith("gif") or href.endswith("jpeg"):
            continue
        if href.endswith("/"): href = href[:-1]
        if len(href) < 1: continue

        # 防止访问整个互联网
        found = False
        for i in webs_collection:
            if href.startswith(i):   # 如果在列表中找到一个匹配的，逃过for循环
                found = True
                break
        if not found: continue  # 如果没有找到，则重复循环，在列表中找

        cursor.execute("INSERT OR IGNORE INTO Urls(url, html, new_rank) VALUES (?, NULL,1.0)", (href, ))
        count = count + 1
        db.commit()

        cursor.execute("SELECT id FROM Urls WHERE url = ? LIMIT 1", (href,))
        try:
            id_2 = cursor.fetchone()
            to_id = id_2[0]
        except:
            print("Failed to found an id")
            continue
        cursor.execute("INSERT OR IGNORE INTO Connect(input_id, output_id) VALUES (?,?)", (from_id, to_id))
        db.commit()











