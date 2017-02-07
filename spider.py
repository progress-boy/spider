#encoding:utf-8
import urllib2
import itertools #itertools模块包含创建有效迭代器的函数，可以用各种方式对数据进行循环操作，此模块中的所有函数返回的迭代器都可以与for循环语句以及其他包含迭代器（如生成器和生成器表达式）的函数联合使用。
import urlparse
import robotparser
import datetime
import Queue
import lxml.html
import csv
import time
import os
import sys
import random
import re
import urllib
from db import *

class Throttle:#对爬虫进行限速
    """
    Add a delay between downloads to the same domain
    """
    def __init__(self,delay):
        #amount of delay between downloads for each domain
        self.delay = delay
        # timestamp of when a domain was last accessed
        self.domains = {}
        
    def wait(self,url):
        domain = urlparse.urlparse(url).netloc
        last_accessed = self.domains.get(domain)
        
        if self.delay >0 and last_accessed is not None:
            sleep_secs = self.delay - (datetime.datetime.now() - last_accessed).seconds
            if sleep_secs >0:
                # domain has been accessed recently 
                # so need to sleep
                time.sleep(sleep_secs)
        # update the last accessed time
        self.domains[domain] = datetime.datetime.now()
class ScrapeCallback:#回调类
    def __init__(self):
        self.writer = csv.writer(open('countrties.csv','w'))
        self.fields = ('area','population','iso','country',\
                        'capital','continent','tld','currency_code','currency_name',\
                            'phone','postal_code_format','postal_code_regex',\
                                'languages','neighbours')
        self.writer.writerow(self.fields)
        
    def __call__(self,url,html):
        if re.search('/view/',url):
            tree = lxml.html.fromstring(html)
            row = []
            for field in self.fields:
                row.append(tree.cssselect('table > tr#places_{}_row > td.w2p_fw'.format(field))[0].text_content())
                self.writer.writerow(row)
class Downloader:
    def __init__(self,delay=5,user_agent='wswp',proxies=None,num_retries=1,cache=None):
        self.throttle = Throttle(delay)
        self.user_agent = user_agent
        self.proxies = proxies
        self.num_retries = num_retries
        self.cache = cache
        
    def __call__(self,url):
        result=None
        if self.cache:
            try:
                result = self.cache[url]
            except KeyError:
                # url is not available in cache
                pass
            else:
                if self.num_retries >0 and 500 <= result['code'] < 600:
                    # server error so ignore result from cache
                    # and re-download
                    result = None
                    
        if result is None:
            # result was not loaded from cache 
            # so still need to download
            self.throttle.wait(url)
            proxy = random.choice(self.proxies) 
#             if self.proxies:
#                 pass
#             else:
#                 None
            headers = {'User-agent':self.user_agent}
            result = self.download(url,headers,proxy,self.num_retries)
            if self.cache:
                # save result to cache
                self.cache[url] = result
            return result['html']
        
    def download(self,url,headers,proxy,num_retries,data=None):
        print 'Downloading:',url
        request = urllib2.Request(url,heeads=headers)
        #支持代理功能
        opener = urllib2.build_opener()
        if proxy:
            proxy_params={urlparse.urlparse(url).scheme:proxy}
            opener.add_handler(urllib2.ProxyHandler(proxy_params))
        try:
            html=opener.open(request).read()
        except urllib2.URLError as e:
            print 'Dowanload error:',e.reason
            html=None
            if num_retries >0:
                if hasattr(e, 'code') and 500 <= e.code < 600:
                    #recursively retry 5xx HTTP errors
                    code=e.code
                    html = self.download(url,headers, proxy,num_retries)
        return {'html':html,'code':code}

def conndb(data1):
    db=MySQLdb.connect(host='localhost',user='root',passwd='123456',db='data',port=3306,use_unicode=1,charset='utf8')
    cursor = db.cursor()
    sql = """INSERT INTO lol_url_list VALUES (%s,%s)"""
    try:
        #print "yes"
        # 执行sql语句
        #print sql
        print data1
        cursor.executemany(sql,data1)
        # 提交到数据库执行
        cursor.close()
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
            
    # 关闭数据库连接
    db.close()

def querydb():
    db=MySQLdb.connect(host='localhost',user='root',passwd='123456',db='data',port=3306,use_unicode=1,charset='utf8')
    cursor = db.cursor()
    try:
        #print "yes"
        # 执行sql语句
        #print sql
        cursor.execute("select * from yma0_urls")
        results=cursor.fetchall()
        for row in results:
            id = row[0]
            name = row[1]
            phone = row[2]
            links = row[3]
            print "%s %s %s %s" % (id,name,phone,links)
        # 提交到数据库执行
        cursor.close()
        db.commit()
    except:
        # 发生错误时回滚
        db.rollback()
            
    # 关闭数据库连接
    db.close()

def insert(data1):
    db=MySQLdb.connect(host='localhost',user='root',passwd='123456',db='data',port=3306,use_unicode=1,charset='utf8')
    cursor = db.cursor()
#     sql = """INSERT IGNORE INTO lol_url_list(name,urls) VALUES (%s,%s)"""
    sql = """INSERT IGNORE INTO yma0_urls(name,phone,links) VALUES (%s,%s,%s)"""
#     sql = "UPDATE lol_url_list SET name = 'xiaocang',urls = 'http://m.lolshipin.com/jieshuo/xiaocang/'\
#                                   WHERE id = '%d'" % (4) #更新
#     sql = "DELETE FROM lol_url_list WHERE id = '%d'" % (4) #删除
    try:
        #print "yes"
        # 执行sql语句
        #print sql
        print data1
        res=list(data1)
        print res
        cursor.executemany(sql,res)
        # 提交到数据库执行
        cursor.close()
        db.commit()
    except:
        print "error"
        # 发生错误时回滚
        db.rollback()
            
    # 关闭数据库连接
    db.close()
    
def download(url,headers,user_agent,proxy,num_retries):#初始化用户代理以及重试下载次数
    print 'Downloading:',url
    request = urllib2.Request(url,headers=headers)
    #支持代理功能
    opener = urllib2.build_opener()
    if proxy:
        proxy_params={urlparse.urlparse(url).scheme:proxy}
        opener.add_handler(urllib2.ProxyHandler(proxy_params))
        
    try:
#         html=urllib2.urlopen(request).read()
        html=opener.open(request).read()
#         print html
    except urllib2.URLError as e:
        print 'Dowanload error:',e.reason
        html=None
        if num_retries >0:
            if hasattr(e, 'code') and 500 <= e.code < 600:
                #recursively retry 5xx HTTP errors
                html = download(url, user_agent, proxy,num_retries)
#                 return download(url, num_retries-1)
    return html

def crawl_sitemap(url):#方法一：网站地图遍历
    #dowanload the sitemap file
    sitemap = download(url)
    #extract tne sitemap links
    links =re.findall('<loc>(.*?)</loc>',sitemap)
    #download each link
    for link in links:
        html = download(link)
        # scrape html here
        return html
    
def crawl_id():#方法二：ID遍历爬虫
        """
        count([n]):
        创建一个迭代器，生成从n开始的连续整数，
        如果忽略n，则从0开始计算（注意：此迭代器不支持长整数），
        如果超出了sys.maxint，计数器将溢出并继续从-sys.maxint-1开始计算。
        """
        # maximum number of consecutive download errors allowed
        max_errors = 5
        # current number of consecutive download errors
        num_errors = 0
        for page in itertools.count ( 1 ) :
            url = 'http://example.webscraping.com/view/-%d' % page 
            html =download(url)
            if html is None:
                # received an error trying to download this webpage 
                num_errors += 1
                if num_errors == max_errors:
                    # reached maximum number of 
                    # consecutive errors so exit
                    break 
            else :
                # success - ca口 scrape the result 
                # success - can scrape the result
                #num_errors = 0
                pass
 
def check_robot(url,robot_path,user_agent='wswp'):#解析robots.txt文件,以避免下载禁止爬取的URL
    rp = robotparser.RobotFileParser()
    rp.set_url(robot_path)
    rp.read()
    rp.can_fetch(user_agent, url)
    return rp.can_fetch(user_agent, url)

def link_crawler(seed_url,link_regex,out_file):#方法三：链接爬虫
    """
    Crawl from the given seed URL following links matched by link_regex
    """
    print seed_url
    fw=open(out_file,'w')
    crawl_queue = Queue.deque([seed_url])
    data=set()
    max_depth=5
    user_agent='wswp'
    headers={'User-agent': user_agent}
    proxy=None
    num_retries=2
    seen = {seed_url:0}
    # keep track which URL ’ s have seen before
#     seen = set(crawl_queue)
    while crawl_queue:
        url = crawl_queue.pop()
        # check url passes robots . txt restrictions
#         flag=check_robot(url,"https://www.baidu.com/robots.txt",user_agent='wswp')
        flag=1
        if flag:
            webpage = download(url,headers,user_agent,proxy,num_retries)
        else :
            print 'Blocked by robots.txt : ',url
        # filter for links matching our regular expression
        depth = seen[url]
        if depth != max_depth:
            if get_links(webpage):
                for link in get_links(webpage):
                    # check if link matches expected regex
                    if re.match(link_regex, link):
                        #/index/1时， 该链接只有网 页的路径部分， 而没有协议和服务器部分， 也就是说这是一个相对链接,使用了 urlparse模块来创建绝对路径。
                        # form absolute link
                        link = urlparse.urljoin(seed_url,link)
                        content = link.strip('\n').split('/')[-2]
                        res=()
                        # check if have already seen this link
                        if link not in seen:#防止同一个页面爬取多次
    #                         seen.add(link)
                            seen[link] = depth + 1
                            print link + ">>>>>ok<<<<<<"
                            crawl_queue.append(link) 
                        fw.write(str(link)+'\t'+str(content)+'\n')
                        res=(content,link)
                        data.add(res)

    insert(data)
    fw.close()
    
def get_links(page):
    """
    Return a list of links from html
    """
    # a regular expression to extract all links from the webpage
#     print page
    webpage_regex = re.compile(r'<a[^>]+href=["\'](.*?)["\']',re.IGNORECASE)
    # <a[^>]+href=["\'](.*?)["\']>.*</a>
    # list of all links from the webpage
    # print webpage_regex.findall(page)
    try:
        res=webpage_regex.findall(page)
        return res
    except:
        print "not finding"
        pass
#     return res
    
    
    
def main(url,out_file):
    throttle = Throttle(1)
    throttle.wait(url)
    link_crawler(url,'/',out_file)
    querydb()
    
'''
Created on 2016年11月21日

@author: weixy
'''

if __name__ == '__main__':
    
#     url='http://m.lolshipin.com'
    url = 'http://www.yma0.com/'
    out_file="/Users/weixy/desktop/test.txt"
    
    main(url,out_file)
