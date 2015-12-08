# -*- coding:UTF-8 -*-
##############################################################################
###Author: Guixing(Fred) Wei##################################################
###Version:1.0.0#############################################################
######The logic of this sciprt is to simulating logging in wap.weibo and then#
# get the usrs' dob and put these information into datbase.#################
# this script is for scraping moblie_weibo###################################
#before you use this, please change to your own account and password#########
#and change the proxy list. you cannot access below proxy coz not authorized##
#and change self.pattern_users to your own user name#########################
############################################################################





import requests
from bs4 import BeautifulSoup
import re
import psycopg2
import time
import random
import time


class mweibo():
    def __init__(self, user=None, password=None):
        self.user = user
        self.password = password
        self.header_pool = [
            {'User-Agent': r'Mozilla/5.0 (Linux; Android 4.4.2; Nexus 4 Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/34.0.1847.114 Mobile Safari/537.36'},
            {'User-Agent': r'Mozilla/5.0 (compatible; MSIE 10.0; Windows Phone 8.0; Trident/6.0; IEMobile/10.0; ARM; Touch; NOKIA; Lumia 920)'},
            {'User-Agent': r'Mozilla/5.0 (BlackBerry; U; BlackBerry 9900; en) AppleWebKit/534.11+ (KHTML, like Gecko) Version/7.1.0.346 Mobile Safari/534.11+'},
            {'User-Agent':r'Opera/12.02 (Android 4.1; Linux; Opera Mobi/ADR-1111101157; U; en-US) Presto/2.9.201 Version/12.02'},
            {'User-Agent':r'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_5_7; en-us) AppleWebKit/530.17 (KHTML, like Gecko) Version/4.0 Safari/530.17 Skyfire/2.0'}
            ]
        self.session = requests.session()

        self.pattern_rand = r'rand=([0-9]+)(?=\&)'
        self.pattern_vk = r'"vk" value="(.+?)(?=")'
        self.pattern_submit = r'[0-9]{4}(?=\_)'
        #self.pattern_dob = r'([0-9]{4}\-[0-12][0-9]*\-[0-31][0-9]*)(?=</p>)'
        self.pattern_dob = r'([0-9]{4}\-[0-12][0-9]*\-[0-31][0-9]*)'
        self.pattern_users = r'your weibo account name'####please change to your weibo account name
        self.pattern_access = u'详细资料'
        self.dict = {}
        self.host = 'localhost'
        self.database = 'weibo'
        self.user = 'postgres'
        self.password = 'pgsql2015'
        self.pool = [('your own weibo account','password'),('your own weibo account','password')
                     ]
        #self.pool = [('cherrywangxujiao@163.com','wangxujiao123456')]
        #              ('wotanb42236@163.com','a123456'),('miyan3350240@163.com','a123456'),]
        # self.pool = [('weiguixing429@gmail.com','112358'),('moyan429@hotmail.com','112358'),
        #              ('hedou4951@163.com','a123456'),('kuihuaiky320302@163.com','a123456'),
        #              ('wotanb42236@163.com','a123456'),('miyan3350240@163.com','a123456'),
        #              ('choulukc47504@163.com','a123456'),('xinkuavf68609@163.com','a123456'),
        #              ('liouxin290@163.com','a123456'),('jiutao422573150@163.com','a123456'),
        #              ('meilu3039188@163.com','a123456'),('jishucy0774696@163.com','a123456'),
        #              ('cebeihk991928@163.com','a123456'),('pingbaofenlk20030@163.com','a123456'),
        #              ('yundunl9963@163.com','a123456'),('zhimu765176@163.com','a123456'),
        #              ('futangh143035@163.com','a123456'),('fuseqb7282377@163.com','a123456'),
        #              ('ziye1991125625@163.com','a123456'),('xiutiaog895076@163.com','a123456'),
        #              ('gaixianzhangzi@163.com','a123456'),('julingpul3612@163.com','a123456')]
        self.total = 0
        self.ini_time = time.time()
        self.proxies = [
            {'http':'http://93.119.23.81:8800'},
            {'http':'http://93.119.22.33:8800'},
            {'http':'http://93.119.22.199:8800'},
            {'http':'http://93.119.23.178:8800'},
            {'http':'http://93.119.20.237:8800'},
            {'http':'http://93.119.23.58:8800'},
            {'http':'http://93.119.20.124:8800'},
            {'http':'http://93.119.22.232:8800'},
            {'http':'http://93.119.23.157:8800'},
            {'http':'http://93.119.20.105:8800'}
        ]


    def getrand(self):
        try:
            resp = self.session.get(
                'http://login.weibo.cn/login/?backURL=http%3A%2F%2Fweibo.cn%2F&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt=4&revalid=2&ns=1&pt=1&sudaref=3g.sina.com.cn',
                headers=self.header,timeout=10)
            self.rand = re.search(self.pattern_rand, resp.text).group(1)
            self.vk = re.search(self.pattern_vk, resp.text).group(1)
            self.submit = re.search(self.pattern_submit, self.vk).group(0)
            print "suceefully pass the getrand"
        except Exception as err:
            print err.message
            self.pool_manage()
            self.getrand()


    def pool_manage(self):
        len_pool = len(self.pool)
        len_header = len(self.header_pool)
        len_proxy = len(self.proxies)
        index_rand = random.randrange(0,len_pool,1)
        self.login_name = self.pool[index_rand][0]
        self.login_passwd = self.pool[index_rand][1]
        index_header = random.randrange(0,len_header,1)
        self.header = self.header_pool[index_header]
        self.pattern_user =self.pattern_users
        index_rand_proxy = random.randrange(0,len_proxy,1)
        self.session.proxies=self.proxies[index_rand_proxy]

    def login(self):

        self.pool_manage()
        self.getrand()
        url = 'https://login.weibo.cn/login/?rand={0}&backURL=http%3A%2F%2Fweibo.cn%2F&backTitle=%E6%96%B0%E6%B5%AA%E5%BE%AE%E5%8D%9A&vt=4&revalid=2&ns=1'.format(
            self.rand)
        button_submit = "password_" + self.submit
        param = {"mobile": self.login_name,
                 button_submit: self.login_passwd,
                 "remember": "on",
                 "backURL": "http%3A%2F%2Fweibo.cn%2F",
                 "backTitle": "新浪微博",
                 "tryCount": "",
                 "vk": self.vk,
                 "submit": "登录"
        }
        print "I am starting logging"
        try:
            self.resp = self.session.post(url, headers=self.header, data=param,timeout=15)
            print "ok, I got response from loggin"

            while hasattr(self.resp, 'location'):
                location = self.resp.location
                self.resp = self.session.get(location)

            if re.search(self.pattern_user, self.resp.text):
                print 'login sucessfully!'
                print('the current account is {0}'.format(self.login_name))
            else:
                print 'login failed,pls check'
                print('the current account is {0}'.format(self.login_name))
                self.login()
        except Exception as err:
            print "loging got no response within 15 seconds"
            print err.message
            self.login()


    def accessweb(self):
        #time.sleep(random.randrange(3, 10, 1 ))
        url = 'http://m.weibo.cn/users/' + self.uid
        try:
            self.resp = self.session.get(url, headers=self.header,timeout=10)
        except Exception as err:
            print "something wrong happened during acess the specified website by proxy"
            print err.message
            self.pool_manage()
            self.accessweb()

        if self.resp.text:
            soup_object = BeautifulSoup(self.resp.text, 'html.parser')
            _section = soup_object.find_all('section')
            for m in range(0, len(_section)):
                for i in range(0, len(_section[m])):
                    if (_section[m].contents[i].span) and (_section[m].contents[i].p):
                        self.dict[_section[m].contents[i].span.string] = _section[m].contents[i].p.string

            if self.dict.has_key(u'生日'):
                self.dob = self.dict[u'生日']
                dob = re.search(self.pattern_dob,self.dob)
                if not dob:
                    self.dob =None
                print ("current user {0} 's dob is {1}".format(self.uid, self.dob))
            else:
                self.dob =None
            self.dict ={}
        else:
            print "Okay, the response is 403 forbidden, trying to change to another account and proxy"

            self.login()
            self.accessweb()

    def conn_loop(self):
        try:
            conn = psycopg2.connect(host=self.host, database=self.database, user=self.user, password=self.password)
            conn.autocommit = True
            cur = conn.cursor()
            while True:

                cur.execute('SELECT t.uid FROM test.users as t where dob is Null;')
                self.row = cur.fetchone()
                if not self.row:
                    break
                else:
                    self.uid = self.row[0]
                    print ('the current uid is {0}'.format(self.uid))
                    self.accessweb()
                    if self.dob:
                        cur.execute("update test.users set dob = '{0}' where test.users.uid='{1}';".format(self.dob,self.uid))
                    else:
                        cur.execute("update test.users set dob = '1111-11-11' where test.users.uid='{0}';".format(self.uid))
                    self.total += 1
                    print ('the script has already processed {0} users'.format(self.total))
                    print (time.time()-self.ini_time)

        except Exception as err:
            print err.message


if __name__=="__main__":
    scraper = mweibo()
    scraper.login()
    scraper.conn_loop()

