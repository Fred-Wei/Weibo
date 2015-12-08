##########################################################################################################
######this scirpt allow you to collect weibo data with location information and remove the duplication####
#######author:Guixing Wei  Email: g_w38@txstate.edu#######################################################
#######before you use this scirpt, please use your own app account&key for api_key and api_secrect#######
######### and weibo account to for wblogin#######################################################################

from __future__ import absolute_import, division, print_function, unicode_literals

import re
import json
import base64
import binascii
import weibo
from weibo import APIClient
import rsa
import requests
import codecs
import random
import sys,os

import logging

# logging.basicConfig(level=logging.DEBUG)
import time

#output debug informaiton to console
debug_output = 0
email_flag = 1

#get starting time,the time difference is used to determine whether the script stops
start_time = time.clock()
#the client account
api_key = "your own key"
api_secret = "your own secrect"
callback_url = "http://oauth.weico.cc"

#initialize the APIClient(weibo_SDK)
client = APIClient(app_key=api_key, app_secret=api_secret, redirect_uri=callback_url)

#get encoded authorization url
referer_url = client.get_authorize_url()

#js string for logging
WBCLIENT = 'ssologin.js(v1.4.5)'
user_agent = (
    'Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.11 (KHTML, like Gecko) '
    'Chrome/20.0.1132.57 Safari/536.11'
)
# use requests.session to keep the cookies
session = requests.session()
session.headers['User-Agent'] = user_agent

# the encryption of key and passward
def encrypt_passwd(passwd, pubkey, servertime, nonce):
    key = rsa.PublicKey(int(pubkey, 16), int('10001', 16))
    message = str(servertime) + '\t' + str(nonce) + '\n' + str(passwd)
    passwd = rsa.encrypt(message.encode('utf-8'), key)
    return binascii.b2a_hex(passwd)

# login the Sina home page and get the session cookies
def wblogin(username, password):
    resp = session.get(
        'http://login.sina.com.cn/sso/prelogin.php?'
        'entry=sso&callback=sinaSSOController.preloginCallBack&'
        'su=%s&rsakt=mod&client=%s' %
        (base64.b64encode(username.encode('utf-8')), WBCLIENT)
    )

    pre_login_str = re.match(r'[^{]+({.+?})', resp.text).group(1)
    pre_login_str_tmp = re.match(r'[^{]+({.+?})', resp.text).group()
    text_tmp = resp.text
    pre_login = json.loads(pre_login_str)

    pre_login = json.loads(pre_login_str)
    data = {
        'entry': 'weibo',
        'gateway': 1,
        'from': '',
        'savestate': 7,
        'userticket': 1,
        'ssosimplelogin': 1,
        'su': base64.b64encode(requests.utils.quote(username).encode('utf-8')),
        'service': 'miniblog',
        'servertime': pre_login['servertime'],
        'nonce': pre_login['nonce'],
        'vsnf': 1,
        'vsnval': '',
        'pwencode': 'rsa2',
        'sp': encrypt_passwd(password, pre_login['pubkey'],
                             pre_login['servertime'], pre_login['nonce']),
        'rsakv': pre_login['rsakv'],
        'encoding': 'UTF-8',
        'prelt': '115',
        'url': 'http://weibo.com/ajaxlogin.php?framelogin=1&callback=parent.si'
               'naSSOController.feedBackUrlCallBack',
        'returntype': 'META'
    }
    resp = session.post(
        'http://login.sina.com.cn/sso/login.php?client=%s' % WBCLIENT,
        data=data
    )

    login_url = re.search(r'replace\([\"\']([^\'\"]+)[\"\']',
                          resp.text).group(1)
    resp = session.get(login_url)
    login_str = re.match(r'[^{]+({.+?})\)', resp.text).group(1)
    return json.loads(login_str)

#when the script stop running, send email (pls use gmail)to me. When you get email, pls check the script
# def send_email():
#             import smtplib
#             gmail_user = "weiguixing429@gmail.com"
#             gmail_pwd = ""
#             FROM = 'weiguixing429@gmail.com'
#             TO = ['g_w38@txstate.edu'] #must be a list
#             SUBJECT = "Weibo_data"
#             TEXT = "Congradulations!!! It seems you need to check the Weibo Script(linux)"
#
#             # Prepare actual message
#             message = """\From: %s\nTo: %s\nSubject: %s\n\n%s
#             """ % (FROM, ", ".join(TO), SUBJECT, TEXT)
#             try:
#                 #server = smtplib.SMTP(SERVER)
#                 server = smtplib.SMTP("smtp.gmail.com", 587) #or port 465 doesn't seem to work!
#                 server.ehlo()
#                 server.starttls()
#                 server.login(gmail_user, gmail_pwd)
#                 server.sendmail(FROM, TO, message)
#                 #server.quit()
#                 server.close()
#                 print ('successfully sent the mail')
#             except:
#                 print ("failed to send mail")

#use initialized session to get code(set allow_redirects false to save time)
def get_code():
    resp = session.get(
        url=referer_url, allow_redirects=False
    )
    location = resp.headers['Location']
    code = location[-32:]
    return code

#use acquired code to get token
def get_token():
    """

    :rtype :
    """
    code = get_code()
    flag = 1
    while flag == 1:
        try:
            token = client.request_access_token(code)
        #to avoid frequent call of same account
        except weibo.APIError:
            time.sleep(random.randrange(120, 300, 60))
            token = client.request_access_token(code)
        except:
            print
            "Unexpected error:", sys.exc_info()[0]
            continue
        else:
            flag = 0
    client.set_access_token(token.access_token, token.expires_in)
    if debug_output == 1:
        print(token)

# this function is used to check the remaining calling status
def get_limit_status():
    limit_status = client.account.rate_limit_status.get()
    if limit_status is None:
        print
        "Error Occurred"
        return False
    else:
        print
        limit_status.remaining_ip_hits

#write the acquired weibo to json file
def write_json(s, n):
    a = int(n / 1000)
    b = int(n % 1000)
    file_name = "/home/g_w38/weibo/Weibo_data_collect/{0}.json".format(a)
    with codecs.open(filename=file_name, mode='a+') as infile:
        json.dump(s, fp=infile, indent=4)
    global cur_file
    cur_file = file_name

#read json file
def read_json():
    with codecs.open(filename='json_practice.json', mode='r')as outfile:
        print
        outfile.read().decode('unicode_escape')

# delelte the duplication between consecutive returns
def judge_del_duplication(states_id_old, new_returned):
    duplication_index = 999
    states = new_returned['states']
    states_id_old = states_id_old['states']
    index_num = 0
    for id_new in states:
        find = 0
        for id_old in states_id_old:
            if id_new['id'] == id_old['id']:
                duplication_index = index_num
                find = 1
                break
        if find == 1:
            break
        index_num += 1
    if duplication_index != 999:
        del new_returned['states'][duplication_index:], new_returned['statuses'][duplication_index:]
    return new_returned

# call "public_location" API for 5000000 times
def get_public_location():
    global email_flag
    states_id_old = {"states": [{'state': 0, 'id': '0'}]}
    N = 1
    while N < 5000000:
        print("I am starting new loop")
        try:
            public_location = client.place.public_timeline.get()
            if debug_output == 1:
                print('successfully call public_location')
                print(public_location)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            time.sleep(random.randrange(60, 180, 60))
            #get_token()
            pass
            continue
        else:
            if public_location is None:
                print("returned data is None")
                time.sleep(random.randrange(60, 180, 60))
                #get_token()
                continue

            else:

                states_id_old = judge_del_duplication(states_id_old, public_location)
                write_json(states_id_old, N)
                timepass = round(time.clock() - start_time)
                timepass_min = timepass / 60
                if N % 100 == 0:
                    if debug_output == 1:
                        print('run 100 times')
                    time.sleep(random.randrange(60, 180, 60))
                else:
                    time.sleep(random.randrange(15, 35, 1))
                N += 1
                if debug_output == 1:
                    print('APIs has been called %s times' % N)
                    print('I have been running for %s minutes' % timepass_min)
                    #get_limit_status()
                    #print 'successfully call limit_status'
        finally:
            if cur_file is None:# to avoid the crash due to failure of the first calling
                current_file = "/home/g_w38/weibo/Weibo_data_collect/0.json"
            else:
                current_file = cur_file
            a = os.stat(current_file).st_mtime
            b = time.time()
            time_dif = round((b - a) / 60)
            if time_dif > 15: # if the time difference is larger than 15 minutes, send email to me and re-login
                print ('I stop working but I am restarting')
                if email_flag == 1:
                    send_email()
                    email_flag = 0
                wblogin('your own weibo account', 'your own password')

                get_token()


def file_detection():
    dir = '/home/g_w38/weibo/Weibo_data_collect'
    old_jsons = []
    files = os.listdir(dir)
    for f in files:
        if f.endswith('.json'):
            old_jsons.append(f)
    if old_jsons:
        for json_file in old_jsons:
            dir_file = dir+'/'+json_file
            lst_mod_time = time.localtime(os.stat(dir_file).st_mtime)
            file_year = str(lst_mod_time[0])
            file_month = str(lst_mod_time[1])
            file_day = str(lst_mod_time[2])
            file_hour =str(lst_mod_time[3])
            file_min = str(lst_mod_time[4])
            file_date = [file_year,file_month,file_day,file_hour,file_min]
            for i in range(0,5):
                if int(file_date[i])<10:
                    file_date[i] = '0'+ file_date[i]

            new_file_name = file_date[0]+file_date[1]+file_date[2]+file_date[3]+file_date[4]
            old_name = dir_file
            new_name =  dir+'/'+ new_file_name+ ".json"
            #print new_name
            os.rename(old_name, new_name)



if __name__ == '__main__':
	# the sequence is that check existing json file->wblogin->get_code->get_token->call API

    file_detection()
    wblogin('your own weibo account', 'your own password')
    #wblogin('st.dlng@hotmail.com','814924tdl')e
    get_token()
    get_public_location()
    # timeline
    # print(session.get('http://weibo.com/').text)
