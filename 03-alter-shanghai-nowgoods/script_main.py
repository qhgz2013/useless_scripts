import requests
from bs4 import BeautifulSoup
from email.mime.text import MIMEText
import smtplib
from time import sleep
from datetime import datetime
import re
import json
import sys
import os


with open(os.path.abspath(os.path.join(sys.argv[0], '..', 'config.json')), 'r') as f:
    config = json.load(f)
# DO NOT REDISTRIBUTE YOUR AUTH CODE!

auth_code = config['auth_code']
email_from = config['email_from']
email_to = config['email_to']

refresh_secs = config['refresh_secs']
subject = config['subject']
content = config['content']
keywords = config['keywords']


def log(s):
    print('[%s] %s' % (datetime.now().strftime('%y-%m-%d %H:%M:%S'), s))


def detect_url(url):
    req = requests.get(url, timeout=15)
    assert req.ok, 'Http responded %d' % req.status_code
    html = BeautifulSoup(req.text, features='html.parser')
    div_con = html.find('div', {'class': 'con'})
    li = div_con.find_all('li')
    for item in li:
        p = item.div.find_all('p')
        assert len(p) == 2, 'length mismatch'
        name = ''.join(p[0].strings)
        status = ''.join(p[1].strings)
        log('%s (%s)' % (name, status))
        for keyword in keywords:
            if keyword.lower() in name.lower() and '无' not in status:
                return True, div_con
    return False, div_con


def main():
    ptn_next_page = re.compile(r'nowgoods_page_(\d+)')
    while True:
        try:
            log('sending request')
            # get data from alter
            got_it, div_con = detect_url('http://alter-shanghai.cn/cn/nowgoods.html')
            if not got_it:
                pageinate = div_con.find('div', {'class': 'page'})
                if pageinate is not None:
                    last_page_href = pageinate.find_all('a')[-1].attrs['href']
                    max_page = int(re.search(ptn_next_page, last_page_href).group(1))
                    log('paginate detected, max_page = %d' % max_page)
                    for page in range(2, max_page+1):
                        got_it, _ = detect_url('http://alter-shanghai.cn/cn/nowgoods_page_%d.html' % page)
                        if got_it:
                            break
                    
            if got_it:
                # send mail
                log('sending e-mail')
                s = smtplib.SMTP_SSL('smtp.qq.com', 465)
                s.login(email_from, auth_code)
                for email in email_to:
                    msg = MIMEText(content)
                    msg['Subject'] = subject
                    msg['From'] = email_from
                    msg['To'] = email
                    s.sendmail(email_from, email, msg.as_string())
                    sleep(5)
                s.quit()
            log('waiting next cycle')
            sleep(refresh_secs)
        except Exception as ex:
            log(str(ex))
            log('Retry in 5 secs')
            sleep(5)


if __name__ == '__main__':
    main()
