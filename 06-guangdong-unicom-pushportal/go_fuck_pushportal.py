import requests
import time
import datetime
from warnings import warn
from bs4 import BeautifulSoup
import traceback
from requests.exceptions import *


def do_print(call_fn, msg, *args, **kwargs):
    call_fn('[%s] %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), msg), *args, **kwargs)


def main():
    while True:
        try:
            req = requests.get('http://connect.rom.miui.com/generate_204', allow_redirects=False, timeout=15)
            if req.status_code == 302:
                target_location = req.headers['Location']
                assert target_location.startswith('http://push.gd165.com'), 'Invalid 302 Redirection url: %s' % target_location
                do_print(print, 'URL: %s' % target_location)
                req = None
                for _ in range(5):
                    try:
                        session = requests.session()
                        req = session.get(target_location, timeout=15)
                        break
                    except (TooManyRedirects, ReadTimeout) as ex:
                        do_print(warn, 'Failed to get url: %s: Exception %s: %s' % (target_location, type(ex).__name__, str(ex)))
                        session.close()
                        session = requests.session()
                        time.sleep(5)
                if req is None:
                    do_print(warn, 'Retry count exceeded, waiting for next heartbeat cycle')
                    continue
                assert req.ok, 'Failed to get push.gd165.com: HTTP %d' % req.status_code
                do_print(print, 'Headers: %s' % str(req.headers))
                do_print(print, 'Response Body:\n%s' % req.text)
                html = BeautifulSoup(req.text, features='html.parser')
                form = html.find('form')
                if form is None:
                    do_print(warn, 'Could not find form in html, ignored submit procedure')
                    continue
                inputs = form.find_all('input')
                post_value = {}
                for args in inputs:
                    post_value[args.attrs['name']] = args.attrs['value']
                do_print(print, 'Submit form data!')
                do_print(print, 'post value: %s' % post_value)
                post_req = session.post('http://push.gd165.com:8090/PushPortalServer', data=post_value, timeout=15,
                                        allow_redirects=False)
                assert post_req.ok, 'Failed to post push.gd165.com: HTTP %d' % post_req.status_code
                do_print(print, 'Headers: %s' % str(post_req.headers))
                do_print(print, 'Response Body:\n%s' % post_req.text)
                session.close()
                do_print(print, 'Interception response done')
            elif req.status_code != 204:
                do_print(warn, 'HTTP status code: %d (expected 204)' % req.status_code)
            else:
                do_print(print, 'Heartbeat done')
        except Exception as ex:
            do_print(warn, '%s: %s' % (type(ex).__name__, str(ex)))
            traceback.print_exc()
        finally:
            time.sleep(60)


if __name__ == '__main__':
    main()
