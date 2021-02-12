import requests
import time
import datetime
from warnings import warn
from bs4 import BeautifulSoup
import traceback
from requests.exceptions import *

# Parameters

# HTTP retry count for performing GET push.gd165.com
retry_count = 5
# HTTP retry delay (in secs) when HTTP GET failed
retry_delay = 5
# HTTP request timeout (in secs)
request_timeout = 10
# HTTP 204 check interval (in secs)
heartbeat_interval = 60
# Whether enable active mode
enable_active_mode = True
# Interval for sending interception data actively to push.gd165.com (in secs)
active_request_interval = 7200
# The duration of active mode after interception detected in passive mode (in secs)
active_request_duration = 86400
# HTTP 204 check url
http_204_url = 'http://connect.rom.miui.com/generate_204'

# Global variables, DO NOT MODIFY!

active_request_url = None
passive_request_timestamp = 0
active_request_timestamp = 0


def do_print(call_fn, msg, *args, **kwargs):
    call_fn('[%s] %s' % (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), msg), *args, **kwargs)


def do_interception(target_url):
    do_print(print, 'URL: %s' % target_url)
    req = None
    for _ in range(retry_count):
        try:
            session = requests.session()
            req = session.get(target_url, timeout=request_timeout)
            break
        except (TooManyRedirects, ReadTimeout, ConnectionError) as ex:
            do_print(print, 'Failed to get url: %s: Exception %s: %s' % (target_url, type(ex).__name__, str(ex)))
            session.close()
            session = requests.session()
            time.sleep(retry_delay)
    if req is None:
        do_print(print, 'Retry count exceeded, waiting for next heartbeat cycle')
        return
    assert req.ok, 'Failed to get push.gd165.com: HTTP %d' % req.status_code
    do_print(print, 'Headers: %s' % str(req.headers))
    do_print(print, 'Response Body:\n%s' % req.text)
    html = BeautifulSoup(req.text, features='html.parser')
    form = html.find('form')
    if form is None:
        do_print(print, 'Could not find form in html, ignored submit procedure')
        return
    inputs = form.find_all('input')
    post_value = {}
    for args in inputs:
        post_value[args.attrs['name']] = args.attrs['value']
    do_print(print, 'Submit form data!')
    do_print(print, 'post value: %s' % post_value)
    post_req = session.post('http://push.gd165.com:8090/PushPortalServer', data=post_value, timeout=request_timeout,
                            allow_redirects=False)
    assert post_req.ok, 'Failed to post push.gd165.com: HTTP %d' % post_req.status_code
    do_print(print, 'Headers: %s' % str(post_req.headers))
    do_print(print, 'Response Body:\n%s' % post_req.text)
    session.close()
    do_print(print, 'Interception response done')


def main():
    global active_request_url, active_request_timestamp, passive_request_timestamp
    while True:
        try:
            req = requests.get(http_204_url, allow_redirects=False, timeout=request_timeout)
            if req.status_code == 302:
                target_location = req.headers['Location']
                assert target_location.startswith('http://push.gd165.com'), 'Invalid 302 Redirection url: %s' % target_location
                do_interception(target_location)
                if enable_active_mode:
                    active_request_url = target_location
                    active_request_timestamp = time.time()
                    passive_request_timestamp = active_request_timestamp
            elif req.status_code != 204:
                do_print(print, 'HTTP status code: %d (expected 204)' % req.status_code)
            else:
                # do_print(print, 'Heartbeat done')
                t = time.time()
                if active_request_url is not None and t < passive_request_timestamp + active_request_duration and \
                        t >= active_request_timestamp + active_request_interval:
                    do_print(print, 'Active interception mode')
                    do_interception(active_request_url)
                    active_request_timestamp = t
        except (TooManyRedirects, ReadTimeout, ConnectionError) as ex:
            do_print(print, 'HTTP request failed: Exception %s: %s' % (type(ex).__name__, str(ex)))
        except Exception as ex:
            do_print(print, '%s: %s' % (type(ex).__name__, str(ex)))
            traceback.print_exc()
        finally:
            time.sleep(heartbeat_interval)


if __name__ == '__main__':
    main()
