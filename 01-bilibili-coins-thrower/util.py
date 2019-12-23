import urllib.parse
import hashlib

appkey = '1d8b6e7d45233436'
appkey_secret = '560c52ccd288fed045859ed18bffd973'
app_header = {'User-Agent': 'Mozilla/5.0 BiliDroid/5.29.1 (bbcallen@gmail.com)'}

def calculate_sign(params: dict) -> str:
    keys = list(params.keys())
    keys.sort()
    sorted_param = dict()
    for key in keys:
        sorted_param[key] = params[key]
    form_data = urllib.parse.urlencode(sorted_param)
    form_data += appkey_secret
    sign = hashlib.md5(form_data.encode('utf8'))
    return sign.hexdigest()
