from oauth import OAuth
import datetime
import json
import os
import requests
from bvlib import bv_to_av

oauth_filename = 'oauth.json'
cache_coin_av_list = 'cache_voted_aid.json'


def main():
    auth = OAuth(oauth_filename)
    if not auth.is_login() or auth.get_expire_time() < datetime.datetime.now():
        username = input('user name or email address: ')
        password = input('password: ')
        auth.login(username, password)
        auth.save(oauth_filename)
    cookies = auth.get_cookies()
    if os.path.isfile(cache_coin_av_list):
        with open(cache_coin_av_list, 'r') as f:
            cache_list = json.load(f)
    else:
        cache_list = []
    cache_list = set(cache_list)
    exp_json = requests.get('https://www.bilibili.com/plus/account/exp.php', cookies=cookies).json()
    coins_left = 5 - int(exp_json['number'] / 10)
    page = 1
    while True:
        params = {
            'callback': 'abc',
            'pn': page,
            'ps': 100,
            'jsonp': 'jsonp'
        }
        history_json = json.loads(requests.get('https://api.bilibili.com/x/v2/history', params=params, cookies=cookies,
                                               headers={'Referer': 'https://www.bilibili.com'
                                                                   '/account/history'}).text[4:-1])
        for video in history_json['data']:
            bvid = video['bvid']
            aid = video.get('aid', bv_to_av(bvid))
            copyright = video['copyright']
            headers = {'Referer': f'https://www.bilibili.com/video/av{aid}'}
            if aid not in cache_list:
                params = {
                    'callback': 'abc',
                    'jsonp': 'jsonp',
                    'aid': aid
                }
                coin_json = json.loads(requests.get('https://api.bilibili.com/x/web-interface/archive/coins',
                                                    params=params, cookies=cookies, headers=headers).text[4:-1])
                available_coins = 3 - copyright - coin_json['data']['multiply']
                throw_coins = min(coins_left, available_coins)
                if throw_coins > 0:
                    params = {
                        'aid': aid,
                        'multiply': throw_coins,
                        'select_like': 0,
                        'cross_domain': True,
                        'csrf': cookies['bili_jct']
                    }
                    requests.post('https://api.bilibili.com/x/web-interface/coin/add', data=params, cookies=cookies,
                                  headers=headers)
                    print('throw %d coins to av%d' % (throw_coins, aid))
                if throw_coins == 0 or throw_coins == available_coins:
                    cache_list.add(aid)
                coins_left -= throw_coins
                if coins_left == 0:
                    with open(cache_coin_av_list, 'w') as f:
                        json.dump(list(cache_list), f)
                    return
        if len(history_json['data']) == 0:
            with open(cache_coin_av_list, 'w') as f:
                json.dump(list(cache_list), f)
            return
        page += 1


if __name__ == '__main__':
    main()
