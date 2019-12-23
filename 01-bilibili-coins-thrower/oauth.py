import requests
import json
# from . import config
import util
import time
import base64
from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_v1_5
import os
import datetime
import threading


class OAuth:
    _API_HOST = 'https://passport.bilibili.com'

    def __init__(self, oauth_file_path):
        self._oauth_file_path = oauth_file_path
        self._response_json = None
        self._global_lock = threading.RLock()
        self.load()

    def login(self, username: str, password: str) -> None:
        with self._global_lock:
            # POST /api/oauth2/getKey
            params = {
                'appkey': util.appkey,
                'build': '5291001',
                'mobi_app': 'android',
                'platform': 'android',
                'ts': int(time.time())
            }
            params['sign'] = util.calculate_sign(params)
            req_key = requests.post(OAuth._API_HOST + '/api/oauth2/getKey', data=params, headers=util.app_header)

            # handling response JSON
            json_key = json.loads(req_key.content)
            if json_key['code'] != 0:
                raise Exception("API call failed: OAuth->getKey: %s" % json_key['message'])

            json_hash = json_key['data']['hash']
            json_key = json_key['data']['key']

            password = json_hash + password

            # RSA encryption
            rsa_key = RSA.import_key(json_key)
            cipher = PKCS1_v1_5.new(rsa_key)
            encrypted_password = base64.b64encode(cipher.encrypt(password.encode('utf8')))

            # POST /api/v3/oauth2/login
            params = {
                'appkey': util.appkey,
                'build': '5291001',
                'mobi_app': 'android',
                'password': encrypted_password,
                'platform': 'android',
                'ts': int(time.time()),
                'username': username,
            }
            params['sign'] = util.calculate_sign(params)
            req_login = requests.post(OAuth._API_HOST + '/api/v3/oauth2/login', data=params, headers=util.app_header)

            # handling response JSON
            json_result = json.loads(req_login.content)
            if (json_result['code']) != 0:
                raise Exception("API call failed: OAuth->login: %s" % json_result['message'])
            self._response_json = json_result
            self.save()

    def logout(self) -> None:
        # todo: implement this
        raise NotImplementedError

    def refresh_token(self) -> None:
        # todo: implement this
        self._response_json = None

    def load(self, filename: str = None) -> None:
        with self._global_lock:
            if filename is not None:
                self._oauth_file_path = filename
            if os.path.exists(self._oauth_file_path):
                with open(self._oauth_file_path, 'r') as f:
                    self._response_json = json.load(f)
            self._check_expiration()

    def save(self, filename: str = None) -> None:
        with self._global_lock:
            if filename is not None:
                self._oauth_file_path = filename
            if self._response_json is not None:
                with open(self._oauth_file_path, 'w') as f:
                    json.dump(self._response_json, f)

    def _check_expiration(self):
        if self._response_json is not None:
            if datetime.datetime.fromtimestamp(self._response_json['ts']) + \
                    datetime.timedelta(seconds=self._response_json['data']['token_info']['expires_in']) < \
                    datetime.datetime.now():
                self.refresh_token()

    def get_access_token(self) -> str:
        if self._response_json is not None:
            with self._global_lock:
                self._check_expiration()
                return self._response_json['data']['token_info']['access_token']

    def get_refresh_token(self) -> str:
        if self._response_json is not None:
            with self._global_lock:
                self._check_expiration()
                return self._response_json['data']['token_info']['refresh_token']

    def get_expire_time(self) -> datetime.datetime:
        if self._response_json is not None:
            with self._global_lock:
                self._check_expiration()
                ts_base = self._response_json['ts']
                expire_secs = self._response_json['data']['token_info']['expires_in']
                return datetime.datetime.fromtimestamp(ts_base) + datetime.timedelta(seconds=expire_secs)

    def is_login(self) -> bool:
        return self._response_json is not None

    def get_cookies(self) -> dict:
        cookie_list = self._response_json['data']['cookie_info']['cookies']
        ret_dict = dict()
        for cookie in cookie_list:
            ret_dict[cookie['name']] = cookie['value']
        return ret_dict
