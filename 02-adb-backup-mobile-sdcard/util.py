# noinspection PyUnresolvedReferences
import hashlib
from typing import *
import pickle
import threading
from or_event import OrEvent
import subprocess
import datetime


def calculate_hash(x: Union[bytes, str], hash_type: Optional[str] = 'md5') -> str:
    if type(x) == str:
        x = x.encode('utf8')
    try:
        hash_class = eval('hashlib.' + hash_type)
    except NameError:
        raise ValueError('Unsupported hash type: %s' % hash_type)
    return hash_class(x).hexdigest()


def pickle_load(fp, allow_compression=False):
    try:
        if allow_compression:
            from zipfile import ZipFile
            with ZipFile(fp, 'r') as zip_fp:
                with zip_fp.open('__compressed') as zip_fp_internal:
                    return pickle.load(zip_fp_internal)
        else:
            return pickle.load(fp)
    except ImportError:
        return pickle.load(fp)


def pickle_dump(obj, fp, allow_compression=False):
    try:
        if allow_compression:
            from zipfile import ZipFile, ZIP_DEFLATED
            with ZipFile(fp, 'w', compression=ZIP_DEFLATED) as zip_fp:
                with zip_fp.open('__compressed', 'w') as zip_fp_internal:
                    pickle.dump(obj, zip_fp_internal)
        else:
            pickle.dump(obj, fp)
    except ImportError:
        pickle.dump(obj, fp)


def _input():
    try:
        input('Enter any string to exit process.\n')
    except EOFError:
        pass


def _cb(call_fc, x):
    try:
        call_fc()
    finally:
        x.set()


class AsyncInputInterrupter:
    def __init__(self, wait_obj_func):
        assert callable(wait_obj_func), '%s is not callable' % str(wait_obj_func)
        self._sig_fin = threading.Event()
        self._sig_int = threading.Event()
        self._wait_event = OrEvent(self._sig_fin, self._sig_int)
        thd = threading.Thread(target=_cb, args=(_input, self._sig_int), daemon=True)
        thd.start()
        thd = threading.Thread(target=_cb, args=(wait_obj_func, self._sig_fin), daemon=True)
        thd.start()

    def wait(self):
        try:
            while not self._wait_event.wait(1):
                pass
        except KeyboardInterrupt:
            self._sig_int.set()

    def is_set(self):
        return self._wait_event.is_set()


def camel_to_underline(s: str) -> str:
    ret_str = ''
    for char in s:
        if ord('A') <= ord(char) <= ord('Z'):
            if len(ret_str) > 0:
                ret_str += '_'
            ret_str += char.lower()
        else:
            ret_str += char
    return ret_str


def underline_to_camel(s: str) -> str:
    ret_str = ''
    i = 0
    while i < len(s):
        if s[i] == '_' and i + 1 < len(s):
            i += 1
            ret_str += s[i].upper()
        else:
            ret_str += s[i]
    return ret_str


def spawn_process(cmd: Union[str, List[str]], encoding: str) -> Tuple[str, str]:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout = p.stdout.read().decode(encoding)
    stderr = p.stderr.read().decode(encoding)
    return stdout, stderr


# fix a strange behavior that datetime.fromtimestamp(0).timestamp() will raise OSError [Errno 22] Invalid argument
def get_datetime_timestamp(dt: datetime) -> float:
    import sys
    if sys.platform == 'win32':
        try:
            return dt.timestamp()
        except OSError as e:
            if e.errno == 22:
                return 0
            else:
                raise
    else:
        return dt.timestamp()
