# Source: https://www.zhihu.com/question/381784377/answer/1099438784
# Author: @mcfx
# License: WTFPL

_base58_str = 'fZodR9XQDSUm21yCkr6zBqiveYah8bt4xsWpHnJE7jL5VG3guMTKNPAwcF'
_base58_table = dict(zip(_base58_str, range(len(_base58_str))))
_xor = 177451812
_add = 8728348608
_access_order = [11, 10, 3, 8, 4, 6]


def bv_to_av(bv: str) -> int:
    r = 0
    for i in range(6):
        r += _base58_table[bv[_access_order[i]]] * (58 ** i)
    return (r - _add) ^ _xor


def av_to_bv(av: int) -> str:
    av = (av ^ _xor) + _add
    r = list('BV1  4 1 7  ')
    for i in range(6):
        r[_access_order[i]] = _base58_str[av // (58 ** i) % 58]
    return ''.join(r)


__all__ = ['bv_to_av', 'av_to_bv']


# TEST Stubs
if __name__ == '__main__':
    assert av_to_bv(170001) == 'BV17x411w7KC'
    assert av_to_bv(455017605) == 'BV1Q541167Qg'
    assert av_to_bv(882584971) == 'BV1mK4y1C7Bz'
    assert bv_to_av('BV17x411w7KC') == 170001
    assert bv_to_av('BV1Q541167Qg') == 455017605
    assert bv_to_av('BV1mK4y1C7Bz') == 882584971
