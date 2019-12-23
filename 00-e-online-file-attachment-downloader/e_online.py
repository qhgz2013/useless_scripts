import requests
import hashlib
import re
import random
import xml.etree.ElementTree as ETree
import os


main_url = 'http://www.scut.edu.cn/e-online/'
sess = requests.session()
logintoken = None


def main_url_jump():
    global main_url
    req = sess.get(main_url, verify=False)
    assert req.ok
    ptn = re.compile(r'<meta\s+http-equiv="refresh"\s+content="\d+;URL=([^"]+)"')
    match = re.search(ptn, req.content.decode('gbk'))
    main_url = match.group(1)
    main_url = '%s://%s/meol/index.do' % (_parse_url(main_url, 'schemas'), _parse_url(main_url, 'host'))
    req = sess.get(main_url, verify=False)
    assert req.ok
    global logintoken
    ptn = re.compile(r'<input.+?name="logintoken"\s+value="(.+?)"\s*/>')
    match = re.search(ptn, req.content.decode('gbk'))
    if match is not None:
        logintoken = match.group(1)


def _get_md5(s: str) -> str:
    obj = hashlib.md5()
    obj.update(bytes(s, 'utf8'))
    return obj.hexdigest()


def _parse_url(url: str, url_part: str) -> str:
    ptn = re.compile(r'^(?P<schemas>https?)://(?P<host>[^/]+)(?P<path>/.*)$')
    assert url_part is None or url_part in ['schemas', 'host', 'path'], \
        'url_part should be None or one of "schemas", "host" or "path"'
    match = re.search(ptn, url)
    if match is not None:
        if url_part is None:
            return match.group(0)
        else:
            return match.group(url_part)


def create_dir(path):
    parent = os.path.abspath(path)
    dir_to_create = []
    while not os.path.exists(parent):
        dir_to_create.append(parent)
        parent = os.path.abspath(os.path.join(parent, '..'))
    dir_to_create = dir_to_create[::-1]
    for dir_path in dir_to_create:
        os.mkdir(dir_path)
        print('Directory %s created' % dir_path)

# noinspection PyShadowingNames
def login(username, password):
    s = _get_md5('v8_blue')
    s1 = _get_md5(password)
    password = _get_md5(s1 + s)
    schemas = _parse_url(main_url, 'schemas')
    host = _parse_url(main_url, 'host')
    url = '%s://%s/meol/loginCheck.do' % (schemas, host)
    req = sess.post(url, {'logintoken': logintoken, 'IPT_LOGINUSERNAME': username, 'IPT_LOGINPASSWORD': password},
                    headers={'Origin': '%s://%s' % (schemas, host), 'Referer': main_url}, verify=False)
    assert req.ok
    ptn = re.compile(r'<div\s+class="loginerror_mess">(.*?)</div>')
    match = re.search(ptn, req.content.decode('gbk').replace('\r', '').replace('\n', ''))
    if match:
        raise ValueError(match.group(1).strip(' '))


def get_course_list():
    schemas, host = _parse_url(main_url, 'schemas'), _parse_url(main_url, 'host')
    url = '%s://%s/meol/welcomepage/student/course_list_v8.jsp?r=%s' % (schemas, host, str(random.random()))
    req = sess.get(url, headers={'X-Requested-With': 'XMLHttpRequest', 'Referer': main_url})
    assert req.ok
    ptn = re.compile('<a\\s+href="###" onclick="window\\.open\\(\'\\.(?P<url>[^\']+)\'.*\\)')
    match = re.findall(ptn, req.content.decode('gbk'))
    result = set()
    ptn = re.compile(r'courseId=(\d+)')
    # noinspection PyShadowingNames
    for course in match:
        result.add(re.search(ptn, course).group(1))
    return list(result)


def download_course_resource(course_id: str, destination_path: str):
    schemas, host = _parse_url(main_url, 'schemas'), _parse_url(main_url, 'host')
    # url = '%s://%s/meol/jpk/course/layout/newpage/index.jsp?courseId=%s' % (schemas, host, course_id)
    # req = sess.get(url, headers={'Referer': main_url})
    # assert req.ok
    # ptn = re.compile(r'<a\s+title="课程资源"\s+href="(?P<url>[^"]+)"\s*/?\s*>')
    # match = re.search(ptn, req.content.decode('gbk'))
    # url = url + match.group('url')
    # req = sess.get(url, headers={'Referer': main_url})
    # assert req.ok
    url = '%s://%s/meol/common/script/xmltree.jsp?lid=%s&groupid=4&_=5203' % (schemas, host, course_id)
    req = sess.get(url, headers={
        'X-Requested-With': 'XMLHttpRequest',
        'Referer': '%s://%s/meol/common/script/left.jsp?lid=%s&groupid=4' % (schemas, host, course_id)})
    assert req.ok
    create_dir(destination_path)
    xml_tree = ETree.fromstring(req.content.decode('utf8'))
    # print(xml_tree)
    parse_document_tree(xml_tree.find('item'), destination_path, course_id)


def parse_document_tree(node, directory, course_id):
    content = node.find('content')
    name = content.find('name')
    href = name.attrib['href']
    ptn = re.compile(r'listview.jsp\?lid=(\d+)&folderid=(\d+)')
    match = re.search(ptn, href)
    lid = match.group(1)
    folderid = match.group(2)
    schemas, host = _parse_url(main_url, 'schemas'), _parse_url(main_url, 'host')
    req = sess.get('%s://%s/meol/common/script/listview.jsp?lid=%s&folderid=%s' % (schemas, host, lid, folderid),
                   headers={'Referer': '%s://%s/meol/common/script/left.jsp?lid=%s&groupid=4' %
                                       (schemas, host, lid)})
    assert req.ok
    create_dir(directory)
    # todo: parse request
    ptn = re.compile(r'<a\s+href="preview/download_preview.jsp\?fileid=(\d+)&resid=(\d+)&lid=(\d+)')
    files = re.finditer(ptn, req.content.decode('gbk'))
    for file in files:
        download_course_data(file.group(3), file.group(1), file.group(2), os.path.join(directory, name.text))
    for dirs in node.findall('item'):
        parse_document_tree(dirs, os.path.join(directory, name.text), course_id)


def download_course_data(course_id: str, file_id: str, res_id: str, destination_path: str):
    schemas, host = _parse_url(main_url, 'schemas'), _parse_url(main_url, 'host')
    url = '%s://%s/meol/common/script/preview/download_preview.jsp?fileid=%s&resid=%s&lid=%s' % \
          (schemas, host, file_id, res_id, course_id)
    req = sess.get(url, headers={'Referer': '%s://%s/meol/common/script/left.jsp?lid=%s&groupid=4' %
                                            (schemas, host, course_id)})
    assert req.ok
    ptn = re.compile(r'<p>文件名:<span>(.+?)</span>')
    match = re.search(ptn, req.content.decode('gbk'))
    file_name = match.group(1)
    url = '%s://%s/meol/common/script/download.jsp?fileid=%s&resid=%s&lid=%s' % \
          (schemas, host, file_id, res_id, course_id)
    req = sess.get(url, headers={'Referer': '%s://%s/meol/common/script/left.jsp?lid=%s&groupid=4' %
                                            (schemas, host, course_id)})
    assert req.ok
    file_name = os.path.join(destination_path, file_name)
    create_dir(destination_path)
    print('downloading: %s' % file_name)
    with open(file_name, 'wb') as f:
        f.write(req.content)


def logoff():
    global sess
    schemas, host = _parse_url(main_url, 'schemas'), _parse_url(main_url, 'host')
    url = '%s://%s/meol/homepage/V8/include/logout.jsp' % (schemas, host)
    req = sess.get(url, headers={'Referer': main_url})
    assert req.ok
    sess = None


if __name__ == '__main__':
    main_url_jump()
    username = input('username: ')
    password = input('password: ')
    savepath = input('savepath: ')
    login(username, password)
    try:
        course = get_course_list()
        for c in course:
            download_course_resource(c, savepath)
    finally:
        logoff()
