import os
import subprocess
from typing import *
from datetime import datetime
import re
import argparse


# This "ls -al" pattern is tested on Android 8.0 (Mi 5s) and Android 4.4.4 (Redmi Note 1 LTE)
# If it is not compatible for your device, modify it by yourself
ls_al_pattern = re.compile(r'^(?P<permission>[dcbl-]([r-][w-][x-]){3}\+?)\s+'
                           r'((?P<links>\d+)\s+)?'
                           r'(?P<owner_name>[a-zA-Z_]+)\s+'
                           r'(?P<owner_group>[a-zA-Z_]+)\s+'
                           r'((?P<file_size>\d+)\s+)?'
                           r'(?P<last_modification>\d+-\d+-\d+\s\d+:\d+)\s'
                           r'(?P<name>.+?)\s*$')


# entity class
class RemoteFileMeta:
    __slots__ = ['permission', 'links', 'owner', 'group', 'file_size', 'last_modification', 'name']

    def __init__(self, permission: str, links: int, owner: str, group: str, file_size: int,
                 last_modification: datetime, name: str):
        self.permission = permission
        self.links = links
        self.owner = owner
        self.group = group
        self.file_size = file_size
        self.last_modification = last_modification
        self.name = name


# spawn a process, returns its stdout and stderr output
def spawn_process(cmd: str, encoding: str) -> Tuple[str, str]:
    print(cmd)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    stdout = p.stdout.read().decode(encoding)
    stderr = p.stderr.read().decode(encoding)
    return stdout, stderr


# creates a directory with its parent directories
def create_dir(path):
    parent = os.path.abspath(path)
    dir_to_create = []
    while not os.path.exists(parent):
        dir_to_create.append(parent)
        parent = os.path.abspath(os.path.join(parent, '..'))
    dir_to_create = dir_to_create[::-1]
    for dir_path in dir_to_create:
        os.mkdir(dir_path)
        print('[Local] mkdir:', dir_path)


# remove a directory and its files and sub-directories
def remove_dir(path):
    _, dirs, files = next(os.walk(path))
    for file in files:
        full_path = os.path.join(path, file)
        os.remove(full_path)
        print('[Local] remove:', full_path)
    for dir_ in dirs:
        remove_dir(os.path.join(path, dir_))
    os.rmdir(path)
    print('[Local] rmdir:', path)


def _to_int(x):
    return int(x) if x is not None else None


# list remote file, returns a tuple of directory and file list
def adb_ls(path: str) -> Tuple[List[RemoteFileMeta], List[RemoteFileMeta]]:
    if path[-1] != '/':
        path = path + '/'
    path = path.replace("'", "'\\\"'\\\"'")  # Enjoying escaping quotes and back slashes
    stdout, stderr = spawn_process(f"adb shell ls -al \"'{path}'\"", 'utf8')
    if len(stderr) > 0:
        raise RuntimeError(stderr)
    stdout = stdout.split('\n')
    dirs = []
    files = []
    for line in stdout:
        if len(line) == 0:
            continue
        match = re.match(ls_al_pattern, line)
        if match is None:
            continue
        last_mod = datetime.strptime(match.group('last_modification'), '%Y-%m-%d %H:%M')
        file_meta = RemoteFileMeta(match.group('permission'), _to_int(match.group('links')), match.group('owner_name'),
                                   match.group('owner_group'), _to_int(match.group('file_size')), last_mod,
                                   match.group('name'))
        if file_meta.name == '.' or file_meta.name == '..':
            continue
        if file_meta.permission[0] == 'd':
            dirs.append(file_meta)
        elif file_meta.permission[0] == '-':
            files.append(file_meta)
        else:
            print('Unsupported file permission attribute:', file_meta.permission)
    return dirs, files


def adb_pull(remote_path: str, local_path: str):
    # 预创建目标文件，因为诡异的adb在某些情况下会报错
    try:
        open(local_path, 'wb').close()
        remote_path = remote_path.replace('"', '\\"')
        local_path = local_path.replace('"', '\\"')
        # NEW: 把文件路径 x:\a\b.txt 转成 \\?\x:\a\b.txt 这种形式，能避开260字节的路径长度限制
        # 如果不这样做，在文件路径长度超过260时，直接调用adb会抛一个"Not a directory"的错误
        local_path = '\\\\?\\' + local_path.replace('/', '\\')
        stdout, stderr = spawn_process(f'adb pull "{remote_path}" "{local_path}"', 'utf8')
        if len(stderr) > 0:
            raise RuntimeError(stderr)
        if stdout.startswith("adb: error:"):
            raise RuntimeError(stdout)
    except FileNotFoundError:
        from warnings import warn
        warn('Could not create file: %s' % local_path, RuntimeWarning)


def escape_windows_file_name(file: Union[str, List[str]]) -> Union[str, List[str]]:
    # 因为linux可以创建一个"."结尾的文件/文件夹，而windows不能，所以要把没有拓展名时的那个"."去掉
    escape_char = '*?<>|":'

    def _func_replace(x):
        for c in escape_char:
            x = x.replace(c, '_')
        return x
    if type(file) == str:
        file = _func_replace(file)
        return file[:-1] if file.endswith('.') else file
    else:
        file = [_func_replace(x) for x in file]
        return [(x[:-1] if x.endswith('.') else x) for x in file]


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


def do_backup(remote_path: str, local_path: str, skip_exist: bool = True):
    remote_path = remote_path.rstrip('/')
    create_dir(local_path)
    dirs = [(remote_path, local_path)]
    total = 1
    finished = 0
    while len(dirs) > 0:
        cur_remote_path, cur_local_path = dirs.pop(0)
        print('[%d / %d]' % (finished, total), end=' ')
        remote_dir_meta_list, remote_file_meta_list = adb_ls(cur_remote_path)
        remote_dir_name_list = [x.name for x in remote_dir_meta_list]
        remote_file_name_list = [x.name for x in remote_file_meta_list]
        remote_file_name_list_escaped = escape_windows_file_name(remote_file_name_list)
        remote_dir_name_list_escaped = escape_windows_file_name(remote_dir_name_list)
        finished += 1
        total += len(remote_dir_meta_list) + len(remote_file_meta_list)
        # remote -> local (new directory)
        for new_dir, new_dir_escaped in zip(remote_dir_name_list, remote_dir_name_list_escaped):
            dirs.append((f'{cur_remote_path}/{new_dir}', os.path.join(cur_local_path, new_dir_escaped)))
        _, local_dirs, local_files = next(os.walk(cur_local_path))
        local_files_set = set(local_files)
        # remote -> local (deleted files)
        files_to_remove = local_files_set.difference(remote_file_name_list_escaped)
        # remote -> local (deleted directories)
        dirs_to_remove = set(local_dirs).difference(remote_dir_name_list_escaped)

        for file in files_to_remove:
            os.remove(os.path.join(cur_local_path, file))
        for dir_ in dirs_to_remove:
            remove_dir(os.path.join(cur_local_path, dir_))

        # remote -> local (both file exists)
        for file in remote_file_meta_list:
            file_name_escaped = escape_windows_file_name(file.name)
            full_local_path = os.path.join(cur_local_path, file_name_escaped)

            local_stat = os.stat(full_local_path) if file_name_escaped in local_files_set else None
            # copy conditions: (1) local file not found, (2) file size mismatch, (3) last modification time mismatch
            if local_stat is None or not skip_exist or local_stat.st_size != file.file_size or \
                    abs(local_stat.st_mtime - get_datetime_timestamp(file.last_modification)) > 1:
                print('[%d / %d]' % (finished, total), end=' ')
                adb_pull(f'{cur_remote_path}/{file.name}', full_local_path)
                os.utime(full_local_path, (datetime.now().timestamp() if local_stat is None else local_stat.st_atime,
                                           get_datetime_timestamp(file.last_modification)))
            finished += 1
        for dir_ in remote_dir_name_list_escaped:
            create_dir(os.path.join(cur_local_path, dir_))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', help='input path of cell phone', dest='input_path', required=True)
    parser.add_argument('-o', help='output path to store files', dest='output_path', required=True)
    args = parser.parse_args()
    spawn_process('adb start-server', 'utf8')
    # do_backup('/storage/sdcard1', 'd:/hm_sdcard1_backup')
    do_backup(args.input_path, args.output_path)


if __name__ == '__main__':
    main()
