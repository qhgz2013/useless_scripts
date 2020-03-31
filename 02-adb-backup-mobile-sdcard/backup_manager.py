import os
from sql_accessor import SqliteAccessor
from entity import *
from exceptions import *
from util import spawn_process, get_datetime_timestamp
import re
import datetime
import shutil
import hashlib
# from time import time
import threading
from thread_safe_buffer_queue import ThreadSafeBufferQueue, QueueClosedException
import traceback
from warnings import warn


# This "ls -al" pattern is tested on Android 8.0 (Mi 5s) and Android 4.4.4 (Redmi Note 1 LTE)
# If it is not compatible for your device, modify it by yourself
ls_al_pattern = re.compile(r'^(?P<permission>[dcbl-]([r-][w-][x-]){3}\+?)\s+'
                           r'((?P<links>\d+)\s+)?'
                           r'(?P<owner_name>[a-zA-Z0-9_]+)\s+'
                           r'(?P<owner_group>[a-zA-Z0-9_]+)\s+'
                           r'((?P<file_size>\d+)\s+)?'
                           r'(?P<last_modification>\d+-\d+-\d+\s\d+:\d+)\s'
                           r'(?P<name>.+?)\s*$')


class _StatusStatistics:
    def __init__(self, thread_count: int):
        self.total_files = 0
        self.current_files = 0
        self.total_dirs = 1
        self.current_dirs = 0
        self.adb_sem = threading.Semaphore(thread_count)
        self.lock = threading.RLock()
    __slots__ = ['total_files', 'current_files', 'total_dirs', 'current_dirs', 'lock', 'adb_sem']


# noinspection PyUnresolvedReferences
class BackupManager:
    _ST_FILE = 1
    _ST_DIR = 0
    _ST_NOT_FOUND = -1

    def __init__(self, path: str, thread_count: int = 4, max_history_backup: int = 30):
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        assert os.path.isdir(path), 'path must be a directory'
        assert thread_count > 0, 'thread_count must be positive'
        assert max_history_backup > 0, 'max_history_backup must be positive'
        self._path = path
        self._sql_file = os.path.join(self._path, 'entries.db')
        if not os.path.isfile(self._sql_file):
            open(self._sql_file, 'wb').close()
        self._sql_conn = SqliteAccessor(self._sql_file)
        # creating repository directory
        for i in range(256):
            os.makedirs(os.path.join(self._path, 'objects', '%02x' % i), exist_ok=True)
        spawn_process('adb start-server', 'utf8')
        self._thread_count = thread_count
        self._max_history_backup = max_history_backup

    def _list_backup_db_file(self):
        candidate_db_files = []
        pattern = re.compile(r'entries\.db\.(\d+)\.(\d+)\.(\d+)\.bak')
        for file in os.listdir(self._path):
            match = re.search(pattern, file)
            if match is not None:
                date = datetime.datetime(int(match.group(1)), int(match.group(2)), int(match.group(3)))
                candidate_db_files.append((file, date))
        candidate_db_files.sort(key=lambda x: x[1], reverse=True)
        return [os.path.join(self._path, x[0]) for x in candidate_db_files]
    
    def _backup_db(self):
        print('Backing up database file.')
        today = datetime.datetime.now()
        today_backup_file_name = 'entries.db.%d.%d.%d.bak' % (today.year, today.month, today.day)
        today_backup_file = os.path.join(self._path, today_backup_file_name)
        with open(today_backup_file, 'wb') as f_out:
            with open(self._sql_file, 'rb') as f_in:
                while True:
                    buffer = f_in.read(4096)
                    f_out.write(buffer)
                    if len(buffer) == 0:
                        break
        candidate_db_files = self._list_backup_db_file()
        for file in candidate_db_files[self._max_history_backup:]:
            os.remove(os.path.join(self._path, file))
        print('Done.')

    def list_database(self, path: str) -> List[FileMeta]:
        path = self._abs_path(path)
        dir_info = self._sql_conn.select(DirectoryMeta, 1, path=path)
        if dir_info is None:
            raise PathNotFoundException()
        else:
            return self._sql_conn.select(FileMeta, 0, path_id=dir_info.path_id)

    def _stat_path(self, path: str) -> Tuple[int, int]:
        dir_meta = self._sql_conn.select(DirectoryMeta, 1, path=path)
        if dir_meta is None:
            # not a dir, querying parent path
            parent_dir = self._sql_conn.select(DirectoryMeta, 1, path=self._abs_path(path + '/..'))
            if parent_dir is None:
                return self._ST_NOT_FOUND, 0
            elif self._sql_conn.select(FileMeta, 1, path_id=parent_dir.path_id,
                                       file_name=self._file_name(path)) is None:
                return self._ST_NOT_FOUND, 0
            else:
                return self._ST_FILE, parent_dir.path_id
        else:
            return self._ST_DIR, dir_meta.path_id

    @staticmethod
    def _abs_path(path: str) -> str:
        path = path.replace('\\', '/')
        if not path.startswith('/'):
            raise InvalidPathException()
        segments = path.split('/')[1:]
        abs_path_segments = []
        for seg in segments:
            if seg == '.' or len(seg) == 0:
                continue
            elif seg == '..':
                if len(abs_path_segments) == 0:
                    raise InvalidPathException()
                abs_path_segments.pop(len(abs_path_segments) - 1)
            else:
                abs_path_segments.append(seg)
        return '/' + '/'.join(abs_path_segments)

    @staticmethod
    def _file_name(path: str) -> str:
        return path.split('/')[-1]

    @staticmethod
    def _split_ext(path: str) -> Tuple[str, str]:
        part = path.split('.')
        return '.'.join(part[:-1]), part[-1]

    @staticmethod
    def _adb_ls(path: str, retry_count: int = 5) -> Tuple[List[str], List[str]]:
        if retry_count == 0:
            raise RuntimeError('Adb repeatedly returned empty ls result for path %s' % path)
        if not path.endswith('/'):
            path = path + '/'
        # escape char (') in linux shell
        path = path.replace("'", "'\"'\"'")
        stdout, stderr = spawn_process(['adb', 'shell', 'ls', '-al', "'%s'" % path], 'utf8')
        if len(stderr) > 0:
            raise RuntimeError(stderr)
        if len(stdout) == 0:
            return BackupManager._adb_ls(path, retry_count - 1)
        dirs = []
        files = []
        for line in stdout.split('\n'):
            if len(line) == 0:
                continue
            match = re.match(ls_al_pattern, line)
            if match is None:
                continue
            # '\ ' will be used in newest android OS
            filename = match.group('name').replace('\\', '')
            permission = match.group('permission')
            if filename == '.' or filename == '..':
                continue
            if permission[0] == 'd':
                dirs.append(filename)
            elif permission[0] == '-':
                files.append(filename)
            else:
                print('Unsupported file permission attribute:', permission)
        return dirs, files

    def _adb_stat(self, path: str, retry_count: int = 5) -> FileMeta:
        if retry_count == 0:
            raise RuntimeError('Adb repeatedly returned empty stat result for path %s' % path)
        # escape char (') in linux shell
        path_escaped = path.replace("'", "'\"'\"'")
        stdout, stderr = spawn_process(['adb', 'shell', 'stat', '-L', '-c', "'%A/%s/%X/%Y/%W/%n'",
                                        "'%s'" % path_escaped], 'utf8')
        if len(stderr) > 0:
            raise RuntimeError(stderr)
        if len(stdout) == 0:  # unknown reason for stat returns nothing
            return self._adb_stat(path, retry_count - 1)
        parts = stdout.rstrip('\r\n').split('/')

        def _cvt_ts(x):
            return 0 if x == '?' else int(x)
        # debug
        try:
            return FileMeta(path_id=0, file_name=parts[-1], file_size=int(parts[1]),
                            access_time=datetime.datetime.fromtimestamp(_cvt_ts(parts[2])),
                            mod_time=datetime.datetime.fromtimestamp(_cvt_ts(parts[3])),
                            create_time=datetime.datetime.fromtimestamp(_cvt_ts(parts[4])),
                            is_dir=int(parts[0][0] == 'd'))
        except IndexError:
            warn('Invalid scheme: "%s" for path "%s"' % (stdout, path))
            raise

    def _pull_file(self, path: str, meta: FileMeta):
        local_path = os.path.join(self._path, 'tmp_adb_pull_file_%d' % threading.get_ident())
        try:
            open(local_path, 'wb').close()
            stdout, stderr = spawn_process(['adb', 'pull', path, local_path], 'utf8')
            if len(stderr) > 0:
                raise RuntimeError(stderr)
            if stdout.startswith("adb: error:"):
                raise RuntimeError(stdout)
            with open(local_path, 'rb') as f:
                md5_hash = hashlib.md5()
                sha256_hash = hashlib.sha256()
                while True:
                    b = f.read(4096)
                    if len(b) == 0:
                        break
                    md5_hash.update(b)
                    sha256_hash.update(b)
                meta.md5 = md5_hash.digest()
                meta.sha256 = sha256_hash.digest()
            dest_path = os.path.join(self._path, 'objects', '%02x' % meta.sha256[0], meta.sha256.hex())
            if not os.path.exists(dest_path):
                shutil.move(local_path, dest_path)
            else:
                st_dest = os.stat(dest_path)
                st_src = os.stat(local_path)
                if st_dest.st_size == st_src.st_size:
                    os.remove(local_path)
                else:
                    raise RuntimeError('Hash conflict for object %s (path: %s)' % (meta.sha256.hex(), path))
            db_meta = self._sql_conn.select(FileMeta, 1, path_id=meta.path_id, file_name=meta.file_name)
            if db_meta is None:
                self._sql_conn.insert(meta)
            elif meta != db_meta:
                self._sql_conn.update(meta)
        except FileNotFoundError:
            warn('Could not pull file: %s' % path)

    def _reuse_index(self, path_id: int):
        if path_id >= 0x40000000:
            cursor = self._sql_conn.cursor()
            cursor.execute("select path_id from directory_meta order by path_id")
            path_ids = [x[0] for x in cursor.fetchall()]
            i = 0
            while i < len(path_ids):
                i += 1
                if i == path_ids[i - 1]:
                    continue
                last_path_id = path_ids.pop()
                cursor.execute("insert into directory_meta(path_id, path) values (?, ?)", (i, 'tmp_path_id_non_used'))
                cursor.execute("update file_meta set path_id = ? where path_id = ?", (i, last_path_id))
                cursor.execute("select path from directory_meta where path_id = ?", (last_path_id,))
                last_path = cursor.fetchone()[0]
                cursor.execute("delete from directory_meta where path_id = ?", (last_path_id,))
                cursor.execute("update directory_meta set path = ? where path_id = ?", (last_path, i))
                path_ids.insert(i - 1, i)
            cursor.execute("update sqlite_sequence set seq = ? where name = 'directory_meta'", (i,))
            cursor.close()
            self._sql_conn.commit()

    def _create_db_path(self, path: str, exist_ok: bool = False) -> int:
        query_path = self._sql_conn.select(DirectoryMeta, 1, path=path)
        if query_path is None:
            if path != '/':
                parent_id = self._create_db_path(self._abs_path(path + '/..'))
                entity = DirectoryMeta(path=path)
                self._sql_conn.insert(entity)
                self._reuse_index(entity.path_id)
                file_name = self._file_name(path)
                db_entry = self._sql_conn.select(FileMeta, 1, path_id=parent_id, file_name=file_name)
                now = datetime.datetime.now()
                # assert directory
                if db_entry is not None and db_entry.is_dir == 0:
                    raise NotADirectoryError()
                # raise if existed and exist_ok == False
                if not exist_ok and db_entry is not None:
                    raise DirectoryExistedException()
                if db_entry is None:
                    self._sql_conn.insert(FileMeta(path_id=parent_id, file_name=file_name, file_size=0, access_time=now,
                                                   mod_time=now, create_time=now, is_dir=1))
                return entity.path_id
            else:
                entity = DirectoryMeta(path=path)
                self._sql_conn.insert(entity)
                self._reuse_index(entity.path_id)
                return entity.path_id
        else:
            return query_path.path_id

    def _sync_remote_parallel_file_callback(self, file_queue: ThreadSafeBufferQueue, stat: _StatusStatistics):
        while True:
            try:
                path_id, remote_path, db_path, call_fn = file_queue.dequeue()
                try:
                    with stat.adb_sem:
                        meta = self._adb_stat(remote_path)
                    meta.path_id = path_id
                    call_fn(remote_path, db_path, meta)
                except Exception as ex1:
                    warn('exception while syncing remote metadata: %s' % ex1)
                    # traceback.print_exc()
                    call_fn(remote_path, db_path, None)
            except QueueClosedException:
                return
            except Exception as ex:
                warn('Unexpected exception in slave thread: %s' % str(ex))

    def _sync_remote_parallel_dir_callback(self, dir_queue: ThreadSafeBufferQueue, file_queue: ThreadSafeBufferQueue,
                                           stat: _StatusStatistics):
        while True:
            try:
                cur_remote_path, cur_db_path = dir_queue.dequeue()
                with stat.lock:
                    stat.current_dirs += 1
                    print('\r[%d/%d] %s' % (stat.current_files + stat.current_dirs,
                                            stat.total_dirs + stat.total_files, cur_remote_path), end=' ', flush=True)
                try:
                    cur_db_path_id = self._sql_conn.select(DirectoryMeta, 1, path=cur_db_path).path_id
                except AttributeError:
                    warn('Failed to get database directory info: %s' % cur_db_path)
                    continue
                try:
                    with stat.adb_sem:
                        remote_dirs, remote_files = self._adb_ls(cur_remote_path)
                except Exception as ex:
                    print('exception while listing path %s: %s' % (cur_remote_path, str(ex)))
                    traceback.print_exc()
                    continue
                with stat.lock:
                    stat.total_dirs += len(remote_dirs)
                    stat.total_files += len(remote_files)
                db_metas = self.list_database(cur_db_path)
                local_dirs = set([x.file_name for x in db_metas if x.is_dir != 0])
                local_files = set([x.file_name for x in db_metas if x.is_dir == 0])
                if cur_db_path == '/':
                    cur_db_path = ''
                if cur_remote_path == '/':
                    cur_remote_path = ''
                # remote -> local (new directory)
                new_db_dirs = set(remote_dirs).difference(local_dirs)
                for dir_name in new_db_dirs:
                    self._create_db_path(cur_db_path + '/' + dir_name)

                # remote -> local (deleted directory)
                deleted_db_dirs = set(local_dirs).difference(remote_dirs)
                for dir_name in deleted_db_dirs:
                    self._remove_db(cur_db_path + '/' + dir_name)

                # remote -> local (directory, sync meta)
                def _sync_dir_meta(remote_path, db_path, meta):
                    if meta is None:
                        # failed to stat directory info, skipped
                        with stat.lock:
                            stat.current_dirs += 1
                            print('\r[%d/%d] %s' % (stat.current_files + stat.current_dirs,
                                                    stat.total_dirs + stat.total_files, cur_remote_path),
                                  end=' ', flush=True)
                        warn('Failed to fetch directory meta: %s, skipped' % remote_path, RuntimeWarning)
                    else:
                        db_meta = self._sql_conn.select(FileMeta, 1, path_id=meta.path_id, file_name=meta.file_name)
                        if db_meta is None or meta != db_meta:
                            # changed: not updating db if nothing changed
                            self._sql_conn.update(meta)
                        dir_queue.enqueue((remote_path, db_path))
                for dirs in remote_dirs:
                    file_queue.enqueue((cur_db_path_id, cur_remote_path + '/' + dirs, cur_db_path + '/' + dirs,
                                        _sync_dir_meta))

                # remote -> local (new file)
                def _fetch_new_file(path, _, meta):
                    with stat.lock:
                        stat.current_files += 1
                        print('\r[%d/%d] %s' % (stat.current_files + stat.current_dirs,
                                                stat.total_dirs + stat.total_files, path), end=' ', flush=True)
                    if meta is not None:
                        with stat.adb_sem:
                            self._pull_file(path, meta)
                new_db_files = set(remote_files).difference(local_files)
                for file in new_db_files:
                    file_queue.enqueue((cur_db_path_id, cur_remote_path + '/' + file, cur_db_path + '/' + file,
                                        _fetch_new_file))

                # remote -> local (delete file)
                deleted_db_files = set(local_files).difference(remote_files)
                for file_name in deleted_db_files:
                    self._sql_conn.delete(FileMeta, path_id=cur_db_path_id, file_name=file_name)

                # remote -> local (existed file)
                def _fetch_exist_file(path, _, meta):
                    with stat.lock:
                        stat.current_files += 1
                        print('\r[%d/%d] %s' % (stat.current_files + stat.current_dirs,
                                                stat.total_dirs + stat.total_files, path), end=' ', flush=True)
                    if meta is not None:
                        db_meta = self._sql_conn.select(FileMeta, 1, path_id=meta.path_id, file_name=meta.file_name)
                        if db_meta is None:
                            warn('Failed to get database file meta: path_id: %d, file_name: %s'
                                 % (cur_db_path_id, meta.file_name))
                            return
                        if abs(get_datetime_timestamp(db_meta.mod_time) - get_datetime_timestamp(meta.mod_time)) > 1 \
                                or db_meta.file_size != meta.file_size:
                            with stat.adb_sem:
                                self._pull_file(path, meta)

                existed_files = set(local_files).intersection(remote_files)
                for file in existed_files:
                    file_queue.enqueue((cur_db_path_id, cur_remote_path + '/' + file, cur_db_path + '/' + file,
                                        _fetch_exist_file))
            except QueueClosedException:
                return
            except Exception as ex:
                warn('Unexpected exception in slave thread: %s' % str(ex))
            finally:
                with stat.lock:
                    if not dir_queue.is_closed and stat.current_dirs == stat.total_dirs:
                        dir_queue.close()
                        file_queue.close()

    def sync_remote(self, remote_path: str, db_path: str = '/'):
        self._backup_db()
        remote_path = self._abs_path(remote_path)
        db_path = self._abs_path(db_path)
        st_remote = self._adb_stat(remote_path)
        if not st_remote.is_dir:
            # handling single file
            st_local, local_path_id = self._stat_path(db_path)
            if st_local == self._ST_NOT_FOUND:
                local_path_id = self._create_db_path(db_path, exist_ok=True)
            st_remote.path_id = local_path_id
            self._pull_file(remote_path, st_remote)
            return
        self._create_db_path(db_path, exist_ok=True)
        dir_queue = ThreadSafeBufferQueue()
        dir_queue.enqueue((remote_path, db_path))
        file_queue = ThreadSafeBufferQueue(16384)
        stat = _StatusStatistics(self._thread_count)
        try:
            thds = []
            for _ in range(self._thread_count):
                thd = threading.Thread(target=self._sync_remote_parallel_dir_callback,
                                       args=(dir_queue, file_queue, stat), daemon=True)
                thds.append(thd)
                thd.start()
                thd = threading.Thread(target=self._sync_remote_parallel_file_callback,
                                       args=(file_queue, stat), daemon=True)
                thds.append(thd)
                thd.start()
            for thd in thds:
                while thd.is_alive():
                    thd.join(300)
                    print('Auto committing database.')
                    self._sql_conn.commit()
            self._validate_objects()
            # debug
            with stat.lock:
                print('Directories: %d/%d' % (stat.current_dirs, stat.total_dirs))
                print('Files: %d/%d' % (stat.current_files, stat.total_files))
        finally:
            self._sql_conn.commit()

    def _push_file(self, path: str, meta: FileMeta):
        local_path = os.path.join(self._path, 'objects', '%02x' % meta.sha256[0], meta.sha256.hex())
        if os.path.exists(local_path):
            os.utime(local_path, (get_datetime_timestamp(meta.access_time), get_datetime_timestamp(meta.mod_time)))
            stdout, stderr = spawn_process(['adb', 'push', local_path, path], 'utf8')
            if len(stderr) > 0:
                raise RuntimeError(stderr)
        else:
            warn("Could not push file %s: object %s not found" % (path, local_path))

    def _create_remote_dir(self, path: str):
        args = ['adb', 'shell', 'mkdir', "'%s'" % path.replace("'", "'\"'\"'")]
        stdout, stderr = spawn_process(args, 'utf8')
        if stderr.rstrip('\r\n').endswith('No such file or directory'):
            # recursive mode
            if path == '/':
                raise RuntimeError(stderr)
            self._create_remote_dir(self._abs_path(path + '/..'))
            # retry after parent dir created
            stdout, stderr = spawn_process(args, 'utf8')
            if len(stderr) > 0:
                raise RuntimeError(stderr)

    @staticmethod
    def _remove_remote(path: str):
        stdout, stderr = spawn_process(['adb', 'shell', 'rm', '-rf', "'%s'" % path.replace("'", "'\"'\"'")], 'utf8')
        if len(stderr) > 0:
            raise RuntimeError(stderr)

    def sync_local(self, remote_path: str, db_path: str = '/'):
        remote_path = self._abs_path(remote_path)
        db_path = self._abs_path(db_path)
        # create remote if not exists
        st_local, local_path_id = self._stat_path(db_path)
        if st_local == self._ST_FILE:
            raise NotImplementedError
        elif st_local == self._ST_NOT_FOUND:
            raise FileNotFoundError
        try:
            st_remote = self._adb_stat(remote_path)
        except RuntimeError:
            self._create_remote_dir(remote_path)
            st_remote = self._adb_stat(remote_path)
        dirs = [(remote_path, db_path)]
        total = 1
        finished = 0
        while len(dirs) > 0:
            cur_remote_path, cur_db_path = dirs.pop(0)
            try:
                remote_dirs, remote_files = self._adb_ls(cur_remote_path)
            except Exception as ex:
                print('exception:', ex)
                continue
            db_metas = self.list_database(cur_db_path)
            total += len(db_metas)
            finished += 1
            print('[%d/%d] %s' % (finished, total, cur_remote_path))

            db_files = dict([(x.file_name, x) for x in db_metas if not x.is_dir])
            db_dirs = [x.file_name for x in db_metas if x.is_dir]

            if cur_remote_path == '/':
                cur_remote_path = ''
            # local -> remote (new directory)
            new_dirs = set(db_dirs).difference(remote_dirs)
            for name in new_dirs:
                self._create_remote_dir(cur_remote_path + '/' + name)
            # local -> remote (delete directory)
            deleted_dirs = set(remote_dirs).difference(db_dirs)
            for name in deleted_dirs:
                self._remove_remote(cur_remote_path + '/' + name)
            # local -> remote (new files)
            new_files = set(db_files.keys()).difference(remote_files)
            for name in new_files:
                finished += 1
                print('[%d/%d] %s' % (finished, total, cur_remote_path + '/' + name))
                self._push_file(cur_remote_path + '/' + name, db_files[name])
            # local -> remote (deleted files)
            deleted_files = set(remote_files).difference(db_files.keys())
            for name in deleted_files:
                self._remove_remote(cur_remote_path + '/' + name)
            # local -> remote (existed files)
            existed_files = set(remote_files).intersection(db_files.keys())
            for name in existed_files:
                finished += 1
                print('[%d/%d] %s' % (finished, total, cur_remote_path + '/' + name))
                st_remote = self._adb_stat(cur_remote_path + '/' + name)
                st_local = db_files[name]
                if abs(get_datetime_timestamp(st_remote.mod_time) - get_datetime_timestamp(st_local.mod_time)) > 1 or\
                        st_remote.file_size != st_local.file_size:
                    self._push_file(cur_remote_path + '/' + name, st_local)
            # BFS-recursion
            if cur_db_path == '/':
                cur_db_path = ''
            for name in db_dirs:
                dirs.append((cur_remote_path + '/' + name, cur_db_path + '/' + name))

    def _validate_objects(self, remove_unused: bool = False):
        print('Checking database objects.')
        backup_db_files = self._list_backup_db_file()
        print('[1/%d] %s' % (len(backup_db_files) + 1, self._sql_file))
        cursor = self._sql_conn.cursor()
        cursor.execute("select sha256 from file_meta where is_dir == 0")
        db_sha256 = set([x[0] for x in cursor.fetchall()])
        cursor.close()
        for i, db_file in enumerate(backup_db_files):
            print('[%d/%d] %s' % (i + 2, len(backup_db_files) + 1, db_file))
            conn = SqliteAccessor(db_file)
            cursor = conn.cursor()
            cursor.execute("select sha256 from file_meta where is_dir == 0")
            db_sha256.update([x[0] for x in cursor.fetchall()])
            cursor.close()
            conn.close()
        if None in db_sha256:
            warn('Detected missing sha256 hash in database, re-run sync to solve this problem')
            db_sha256.remove(None)
        print('Checking file system objects.')
        fs_sha256 = set()
        for _, _, files in os.walk(os.path.join(self._path, 'objects')):
            for file in files:
                fs_sha256.add(bytes.fromhex(file))
        print('Done.')
        missing_sha256 = db_sha256.difference(fs_sha256)
        non_reference_sha256 = fs_sha256.difference(db_sha256)
        if len(missing_sha256) > 0:
            warn("Detected missing %d objects from file system, re-run sync to solve this problem" %
                 len(missing_sha256))
        if len(non_reference_sha256) > 0:
            if remove_unused:
                for sha256 in non_reference_sha256:
                    path = os.path.join(self._path, 'objects', '%02x' % sha256[0], sha256.hex())
                    os.remove(path)
                print('Removed %d unused objects' % len(non_reference_sha256))
            else:
                print('Detected %d objects are unreferenced' % len(non_reference_sha256))

    def _remove_db(self, db_path: str):
        db_path = self._abs_path(db_path)
        db_stat, path_id = self._stat_path(db_path)
        if db_stat == self._ST_FILE:
            self._sql_conn.delete(FileMeta, path_id=path_id, file_name=self._file_name(db_path))
        elif db_stat == self._ST_DIR:
            if db_path == '/':
                db_path = ''
            to_delete_dir_list = [(db_path, path_id)]
            while len(to_delete_dir_list) > 0:
                path, path_id = to_delete_dir_list.pop(0)
                dir_names = [x.file_name for x in self._sql_conn.select(FileMeta, 0, path_id=path_id) if x.is_dir]
                for dir_name in dir_names:
                    dir_id = self._sql_conn.select(DirectoryMeta, 1, path=path + '/' + dir_name).path_id
                    to_delete_dir_list.append((path + '/' + dir_name, dir_id))
                self._sql_conn.delete(FileMeta, path_id=path_id)
                self._sql_conn.delete(DirectoryMeta, path_id=path_id)
            # delete entry
            if db_path != '':
                parent_path_id = self._sql_conn.select(DirectoryMeta, 1, path=self._abs_path(db_path + '/..')).path_id
                self._sql_conn.delete(FileMeta, path_id=parent_path_id, file_name=self._file_name(db_path))

    def remove_database(self, db_path: str):
        self._remove_db(db_path)
        self._sql_conn.commit()
        self._validate_objects()

    def _extract_object(self, meta: FileMeta, dst: str):
        src = os.path.join(self._path, 'objects', '%02x' % meta.sha256[0], meta.sha256.hex())
        print('Extracting %s' % dst)
        shutil.copy(src, dst)
        os.utime(dst, (int(get_datetime_timestamp(meta.access_time)), int(get_datetime_timestamp(meta.mod_time))))

    def _map_dir(self, db_path: str, fs_path: str):
        os.makedirs(fs_path, exist_ok=True)
        path_id = self._sql_conn.select(DirectoryMeta, 1, path=db_path).path_id
        if db_path == '/':
            db_path = ''
        files = self._sql_conn.select(FileMeta, 0, path_id=path_id)
        for file in files:
            fs_file_name = self._escape_windows_file_name(file.file_name)
            if file.is_dir:
                self._map_dir(db_path + '/' + file.file_name, os.path.join(fs_path, fs_file_name))
            else:
                self._extract_object(file, os.path.join(fs_path, fs_file_name))

    @staticmethod
    def _escape_windows_file_name(s: str):
        s = s.rstrip('.')
        for ch in r'?<>|\/:*"':
            s = s.replace(ch, '_')
        return s

    def map_database_to_fs(self, db_path: str, fs_path: str):
        db_path = self._abs_path(db_path)
        path_type, path_id = self._stat_path(db_path)
        if path_type == self._ST_FILE:
            file_name = self._file_name(db_path)
            if os.path.isdir(fs_path):
                # if fs path is a directory, create a new file to directory
                fs_path = os.path.join(fs_path, self._escape_windows_file_name(file_name))
            meta = self._sql_conn.select(FileMeta, 1, path_id=path_id, file_name=file_name)
            self._extract_object(meta, fs_path)
        elif path_type == self._ST_DIR:
            if os.path.isfile(fs_path):
                raise NotADirectoryError(fs_path)
            self._map_dir(db_path, fs_path)
    
    def compress_database(self):
        cursor = self._sql_conn.cursor()
        cursor.execute("vacuum")
        cursor.close()
        self._sql_conn.commit()
    
    # def restore_database(self):
    #     if os.path.isfile(self._sql_file + '.bak'):
    #         with open(self._sql_file + '.bak', 'rb') as f_in:
    #             with open(self._sql_file, 'wb') as f_out:
    #                 while True:
    #                     buffer = f_in.read(4096)
    #                     f_out.write(buffer)
    #                     if len(buffer) == 0:
    #                         break
    #     else:
    #         warn('Could not find backup database file, operation aborted')

    def cleanup_objects(self):
        self._validate_objects(remove_unused=True)
