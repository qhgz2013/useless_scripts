from backup_manager import BackupManager
import argparse


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--action", choices=['sync_local', 'sync_remote', 'map_fs', 'cleanup'],
                        dest='action', required=True,
                        help='choose action, sync_local: sync from local to remote, sync_remote: sync from remote to'
                             ' local, map_fs: map objects to database, cleanup: reduce databases and clean up'
                             ' unreferenced objects')
    parser.add_argument("--thread", help='threads for parallel adb pull/push/stat', type=int, default=8,
                        dest='thread_count')
    parser.add_argument('base_path', help='path where the backup files stores in the fs', type=str)
    parser.add_argument('db_path', help='path in the database system', type=str, nargs='?')
    parser.add_argument('fs_or_remote_path', help='remote path when syncing or fs path when mapping',
                        type=str, nargs='?')
    args = parser.parse_args()
    # print(args)
    manager = BackupManager(args.base_path, args.thread_count)
    if args.action == 'sync_local':
        assert args.fs_or_remote_path is not None, 'Missing required field: fs_or_remote_path'
        assert args.db_path is not None, 'Missing required field: db_path'
        manager.sync_local(args.fs_or_remote_path, args.db_path)
    elif args.action == 'sync_remote':
        assert args.fs_or_remote_path is not None, 'Missing required field: fs_or_remote_path'
        assert args.db_path is not None, 'Missing required field: db_path'
        manager.sync_remote(args.fs_or_remote_path, args.db_path)
    elif args.action == 'map_fs':
        assert args.fs_or_remote_path is not None, 'Missing required field: fs_or_remote_path'
        assert args.db_path is not None, 'Missing required field: db_path'
        manager.map_database_to_fs(args.db_path, args.fs_or_remote_path)
    elif args.action == 'cleanup':
        manager.compress_database()
        manager.cleanup_objects()
    else:
        print("Don't know what to do for action", args.action)
        exit(1)


if __name__ == '__main__':
    main()
