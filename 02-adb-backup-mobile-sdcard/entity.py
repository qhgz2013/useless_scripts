from orm_utils import *


class DirectoryMeta(Entity):
    __FIELDS__ = [TableFieldDescriptor('path_id', 'integer', primary_key=True, auto_increment=True),
                  TableFieldDescriptor('path', 'text', not_null=True, unique=True),
                  TableIndexDescriptor('index_path', 'path')]


class FileMeta(Entity):
    __FIELDS__ = [TableFieldDescriptor('path_id', 'integer', not_null=True),
                  TableFieldDescriptor('file_name', 'text', not_null=True),
                  TableFieldDescriptor('file_size', 'bigint', not_null=True),
                  TableFieldDescriptor('access_time', 'timestamp', not_null=True),
                  TableFieldDescriptor('mod_time', 'timestamp', not_null=True),
                  TableFieldDescriptor('create_time', 'timestamp', not_null=True),
                  TableFieldDescriptor('md5', 'binary(16)'),
                  TableFieldDescriptor('sha256', 'binary(32)'),
                  TableFieldDescriptor('is_dir', 'tinyint'),
                  TableIndexDescriptor('index_md5', 'md5'),
                  TableIndexDescriptor('index_sha256', 'sha256'),
                  TableIndexDescriptor('index_file_name', 'path_id', 'file_name'),
                  MultiPrimaryKeyOrderDescriptor('path_id', 'file_name'),
                  ForeignKeyDescriptor('path_id', 'directory_meta')]
