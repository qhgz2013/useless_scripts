# Version 1.2
# CHANGELOG
# Ver 1.2 Added mysql support
# Ver 1.1 Bug fixed and type hint changed for abstract sql statement generator
import orm_utils
from typing import *
from warnings import warn


class AbstractSqlStatementGenerator:
    _dialect_impl = {}

    def __init_subclass__(cls, **kwargs):
        if 'dialect' in kwargs:
            if type(kwargs['dialect']) == str:
                cls._dialect_impl[kwargs['dialect']] = cls
            else:
                warn('Invalid dialect string: got type %s' % type(kwargs['dialect']).__name__)
        else:
            warn('No dialect is set for subclass %s' % cls.__name__)

    @staticmethod
    def create_table(entity: Type[orm_utils.Entity], cursor: Any):
        raise NotImplementedError()

    @staticmethod
    def insert(entity: orm_utils.Entity, cursor: Any):
        raise NotImplementedError()

    @staticmethod
    def update(entity: orm_utils.Entity, cursor: Any):
        raise NotImplementedError()

    @staticmethod
    def select(entity: Type[orm_utils.Entity], cursor: Any, fetch_count: int = 1, **keys: Any):
        raise NotImplementedError()

    @staticmethod
    def delete(entity: Type[orm_utils.Entity], cursor: Any, **keys: Any):
        raise NotImplementedError()

    @classmethod
    def get_dialect_generator(cls, dialect: str) -> 'AbstractSqlStatementGenerator':
        return cls._dialect_impl[dialect]


# common functions for all sql dialects

def _validate_select_query_fields(entity: Type[orm_utils.Entity], args: Iterable[str]) -> Tuple[Set[str], Set[str]]:
    # returns the name of table fields of specified entity type, and unexpected fields passed to args
    basic_fields = [x for x in entity.__FIELDS__ if type(x) == orm_utils.TableFieldDescriptor]
    field_names = set([x.field_name for x in basic_fields])
    unexpected_fields = set(args).difference(field_names)
    return field_names, unexpected_fields


def _fetch_result(entity: Type[orm_utils.Entity], field_names: Iterable[str], cursor: Any, fetch_count: int) \
        -> Optional[Union[orm_utils.Entity, List[orm_utils.Entity]]]:
    # fetch result from sql cursor (must support fetchone(), fetchmany() and fetchall(), and returns the entity(s)
    if fetch_count == 1:
        fetch_result = cursor.fetchone()
        if fetch_result is None:
            return None
        entity_obj = object.__new__(entity)
        entity_obj.__init__(**dict([x for x in zip(field_names, fetch_result)]))
    else:
        if fetch_count > 1:
            fetch_results = cursor.fetchmany(fetch_count)
        else:
            fetch_results = cursor.fetchall()
        entity_obj_list = []
        for fetch_result in fetch_results:
            entity_obj = object.__new__(entity)
            entity_obj.__init__(**dict([x for x in zip(field_names, fetch_result)]))
            entity_obj_list.append(entity_obj)
        entity_obj = entity_obj_list
    return entity_obj


def _extract_primary_fields(entity: Union[Type[orm_utils.Entity], orm_utils.Entity],
                            field_descriptors: Iterable[Any]) -> Set[str]:
    # extract the name of primary key fields
    primary_key_field_names = set([x.field_name for x in field_descriptors if x.primary_key])
    if len(primary_key_field_names) == 0:
        primary_key_field = [x for x in entity.__FIELDS__ if type(x) == orm_utils.MultiPrimaryKeyOrderDescriptor]
        if len(primary_key_field) > 0:
            assert len(primary_key_field) == 1, 'Invalid primary key count, got %d' % len(primary_key_field)
            primary_key_field_names.update(primary_key_field[0].primary_key_orders)
    else:
        assert len(primary_key_field_names) == 1, 'Invalid primary key count, got %d' % len(primary_key_field_names)
    return primary_key_field_names


class SqliteSqlStatementGenerator(AbstractSqlStatementGenerator, dialect='sqlite'):
    @staticmethod
    def create_table(entity: Type[orm_utils.Entity], cursor: Any):
        def _handle_basic_table_field(f: orm_utils.TableFieldDescriptor):
            attrs = [f.field_name, f.field_type]
            if f.not_null:
                attrs.append('not null')
            if f.unique:
                attrs.append('unique')
            if f.primary_key:
                attrs.append('primary key')
            if f.auto_increment:
                attrs.append('autoincrement')
            if f.default:
                attrs.append('default')
                attrs.append(f.default)
            return ' '.join(attrs)

        _field_dict = {
            orm_utils.TableFieldDescriptor: _handle_basic_table_field,
            orm_utils.TableIndexDescriptor: lambda f: 'create index %s on %s (%s)' %
                                                      (f.index_name, entity.__TABLE_NAME__, ', '.join(f.index_fields)),
            orm_utils.MultiPrimaryKeyOrderDescriptor: lambda f: 'primary key (%s)' % ', '.join(f.primary_key_orders),
            orm_utils.ForeignKeyDescriptor: lambda f: 'foreign key (%s) references %s%s' %
                                                      (f.field_name, f.ref_table_name, '' if f.ref_table_field_name is
                                                       None else '(%s)' % f.ref_table_field_name)
        }

        def _handle_field(field):
            return _field_dict[type(field)](field)

        field_segments = [_handle_field(x) for x in entity.__FIELDS__ if type(x) != orm_utils.TableIndexDescriptor]
        cursor.execute('create table %s (%s)' % (entity.__TABLE_NAME__, ', '.join(field_segments)))
        # Extra indices
        for index_field in entity.__FIELDS__:
            if type(index_field) == orm_utils.TableIndexDescriptor:
                cursor.execute(_handle_field(index_field))

    @staticmethod
    def insert(entity: orm_utils.Entity, cursor: Any):
        basic_fields = [x for x in entity.__FIELDS__ if type(x) == orm_utils.TableFieldDescriptor]
        field_names = [x.field_name for x in basic_fields]
        sql = 'insert into %s(%s) values (%s)' % (entity.__TABLE_NAME__, ', '.join(field_names),
                                                  ', '.join(['?'] * len(basic_fields)))
        args = [getattr(entity, x) for x in field_names]
        cursor.execute(sql, args)
        auto_increment_fields = [x for x in basic_fields if x.auto_increment]
        if len(auto_increment_fields) > 0:
            assert len(auto_increment_fields) == 1, 'More than 1 auto increment fields are unsupported'
            if getattr(entity, auto_increment_fields[0].field_name) is None:
                # retrieve the inserted id
                cursor.execute("select last_insert_rowid()")
                auto_increment_id = cursor.fetchone()[0]
                setattr(entity, auto_increment_fields[0].field_name, auto_increment_id)

    @staticmethod
    def update(entity: orm_utils.Entity, cursor: Any):
        basic_fields = [x for x in entity.__FIELDS__ if type(x) == orm_utils.TableFieldDescriptor]
        field_names = set([x.field_name for x in basic_fields])
        primary_key_field_names = _extract_primary_fields(entity, basic_fields)
        updated_fields = field_names.difference(primary_key_field_names)
        sql = "update %s set %s where %s" % (entity.__TABLE_NAME__, ', '.join([x + ' = ?' for x in updated_fields]),
                                             ' and '.join([x + ' = ?' for x in primary_key_field_names]))
        args = [getattr(entity, x) for x in updated_fields]
        args.extend([getattr(entity, x) for x in primary_key_field_names])
        cursor.execute(sql, args)

    @staticmethod
    def select(entity: Type[orm_utils.Entity], cursor: Any, fetch_count: int = 1, **keys: Any):
        field_names, unexpected_fields = _validate_select_query_fields(entity, keys.keys())
        if len(unexpected_fields):
            raise ValueError('Unexpected fields: %s' % ', '.join(unexpected_fields))
        sql = 'select %s from %s' % (', '.join(field_names), entity.__TABLE_NAME__)
        args = ()
        if len(keys) > 0:
            sql += ' where %s' % ' and '.join([x + ' = ?' for x in keys])
            args = [keys[x] for x in keys]
        cursor.execute(sql, args)
        return _fetch_result(entity, field_names, cursor, fetch_count)

    @staticmethod
    def delete(entity: Type[orm_utils.Entity], cursor: Any, **keys: Any):
        # noinspection SqlWithoutWhere
        sql = 'delete from %s' % entity.__TABLE_NAME__
        args = ()
        if len(keys) > 0:
            sql += ' where %s' % ' and '.join([x + ' = ?' for x in keys])
            args = [keys[x] for x in keys]
        cursor.execute(sql, args)


# noinspection SqlResolve
class MysqlSqlStatementGenerator(AbstractSqlStatementGenerator, dialect='mysql'):
    @staticmethod
    def create_table(entity: Type[orm_utils.Entity], cursor: Any):
        def _handle_basic_table_field(f: orm_utils.TableFieldDescriptor):
            attrs = ['`'+f.field_name+'`', f.field_type]
            if f.not_null:
                attrs.append('not null')
            if f.unique:
                attrs.append('unique')
            if f.primary_key:
                attrs.append('primary key')
            if f.auto_increment:
                attrs.append('auto_increment')
            if f.default:
                attrs.append('default')
                attrs.append(f.default)
            return ' '.join(attrs)

        _field_dict = {
            orm_utils.TableFieldDescriptor: _handle_basic_table_field,
            orm_utils.TableIndexDescriptor: lambda f: 'create index %s on `%s` (`%s`)' %
                                                      (f.index_name, entity.__TABLE_NAME__, ', '.join(f.index_fields)),
            orm_utils.MultiPrimaryKeyOrderDescriptor: lambda f: 'primary key (`%s`)' % ', '.join(f.primary_key_orders),
            orm_utils.ForeignKeyDescriptor: lambda f: 'foreign key (`%s`) references `%s`%s' %
                                                      (f.field_name, f.ref_table_name, '' if f.ref_table_field_name is
                                                       None else '(`%s`)' % f.ref_table_field_name)
        }

        def _handle_field(field):
            return _field_dict[type(field)](field)

        field_segments = [_handle_field(x) for x in entity.__FIELDS__ if type(x) != orm_utils.TableIndexDescriptor]
        cursor.execute('create table `%s` (%s)' % (entity.__TABLE_NAME__, ', '.join(field_segments)))
        # Extra indices
        for index_field in entity.__FIELDS__:
            if type(index_field) == orm_utils.TableIndexDescriptor:
                cursor.execute(_handle_field(index_field))

    @staticmethod
    def insert(entity: orm_utils.Entity, cursor: Any):
        basic_fields = [x for x in entity.__FIELDS__ if type(x) == orm_utils.TableFieldDescriptor]
        field_names = [x.field_name for x in basic_fields]
        sql = 'insert into `%s`(`%s`) values (%s)' % (entity.__TABLE_NAME__, '`, `'.join(field_names),
                                                      ', '.join(['%s'] * len(basic_fields)))
        args = [getattr(entity, x) for x in field_names]
        cursor.execute(sql, args)
        auto_increment_fields = [x for x in basic_fields if x.auto_increment]
        if len(auto_increment_fields) > 0:
            assert len(auto_increment_fields) == 1, 'More than 1 auto increment fields are unsupported'
            if getattr(entity, auto_increment_fields[0].field_name) is None:
                # retrieve the inserted id
                auto_increment_id = cursor.lastrowid
                setattr(entity, auto_increment_fields[0].field_name, auto_increment_id)

    @staticmethod
    def update(entity: orm_utils.Entity, cursor: Any):
        basic_fields = [x for x in entity.__FIELDS__ if type(x) == orm_utils.TableFieldDescriptor]
        field_names = set([x.field_name for x in basic_fields])
        primary_key_field_names = _extract_primary_fields(entity, basic_fields)
        updated_fields = field_names.difference(primary_key_field_names)
        sql = "update `%s` set %s where %s" % (entity.__TABLE_NAME__,
                                               ', '.join(['`'+x+'` = %s' for x in updated_fields]),
                                               ' and '.join(['`'+x+'` = %s' for x in primary_key_field_names]))
        args = [getattr(entity, x) for x in updated_fields]
        args.extend([getattr(entity, x) for x in primary_key_field_names])
        cursor.execute(sql, args)

    @staticmethod
    def select(entity: Type[orm_utils.Entity], cursor: Any, fetch_count: int = 1, **keys: Any):
        field_names, unexpected_fields = _validate_select_query_fields(entity, keys.keys())
        if len(unexpected_fields):
            raise ValueError('Unexpected fields: %s' % ', '.join(unexpected_fields))
        sql = 'select `%s` from `%s`' % ('`, `'.join(field_names), entity.__TABLE_NAME__)
        args = ()
        if len(keys) > 0:
            sql += ' where %s' % ' and '.join(['`' + x + '` = %s' for x in keys])
            args = [keys[x] for x in keys]
        cursor.execute(sql, args)
        return _fetch_result(entity, field_names, cursor, fetch_count)

    @staticmethod
    def delete(entity: Type[orm_utils.Entity], cursor: Any, **keys: Any):
        # noinspection SqlWithoutWhere
        sql = 'delete from `%s`' % entity.__TABLE_NAME__
        args = ()
        if len(keys) > 0:
            sql += ' where %s' % ' and '.join(['`'+x+'` = %s' for x in keys])
            args = [keys[x] for x in keys]
        cursor.execute(sql, args)
