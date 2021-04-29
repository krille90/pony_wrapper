import sys
import types
from datetime import datetime

import pony.orm
from pony.utils import throw

from .database import db, is_connected
from .fields import Field, PrimaryKey


class EntityIter(object):
    def __init__(self, entity):
        self.entity = entity

    def next(self):
        throw(TypeError, 'Use select(...) function or %s.select(...) method for iteration'
              % self.entity.__name__)

    __next__ = next


def get_base(entity):
    base = entity
    while base.__bases__[0] != object:
        base = base.__bases__[0]
    return base


def _add_entity(entity):
    base = get_base(entity)
    if entity == base:
        return

    replaced = False
    new_entities = []
    for e in base._entities_:
        if e.__name__ != entity.__name__:
            new_entities.append(e)
        else:
            new_entities.append(entity)
            replaced = True

    if not replaced:
        new_entities.append(entity)

    base._entities_ = new_entities
    base._entity_map_ = {e.__name__: e for e in new_entities}

    for index in entity._indexes_:
        if index.is_pk:
            entity._pks_.extend(index.attr_names)

    if len(entity._pks_) == 0:
        for k, v in entity.fields.items():
            if isinstance(v, PrimaryKey):
                entity._pks_.append(k)


def parse(field, v):
    field_type = field.field_type

    if isinstance(field.field_type, str):
        field_type = Entity._entity_map_[field.field_type]

    if isinstance(v, field_type):
        return v
    elif field_type == float and isinstance(v, int):
        return float(v)
    elif isinstance(v, str):
        if issubclass(field_type, Entity):
            return field_type[v]
        elif issubclass(field_type, datetime):
            return datetime.fromtimestamp(float(v))
        else:
            return field_type(v)
    else:
        raise Exception(f'Error parsing {v}(type:{type(v)}) to {field_type}.')


def convert(field):
    new_args = []
    for arg in field.args:
        if isinstance(arg, type) and issubclass(arg, Entity):
            new_args.append(arg.db_entity)
        else:
            new_args.append(arg)

    return field.attr(*new_args, **field.kwargs)


class EntityMeta(type):
    def __init__(entity, what, bases=None, dict=None):
        super().__init__(what, bases, dict)

        if not hasattr(entity, '_indexes_'):
            entity._indexes_ = []
        entity._entities_ = []
        entity._entity_map_ = {}
        entity._attrs_ = []
        entity._pks_ = []

        for k, v in dict.items():
            if isinstance(v, Field):
                v._connect(entity, k)

        _add_entity(entity)

    def __iter__(entity):
        return EntityIter(getattr(db, entity.__name__))

    def __getitem__(entity, item):
        if is_connected():
            try:
                return entity(**entity.db_entity[item].to_dict())
            except pony.orm.ObjectNotFound:
                pass

        if not isinstance(item, list):
            item = [item]

        return entity(**dict(zip(entity._pks_, item)))

    @property
    def db_entity(entity):
        return db.entities[entity.__name__]

    @property
    def fields(self):
        fields = dict(vars(self))

        base = self
        while base.__bases__[0] != Entity:
            base = base.__bases__[0]
            fields.update(vars(base))

        return {k: v for k, v in fields.items() if isinstance(v, Field)}

    def create_entity(entity):
        if get_base(entity) == entity:
            base_class = db.Entity
        else:
            base_class = getattr(db, entity.__bases__[0].__name__)

        attrs = {name: convert(field) for name, field in vars(entity).items() if isinstance(field, Field)}

        if hasattr(entity, '_indexes_'):
            attrs['_indexes_'] = [idx.convert(attrs) for idx in entity._indexes_]

        type(entity.__name__, (base_class,), attrs)

    def from_db(entity, db_instance):
        return Entity._entity_map_[db_instance.__class__.__name__](**db_instance.to_dict())

    def get(entity, **kwargs):
        return entity(**entity.db_entity.get(**kwargs).to_dict())

    def exists(entity, **kwargs):
        return entity.db_entity.exists(**kwargs)

    def select(entity, **kwargs):
        kwargs = {k: v.db_instance if isinstance(v, Entity) else v for k, v in kwargs.items()}
        return Query(entity.db_entity.select(**kwargs))


class Entity(metaclass=EntityMeta):
    def __init__(self, **attrs):
        self._attrs = self._parse_attrs(attrs)

        vars(self).update(self._attrs)

    def _parse_attrs(self, kwargs):
        return {k: parse(self.__class__.fields[k], v) for k, v in kwargs.items()}

    def __str__(self):
        attrs = []
        for attr in self._pks_:
            a = getattr(self, attr)
            attrs.append(f'\'{a}\'' if type(a) is str else str(a))

        return f'{self.__class__.__name__}<{",".join(attrs)}>'

    def __repr__(self):
        return str(self)

    def _get_db_attrs(self, names=None):
        if names is None:
            names = self._attrs.keys()

        attrs = {}
        for k in names:
            v = getattr(self, k)
            attrs[k] = v.db_instance if isinstance(v, Entity) else v

        return attrs

    @pony.orm.db_session
    def create(self):
        self.__class__.db_entity(**self._get_db_attrs())
        return self

    @pony.orm.db_session
    def update(self, insert=False):
        db_instance = self.db_instance
        if db_instance is None and insert:
            self.create()
        elif db_instance is None:
            raise pony.orm.RowNotFound()
        else:
            for k, v in self._get_db_attrs().items():
                setattr(db_instance, k, v)
        return self

    @pony.orm.db_session
    def fetch(self):
        vars(self).update(self.db_instance.to_dict())
        return self

    @property
    def db_instance(self):
        attrs = self._get_db_attrs(self._pks_)

        return self.__class__.db_entity.get(**attrs)


class QueryResult:
    def __init__(self, pony_query_result: pony.orm.core.QueryResult):
        self.pony_query_result = pony_query_result

    def __str__(self):
        return str(self.pony_query_result)

    def __repr__(self):
        return repr(self.pony_query_result)

    def __len__(self):
        return len(self.pony_query_result)

    def __getitem__(self, key):
        db_instance = self.pony_query_result[key]
        return Entity.from_db(db_instance)

    def __contains__(self, item):
        db_instance = item.get_db_instance(item)
        return db_instance in self.pony_query_result

    def index(self, item):
        db_instance = item.get_db_instance(item)
        return Entity.from_db(self.pony_query_result.index(db_instance))

    def __eq__(self, other):
        return self.pony_query_result == other.pony_query_result

    def __ne__(self, other):
        return self.pony_query_result != other.pony_query_result

    def __lt__(self, other):
        return self.pony_query_result < other.pony_query_result

    def __le__(self, other):
        return self.pony_query_result <= other.pony_query_result

    def __gt__(self, other):
        return self.pony_query_result > other.pony_query_result

    def __ge__(self, other):
        return self.pony_query_result >= other.pony_query_result

    def __reversed__(self):
        return reversed(self.pony_query_result)

    def reverse(self):
        self.pony_query_result.reverse()

    def sort(self, *args, **kwargs):
        self.pony_query_result.sort(*args, **kwargs)

    def shuffle(self):
        self.pony_query_result.shuffle()

    def show(self, width=None, stream=None):
        self.pony_query_result.show(width, stream)

    def to_json(self, include=(), exclude=(), converter=None, with_schema=True, schema_hash=None):
        return self.pony_query_result.to_json(include, exclude, converter, with_schema, schema_hash)

    def __add__(self, other):
        result = []
        result.extend(self)
        result.extend(other)
        return result

    def __radd__(self, other):
        result = []
        result.extend(other)
        result.extend(self)
        return result

    def __iter__(self):
        for e in self.pony_query_result:
            yield Entity.from_db(e)

    def to_list(self):
        return list(self)


class Query:
    def __init__(self, pony_query: pony.orm.core.Query):
        self.pony_query = pony_query

    def prefetch(self, *args):
        return Query(self.pony_query.prefetch(*args))

    def get_sql(self):
        return self.pony_query.get_sql()

    def show(self, width=None, stream=None):
        self.pony_query.show(width, stream)

    def get(self):
        return Entity.from_db(self.pony_query.get())

    def first(self):
        return Entity.from_db(self.pony_query.first())

    def without_distinct(self):
        return Query(self.pony_query.without_distinct())

    def distinct(self):
        return Query(self.pony_query.distinct())

    def exists(self):
        return self.pony_query.exists()

    def delete(self, bulk=None):
        return self.pony_query.delete(bulk)

    def order_by(self, *args):
        new_args = [arg.db_field if isinstance(arg, Field) else arg for arg in args]

        if isinstance(args[0], types.FunctionType):
            return Query(self.pony_query.order_by(*new_args, {}, {'desc': pony.orm.desc}))
        else:
            return Query(self.pony_query.order_by(*new_args))

    def sort_by(self, *args):
        return Query(self.pony_query.sort_by(*args))

    def filter(self, *args, **kwargs):
        return Query(self.pony_query.filter(*args, **kwargs))

    def where(self, *args, **kwargs):
        return Query(self.pony_query.where(*args, **kwargs))

    def fetch(self, limit=None, offset=None):
        return QueryResult(self.pony_query.fetch(limit, offset))

    def limit(self, limit=None, offset=None):
        return QueryResult(self.pony_query.limit(limit, offset))

    def page(self, pagenum, pagesize=10):
        return QueryResult(self.pony_query.page(pagenum, pagesize))

    def sum(self, distinct=None):
        return self.pony_query.sum(distinct)

    def avg(self, distinct=None):
        return self.pony_query.avg(distinct)

    def group_concat(self, sep=None, distinct=None):
        return self.pony_query.group_concat(sep, distinct)

    def min(self):
        return self.pony_query.min()

    def max(self):
        return self.pony_query.max()

    def count(self, distinct=None):
        return self.pony_query.count(distinct)

    def for_update(self, nowait=False, skip_locked=False):
        return Query(self.pony_query.for_update(nowait, skip_locked))

    def random(self, limit):
        return self.pony_query.order_by('random()')[:limit]

    def to_json(self, include=(), exclude=(), converter=None, with_schema=True, schema_hash=None):
        return self.pony_query.to_json(include, exclude, converter, with_schema, schema_hash)

    def __iter__(self):
        for e in self.pony_query:
            yield Entity.from_db(e)

    def __getitem__(self, key):
        return QueryResult(self.pony_query[key])

    def __len__(self):
        return len(self.pony_query)

    def __reduce__(self):
        return self.pony_query.__reduce__()


def desc(expr):
    if isinstance(expr, Field):
        return expr.db_field
    return expr


def get_local_vars(depth=2):
    local_vars = {}

    cls_dict = sys._getframe(depth).f_locals

    for k, v in cls_dict.items():
        if isinstance(v, Entity):
            local_vars[k] = v.db_instance
        if isinstance(v, type) and issubclass(v, Field) and v != Field:
            local_vars[k] = getattr(pony.orm, k)

    for e in Entity._entities_:
        local_vars[e.__name__] = e.db_entity

    local_vars['desc'] = pony.orm.desc

    return local_vars


def select(*args):
    return Query(pony.orm.select(*args, {}, get_local_vars()))


def show(entity):
    pony.orm.show(entity.db_entity)


def delete(*args):
    return pony.orm.delete(*args)


def exists(*args):
    return pony.orm.exists(*args, {}, get_local_vars())


def get(*args):
    return Query(pony.orm.get(*args, {}, get_local_vars()))


def left_join(*args):
    return Query(pony.orm.left_join(*args, {}, get_local_vars()))


def create_entities():
    for e in Entity._entities_:
        e.create_entity()
