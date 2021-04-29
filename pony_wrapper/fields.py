import sys
from typing import Type

import pony.orm

from .database import db
from .index import Index


class Field:
    def __init__(self, attr: Type[pony.orm.core.Attribute], *args, **kwargs):
        self.attr = attr
        self.args = args
        self.kwargs = kwargs

        self.field_type = args[0]

        self.entity = None
        self.name = None

    def _connect(self, entity, name):
        self.entity = entity
        self.name = name

        if isinstance(self.field_type, str):
            pass

    @property
    def db_field(self):
        if self.entity is not None and self.name is not None:
            return getattr(db.entities[self.entity.__name__], self.name)


class PrimaryKey(Field):
    def __init__(self, *args, **kwargs):
        super().__init__(pony.orm.core.PrimaryKey, *args, **kwargs)

    def __new__(cls, *args, **kwargs):
        attrs = tuple(a for a in args if isinstance(a, Field))

        if not attrs:
            return super().__new__(cls)

        cls_dict = sys._getframe(1).f_locals

        attr_names = []
        for attr in attrs:
            for k, v in cls_dict.items():
                if v == attr:
                    attr_names.append(k)

        indexes = cls_dict.setdefault('_indexes_', [])
        indexes.append(Index(attr_names, is_pk=True))


class Required(Field):
    def __init__(self, *args, **kwargs):
        super().__init__(pony.orm.core.Required, *args, **kwargs)


class Optional(Field):
    def __init__(self, *args, **kwargs):
        super().__init__(pony.orm.core.Optional, *args, **kwargs)


class Set(Field):
    def __init__(self, *args, **kwargs):
        super().__init__(pony.orm.core.Set, *args, **kwargs)
