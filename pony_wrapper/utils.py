import types

from .database import *
from .core import create_entities

_db_stack = []


def _bind_database(*args, **kwargs):
    db.bind(*args, **kwargs)
    db.execute('PRAGMA journal_mode=WAL')

    create_entities()

    db.generate_mapping(create_tables=True)


def bind_database(*args, **kwargs):
    clear_database()

    _bind_database(*args, **kwargs)

    _db_stack.append((args, kwargs))


def unbind_database():
    _db_stack.pop()
    clear_database()

    if len(_db_stack) > 0:
        args, kwargs = _db_stack[-1]
        _bind_database(*args, **kwargs)


class DatabaseContext:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class DatabaseConsumer:
    def __init__(self, db_context: DatabaseContext):
        self.db_context = db_context

    @staticmethod
    def _decorate_function(db_context, func):
        def wrapper(*args, **kwargs):
            bind_database(*db_context.args, **db_context.kwargs)
            func(*args, **kwargs)
            unbind_database()

        return wrapper

    def __getattribute__(self, name):
        func = object.__getattribute__(self, name)

        if isinstance(func, types.MethodType):
            return self._decorate_function(self.db_context, func)
        else:
            return func


class DatabaseContextManager:
    def __init__(self, *_args, **_kwargs):
        self._args = _args
        self._kwargs = _kwargs

    def __enter__(self):
        bind_database(*self._args, **self._kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        unbind_database()


using_db = DatabaseContextManager


def db_context(*_args, **_kwargs):
    def decorator(func):
        def wrapper(*args, **kwargs):
            bind_database(*_args, **_kwargs)
            func(*args, **kwargs)
            unbind_database()

        return wrapper

    return decorator
