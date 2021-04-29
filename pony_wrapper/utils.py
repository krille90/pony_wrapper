import types

from .database import *
from .core import create_entities

_db_stack = []


@db.on_connect(provider='sqlite')
def sqlite_wal_mode(db, connection):
    cursor = connection.cursor()
    cursor.execute('PRAGMA journal_mode=WAL')


def _bind_database(*args, **kwargs):
    db.bind(*args, **kwargs)

    create_entities()

    db.generate_mapping(create_tables=True)


def bind_database(*args, **kwargs):
    clear_database()

    _bind_database(*args, **kwargs)

    _db_stack.append(DatabaseContext(*args, **kwargs))


def unbind_database():
    _db_stack.pop()
    clear_database()

    db_ctx = get_current_db_context()

    if db_ctx is not None:
        _bind_database(*db_ctx.args, **db_ctx.kwargs)


def get_current_db_context():
    if len(_db_stack) > 0:
        return _db_stack[-1]


class DatabaseContext:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        return f'DBContext({self.args}, {self.kwargs})'


class DatabaseConsumer:
    def __init__(self):
        self._db_context = get_current_db_context()

    @staticmethod
    def _decorate_function(db_ctx, func):
        def wrapper(*args, **kwargs):
            if db_ctx is not None:
                with using_db(db_ctx):
                    func(*args, **kwargs)
            else:
                func(*args, **kwargs)

        return wrapper

    def set_db_context(self, *args, **kwargs):
        if isinstance(args[0], DatabaseContext):
            self._db_context = args[0]
        else:
            self._db_context = DatabaseContext(*args, **kwargs)

    def __getattribute__(self, name):
        func = object.__getattribute__(self, name)

        if isinstance(func, types.MethodType):
            return self._decorate_function(self._db_context, func)
        else:
            return func


class DatabaseContextManager:
    def __init__(self, *args, **kwargs):
        assert len(args) > 0, "Database arguments need to be set."
        if isinstance(args[0], DatabaseContext):
            assert len(args) == 1 and len(kwargs) == 0, \
                'DatabaseContext can only be used alone without additional arguments.'
            self._args = args[0].args
            self._kwargs = args[0].kwargs
        else:
            self._args = args
            self._kwargs = kwargs

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
