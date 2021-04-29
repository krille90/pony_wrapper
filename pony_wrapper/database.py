import pony.orm

db = pony.orm.Database()


def clear_database():
    db.disconnect()
    db.provider = None
    db.schema = None
    db.entities = {}

    db_vars = vars(db).copy()
    for k, v in db_vars.items():
        if isinstance(v, type) and issubclass(v, pony.orm.core.Entity) and v.__name__ != 'Entity':
            del vars(db)[k]


def is_connected():
    return db.provider is not None
