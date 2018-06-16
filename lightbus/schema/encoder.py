from json import JSONEncoder


def json_encode(obj, indent=2, sort_keys=True, **options):
    return JSONEncoder(indent=indent, sort_keys=sort_keys, **options).encode(obj)
