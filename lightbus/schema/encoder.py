from json import JSONEncoder


def json_encode(obj, indent=2, sort_keys=True, **options):
    # TODO: This is also used for non-schema related encoding. Either move
    #       this elsewhere, or create a new general purpose encoder
    return JSONEncoder(indent=indent, sort_keys=sort_keys, **options).encode(obj)
