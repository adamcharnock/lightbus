""" Some spaghetti code to test the practicality of stub generation

Use:

    python stub_maker.py

"""
from collections import OrderedDict
from typing import Any

from bottle import HTTPResponse


class Event(object): pass


class MyApi(object):
    my_event = Event()

    def _util(self):
        pass

    def method1(self, user_id: int, *args, **kwargs) -> dict:
        return {}

    def method2(self) -> HTTPResponse:
        return HTTPResponse()


def parse_type(t):
    if t.__module__ == 'typing':
        return t, repr(t).split('.', maxsplit=1)[1]
    else:
        return t, t.__name__

if __name__ == '__main__':
    import inspect

    api = MyApi
    events = []
    methods = []
    for k, v in api.__dict__.items():
        if isinstance(v, Event):
            events.append(k)
        elif callable(v) and not k.startswith('_'):
            methods.append(k)

    imports_needed = set()

    imports_needed |= set(api.__bases__)
    stub = "class {}({}):\n".format(api.__name__, ','.join(c.__name__ for c in api.__bases__))

    for event in events:
        imports_needed.add(Event)
        stub += "    {} = Event()\n".format(event)

    for method in methods:
        arg_spec = inspect.getfullargspec(getattr(api, method))
        annotated_args = OrderedDict()
        for arg_name in arg_spec.args:
            type_ = arg_spec.annotations.get(arg_name, Any)
            imports_needed.add(type_)
            annotated_args[arg_name] = type_

        annotated_args_formatted = []
        for i, (name, t) in enumerate(annotated_args.items()):
            if i == 0:
                annotated_args_formatted.append(name)  # 'self' doesn't need an annotation
            else:
                t, type_name = parse_type(t)
                imports_needed.add(t)
                annotated_args_formatted.append('{}: {}'.format(name, type_name))

        return_type = arg_spec.annotations.get('return', Any)
        return_type, return_type_name = parse_type(return_type)
        imports_needed.add(return_type)

        stub += "    def {}({}) -> {}:\n".format(method, ', '.join(annotated_args_formatted), return_type_name)
        stub += "        pass\n\n"

    import_statements = []
    for type_ in imports_needed:
        type_, name = parse_type(type_)
        if type_.__module__ in ('builtins', '__main__'):
            continue
        import_statements.append(
            'from {} import {}'.format(type_.__module__, name)
        )
    stub = '\n'.join(import_statements) + '\n\n' + stub

    print(stub)

