from typing import TypeVar, Type

T = TypeVar('T', bound='Parent')


class Parent(object):

    def instance_method(self: T, x: int) -> T:
        return self

    @classmethod
    def class_method(cls: Type[T]) -> T:
        return cls()


class Child(Parent):

    def foo(self):
        pass


# Child().instance_method()  # No hinting
# Child().class_method()  # No hinting
# Child().instance_method().foo()  # Ok
# Child().instance_method().bad()  # Invalid


import pdb; pdb.set_trace()
