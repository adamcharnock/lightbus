from typing import Dict, List, NamedTuple, Callable


class Listener(NamedTuple):
    once: bool
    callback: Callable


class ListenerProxy(object):
    """ Allow for listening for attribute access """

    def __init__(self, obj):
        self.obj = obj
        self._lp_listeners: Dict[str, List[Listener]] = {}

    def __getattr__(self, item):
        if item not in self._lp_listeners:
            return getattr(self.obj, item)
        else:
            pass

    def lp_listen(self, attr_name, callback, once=False):
        self._lp_listeners.setdefault(attr_name, [])
        self._lp_listeners[attr_name].append(Listener(once=once, callback=callback))

    def _lp_fire(self, attr_name):
        listeners = self._lp_listeners.get(attr_name, [])
        for listener in listeners:
            listener.callback()

        if listeners:
            self._lp_listeners[attr_name] = [l for l in listeners if not l.once]
