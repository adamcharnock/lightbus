import asyncio
from collections import defaultdict
from typing import Dict, NamedTuple, List, Callable, Optional

from lightbus.client.utilities import queue_exception_checker, ErrorQueueType
from lightbus.utilities.async_tools import run_user_provided_callable


class CallbackKey(NamedTuple):
    name: str
    run_before_plugins: bool


class HookRegistry:
    def __init__(
        self,
        error_queue: ErrorQueueType,
        execute_plugin_hooks: Callable,
        extra_parameters: Optional[dict] = None,
    ):
        self._hook_callbacks: Dict[CallbackKey, List[Callable]] = defaultdict(list)
        self.error_queue = error_queue
        self.execute_plugin_hooks = execute_plugin_hooks
        self.extra_parameters = extra_parameters or {}

    def set_extra_parameter(self, name, value):
        self.extra_parameters[name] = value

    async def execute(self, name, **kwargs):
        # Hooks that need to run before plugins
        key = CallbackKey(name, run_before_plugins=True)
        for callback in self._hook_callbacks[key]:
            await queue_exception_checker(
                run_user_provided_callable(
                    callback,
                    args=[],
                    kwargs=dict(**self.extra_parameters, **kwargs),
                    type_name="hook",
                ),
                self.error_queue,
            )

        await self.execute_plugin_hooks(name, **self.extra_parameters, **kwargs)

        # Hooks that need to run after plugins
        key = CallbackKey(name, run_before_plugins=False)
        for callback in self._hook_callbacks[key]:
            await run_user_provided_callable(
                callback,
                args=[],
                kwargs=dict(**self.extra_parameters, **kwargs),
                type_name="hook",
            )

    def register_callback(self, name, fn, before_plugins=False):
        key = CallbackKey(name, bool(before_plugins))
        self._hook_callbacks[key].append(fn)
