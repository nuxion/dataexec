import asyncio
from importlib import import_module
import hashlib
import random
import string
import secrets
import inspect

_letters = string.ascii_lowercase


def basic_random(lenght=10) -> str:
    return "".join(random.choice(_letters) for i in range(lenght))


def basic_hash(txt: str) -> str:
    _hash = hashlib.md5(txt.encode())
    return _hash.hexdigest()


def secure_random_str(size=12) -> str:
    return secrets.token_urlsafe(size)


def get_class(fullclass_path):
    """get a class or object from a module. The fullclass_path should be passed as:
    package.my_module.MyClass
    """
    module, class_ = fullclass_path.rsplit(".", maxsplit=1)
    mod = import_module(module)
    cls = getattr(mod, class_)
    return cls


async def from_async2sync(func, *args, **kwargs):
    """Run sync functions from async code"""
    loop = asyncio.get_running_loop()
    rsp = await loop.run_in_executor(None, func, *args, **kwargs)
    return rsp


def from_sync2async(func, *args, **kwargs):
    """run async functions from sync code"""
    loop = asyncio.get_event_loop()
    rsp = loop.run_until_complete(func(*args, **kwargs))
    return rsp


def async_wrapper(func, *args, **kwargs):
    coro = inspect.iscoroutinefunction(func)
    if coro:
        loop = asyncio.get_event_loop()
        rsp = loop.run_until_complete(func(*args, **kwargs))
    else:
        rsp = func(*args, **kwargs)
    return rsp
