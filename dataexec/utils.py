from importlib import import_module
import hashlib
import random
import string
import secrets

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
