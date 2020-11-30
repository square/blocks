import atexit
import tempfile

import wrapt


@wrapt.decorator
def with_function_tmpdir(wrapped, instance, args, kwargs):
    with tempfile.TemporaryDirectory() as tmpdir:
        kwargs["tmpdir"] = tmpdir
        return wrapped(*args, **kwargs)


@wrapt.decorator
def with_session_tmpdir(wrapped, instance, args, kwargs):
    tmpdir = tempfile.TemporaryDirectory()
    kwargs["tmpdir"] = tmpdir.name
    atexit.register(tmpdir.cleanup)
    return wrapped(*args, **kwargs)
