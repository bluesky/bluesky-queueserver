import io
import sys


class LogStream(io.TextIOBase):
    def __init__(self, *, source):
        super().__init__()
        self._source = source
        self._stdout = sys.__stdout__

    def write(self, s):
        msg = f"==> {s}"
        return self._stdout.write(msg)


def override_streams(file_obj):
    sys.stdout = file_obj
    sys.stderr = file_obj
