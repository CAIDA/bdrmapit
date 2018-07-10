NONE = 0
ERROR = 1
WARNING = 2
INFO = 3
DEBUG = 4


class Log:
    level = NONE

    def __init__(self, level=NONE):
        self._clevel = self.level
        self._level = level

    def __enter__(self):
        Log.level = self._level
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        Log.level = self._clevel
        return False

    def set_level(self, level):
        Log.level = level

    def log(self, level: int, *args, **kwargs):
        if level <= Log.level:
            print(*args, **kwargs)

    def error(self, *args, **kwargs):
        self.log(ERROR, *args, **kwargs)

    def warning(self, *args, **kwargs):
        self.log(WARNING, *args, **kwargs)

    def info(self, *args, **kwargs):
        self.log(INFO, *args, **kwargs)

    def debug(self, *args, **kwargs):
        self.log(DEBUG, *args, **kwargs)

    def level_check(self, level):
        return level <= Log.level

    def isdebug(self):
        return self.level_check(DEBUG)
