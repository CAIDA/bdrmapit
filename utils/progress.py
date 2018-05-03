import sys


class Progress:
    """A class for creating progress updates for long running operations."""

    should_output = True

    def __init__(self, total=None, message='', increment=1, multiplier=1, start=0, callback=None, force=False):
        self.total = total
        self.message = message
        self.increment = increment
        self.multiplier = multiplier
        self.current = start
        self.callback = callback if callback else str
        self.should_output = force or Progress.should_output

    def iterator(self, iterable):
        """Iterates over iterable and automatically updates the status at the predefined increments."""
        if self.should_output:
            self.show()
            i = 0
            for n in iterable:
                i += self.multiplier
                yield n
                if i == self.increment:
                    self.current += i
                    i = 0
                    self.show()
            self.current += i
            self.finish()
        else:
            yield from iterable

    def finish(self):
        self.show()
        sys.stderr.write('\n')

    def show(self):
        if self.total:
            sys.stderr.write('\r\033[K{:s} {:.2%} ({:,d} / {:,d}). {:s}'.format(self.message, self.current / self.total, self.current, self.total, self.callback()))
        else:
            sys.stderr.write('\r\033[K{:s} {:,d}. {:s}'.format(self.message, self.current, self.callback()))

    @staticmethod
    def set_output(b):
        Progress.should_output = b

    @staticmethod
    def message(message):
        if Progress.should_output:
            print(message)
