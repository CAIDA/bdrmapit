import json
from subprocess import Popen, PIPE


class RIB:
    def __init__(self, filename):
        self.filename = filename

    def __enter__(self):
        self.p = Popen('bgpdump -m {}'.format(self.filename), shell=True, stdout=PIPE, universal_newlines=True)
        return self.p.stdout

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.p.kill()
        return False
