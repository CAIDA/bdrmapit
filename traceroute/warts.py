import json
from subprocess import Popen, PIPE


class Warts:
    def __init__(self, filename, json=True):
        self.filename = filename
        self.json = json

    def __enter__(self):
        ftype = self.filename.rpartition('.')[2]
        if ftype == 'gz':
            self.p = Popen('gunzip -c {} | sc_warts2json'.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        elif ftype == 'bz2':
            self.p = Popen('bzcat {} | sc_warts2json'.format(self.filename), shell=True, stdout=PIPE,
                           universal_newlines=True)
        else:
            self.p = self.p = Popen('sc_warts2json {}'.format(self.filename), shell=True, stdout=PIPE,
                                    universal_newlines=True)
        if json:
            for line in self.p.stdout:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass
        else:
            return self.p.stdout

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.p.kill()
        return False
