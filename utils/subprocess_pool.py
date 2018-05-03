import subprocess
from select import select
from typing import Iterable


class Popen2:
    def __init__(self, max_num):
        self.max_num = max_num
        self.ps = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wait()
        return False

    def map(self, cmd: str, iterable: Iterable, stderr=subprocess.DEVNULL):
        iters = iter(iterable)
        rs = []
        for _, i in zip(range(self.max_num), iters):
            p = subprocess.Popen(cmd.format(i), shell=True, stdout=subprocess.PIPE, universal_newlines=True)
            rs.append(p.stdout)
        while rs:
            rlist, _, _ = select(rs, [], [])
            for p in rlist:
                line = next(p)
                if line is None:
                    rlist.remove(p)
                    try:
                        i = next(iters)
                    except StopIteration:
                        pass
                    p = subprocess.Popen(cmd.format(i), shell=True, stdout=subprocess.PIPE, universal_newlines=True)
                    rs.append(p.stdout)
                else:
                    yield line

    def run(self, cmd, stderr=subprocess.DEVNULL, **kargs):
        while len(self.ps) >= self.max_num:
            for i, (p, c) in enumerate(self.ps):
                p.poll()
                if p.returncode is not None:
                    o = self.ps.pop(i)
                    print('Done: {}'.format(c))
                    break
        print(cmd)
        p = subprocess.Popen(cmd, stderr=stderr, **kargs)
        self.ps.append((p, cmd))

    def wait(self):
        for (p, c) in self.ps:
            p.wait()
            print('Done: {}'.format(c))