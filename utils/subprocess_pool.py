import subprocess


class Popen2:
    def __init__(self, max_num):
        self.max_num = max_num
        self.ps = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wait()
        return False

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