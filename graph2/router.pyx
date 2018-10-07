cdef class Router:

    def __init__(self, str name):
        self.name = name

    def __repr__(self):
        return 'Router<{}>'.format(self.name)
