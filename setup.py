from distutils.core import setup
from multiprocessing import cpu_count

from Cython.Build import cythonize

setup(
    name='bdrmapit',
    ext_modules=cythonize([
        'utils/utils.pyx',
        # 'utils/progress.py',
        'bgp/routing_table.pyx',
        'bgp/bgp.pyx',
        # 'graph/bdrmapit.pyx',
        'graph/router.pyx',
        'graph/interface.pyx',
        # 'graph/hybrid_graph.pyx',
        'as2org.pyx',
        # 'updates_dict.pyx',
        # 'create_objs.pyx',
        'traceroute/hop.pyx'
    ], nthreads=0)
    # ext_modules=cythonize(['ip2as.py'])
)
