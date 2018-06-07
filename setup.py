from argparse import ArgumentParser
from distutils.core import setup
from multiprocessing import cpu_count
import numpy as np

from Cython.Build import cythonize

# setup(
#     name='bdrmapit',
#     ext_modules=cythonize([
#         'utils/utils.pyx',
#         'utils/*.py',
#         'bgp/routing_table.pyx',
#         'bgp/bgp.pyx',
#         'graph/bdrmapit.pyx',
#         'graph/router.pyx',
#         'graph/interface.pyx',
#         'graph/hybrid_graph.pyx',
#         'as2org.pyx',
#         'updates_dict.pyx',
#         'traceroute/abstract_parser.py',
#         'traceroute/parser_pool_sqlite.py',
#         'create_objs_sqlite.pyx',
#         # 'algorithm_cy.pyx'
#     ], nthreads=0)
#     # ext_modules=cythonize(['ip2as.py'])
# )
setup(
    name='bdrmapit',
    ext_modules=cythonize(
        [
            'traceroutecy/hop.pyx',
            'bgp/routing_table.pyx'
        ]
    ),
    include_dirs=[np.get_include()]
)
