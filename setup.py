from distutils.core import setup

from Cython.Build import cythonize

setup(
    name='bdrmapit',
    ext_modules=cythonize([
        'utils/utils.pyx',
        'utils/progress.py',
        'utils/subprocess_pool.py',
        'bgp/routing_table.pyx',
        'bgp/bgp.pyx',
        'graph/bdrmapit.pyx',
        'graph/router.pyx',
        'graph/interface.pyx',
        'graph/hybrid_graph.pyx',
        'as2org.pyx',
        'updates_dict.pyx',
        'traceroute/abstract_parser.py',
        'traceroute/parser_pool_sqlite.py',
        'create_objs_cy.pyx',
        'algorithm_cy.pyx',
        'last_hop.pyx'
    ], nthreads=0)
)
