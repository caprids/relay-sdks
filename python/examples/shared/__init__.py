"""Policies the examples and the live smoke test share; the SDK deliberately owns none of them.

Each module is one policy, named after its concept in ``CONTEXT.md``. Import it as
``from shared.<policy> import ...`` with ``python/examples`` on ``sys.path``, which the example
scripts arrange for themselves and the tests' ``conftest.py`` arranges for them.
"""
