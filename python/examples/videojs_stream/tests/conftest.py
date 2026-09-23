"""The example is imported as the package ``videojs_stream``, which lives one directory above this one.

The product packages' tests never import the example (see ``pyproject.toml``); only these do, so the
path entry lives here rather than in the workspace's ``pythonpath``.
"""

import sys
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[2]

if str(EXAMPLES) not in sys.path:
    sys.path.insert(0, str(EXAMPLES))
