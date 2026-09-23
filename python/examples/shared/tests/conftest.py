"""The shared policies are imported as the package ``shared``, which lives one directory above this one.

The product packages' tests never import the examples (see ``pyproject.toml``); the example scripts
put this directory on ``sys.path`` themselves, and these tests do the same here.
"""

import sys
from pathlib import Path

EXAMPLES = Path(__file__).resolve().parents[2]

if str(EXAMPLES) not in sys.path:
    sys.path.insert(0, str(EXAMPLES))
