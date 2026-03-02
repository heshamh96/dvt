"""
Internal "global" project name for DVT built-in macros and reserved project names.
User projects cannot be named this; it is used by the macro resolver and init task.
"""

import os

PACKAGE_PATH = os.path.dirname(__file__)
PROJECT_NAME = "dvt"
