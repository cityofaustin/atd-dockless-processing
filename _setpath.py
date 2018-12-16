# appends parent directory to path so that script can be imported an
# external package (e.g., launch.py)
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
