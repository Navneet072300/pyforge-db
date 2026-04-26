from forge.repl import REPL
from forge.engine import DatabaseEngine
import sys, os

data_dir = sys.argv[1] if len(sys.argv) > 1 else '/tmp/forge_data'
engine = DatabaseEngine(data_dir)
REPL(engine).run()
