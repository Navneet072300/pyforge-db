from forge.repl import REPL
import sys

data_dir = sys.argv[1] if len(sys.argv) > 1 else '/tmp/forge_data'
REPL(data_dir).run()
