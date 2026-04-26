"""
Strata interactive CQL shell.

    python3 -m strata.repl                         # single-node (default)
    python3 -m strata.repl --data /tmp/strata      # custom data dir
    python3 -m strata.repl --cluster n1,n2,n3      # multi-node cluster
    python3 -m strata.repl --rf 3 --consistency QUORUM

Meta-commands:
    \q, EXIT, QUIT  — quit
    \s              — cluster status
    \r              — ring token distribution
    \ks             — list keyspaces (alias for DESCRIBE KEYSPACES)
    \dt             — list tables (alias for DESCRIBE TABLES)
    \h, HELP        — print this help
"""
from __future__ import annotations

import argparse
import os
import sys

from .engine import StrataEngine
from .query.executor import ExecutionError


# ── pretty printer ────────────────────────────────────────────────────────────

def _print_table(rows: list):
    if not rows:
        print("(0 rows)")
        return
    cols = list(rows[0].keys())
    widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            widths[c] = max(widths[c], len(str(row.get(c, ''))))
    sep   = '+' + '+'.join('-' * (widths[c] + 2) for c in cols) + '+'
    hdr   = '|' + '|'.join(f" {c:{widths[c]}} " for c in cols) + '|'
    print(sep)
    print(hdr)
    print(sep)
    for row in rows:
        line = '|' + '|'.join(
            f" {str(row.get(c, '')):{widths[c]}} " for c in cols) + '|'
        print(line)
    print(sep)
    print(f"({len(rows)} row{'s' if len(rows) != 1 else ''})")


# ── REPL ─────────────────────────────────────────────────────────────────────

class REPL:
    PROMPT = 'strata> '

    def __init__(self, engine: StrataEngine, consistency: str = 'QUORUM'):
        self._engine      = engine
        self._consistency = consistency
        self._buf: list[str] = []

    def run(self):
        print("Strata CQL Shell  (\\h for help, \\q to quit)")
        print()
        while True:
            prompt = self.PROMPT if not self._buf else '     -> '
            try:
                line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                print()
                break

            stripped = line.strip()

            # Meta-commands (only at start of new statement)
            if not self._buf:
                low = stripped.lower()
                if low in ('\\q', 'exit', 'quit', 'exit;', 'quit;'):
                    break
                if low in ('\\h', 'help', 'help;'):
                    print(__doc__)
                    continue
                if low == '\\s':
                    status = self._engine.cluster_status()
                    for nid, st in status.items():
                        print(f"  {nid:20s}  {st}")
                    continue
                if low == '\\r':
                    dist = self._engine.ring_distribution()
                    for nid, cnt in sorted(dist.items()):
                        print(f"  {nid:20s}  {cnt} vnodes")
                    continue
                if low in ('\\ks',):
                    stripped = 'DESCRIBE KEYSPACES'
                    self._execute(stripped)
                    continue
                if low in ('\\dt',):
                    stripped = 'DESCRIBE TABLES'
                    self._execute(stripped)
                    continue

            self._buf.append(line)
            combined = ' '.join(self._buf)

            # Execute when we see a semicolon or a complete statement
            if ';' in combined or self._looks_complete(combined):
                stmt = combined.rstrip().rstrip(';').strip()
                self._buf = []
                if stmt:
                    self._execute(stmt)

    def _execute(self, cql: str):
        try:
            result = self._engine.execute(cql, consistency=self._consistency)
            if result.rows:
                _print_table(result.rows)
            elif result.message:
                print(result.message)
            else:
                print("OK")
        except ExecutionError as e:
            print(f"ERROR: {e}")
        except Exception as e:
            print(f"ERROR (unexpected): {e}")

    @staticmethod
    def _looks_complete(text: str) -> bool:
        up = text.strip().upper()
        return any(up.startswith(kw) for kw in
                   ('USE ', 'EXIT', 'QUIT', 'DESCRIBE', 'DESC'))


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description='Strata CQL interactive shell')
    parser.add_argument('--data', default='/tmp/strata_repl',
                        help='Data directory')
    parser.add_argument('--cluster', default='',
                        help='Comma-separated node IDs (multi-node mode)')
    parser.add_argument('--rf', type=int, default=1,
                        help='Replication factor (default 1 for single-node)')
    parser.add_argument('--consistency', default='ONE',
                        help='Default consistency level (ONE/QUORUM/ALL)')
    args = parser.parse_args()

    if args.cluster:
        node_ids = [n.strip() for n in args.cluster.split(',')]
        engine = StrataEngine.cluster(
            args.data, node_ids=node_ids,
            replication_factor=args.rf,
            consistency=args.consistency,
        )
    else:
        engine = StrataEngine.single_node(args.data, consistency=args.consistency)

    repl = REPL(engine, consistency=args.consistency)
    try:
        repl.run()
    finally:
        engine.shutdown()
    print("bye.")


if __name__ == '__main__':
    main()
