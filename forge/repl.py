"""
Interactive REPL for the Forge relational engine.

Usage:
  python3 -m forge.repl [--data-dir PATH]
"""
import argparse
import os
import readline
import sys
import textwrap
from typing import List

from .engine import DatabaseEngine, Connection, QueryResult


# ── pretty-printer ────────────────────────────────────────────────────────────

def format_table(result: QueryResult) -> str:
    rows    = result.rows
    columns = result.columns

    if not columns and not rows:
        return result.message

    if not rows:
        return f"{result.message}\n(0 rows)"

    # Compute column widths
    widths = {c: len(str(c)) for c in columns}
    for row in rows:
        for c in columns:
            w = len(str(row.get(c, '')))
            if w > widths[c]:
                widths[c] = w

    sep  = '+' + '+'.join('-' * (widths[c] + 2) for c in columns) + '+'
    hdr  = '|' + '|'.join(f" {c:<{widths[c]}} " for c in columns) + '|'

    lines = [sep, hdr, sep]
    for row in rows:
        line = '|' + '|'.join(
            f" {str(row.get(c, '')):<{widths[c]}} " for c in columns) + '|'
        lines.append(line)
    lines.append(sep)

    n    = len(rows)
    time = f" ({result.elapsed * 1000:.1f} ms)" if result.elapsed else ''
    lines.append(f"({n} row{'s' if n != 1 else ''}){time}")
    if result.message:
        lines.append(result.message)
    return '\n'.join(lines)


# ── REPL ──────────────────────────────────────────────────────────────────────

BANNER = textwrap.dedent("""\
    ┌─────────────────────────────────────────────┐
    │     PyDB Phase-1 — PostgreSQL-like engine    │
    │  Type \\q or Ctrl-D to quit, \\h for help     │
    └─────────────────────────────────────────────┘
""")

HELP_TEXT = textwrap.dedent("""\
    Commands:
      \\q, \\quit         quit
      \\dt               list tables
      \\d TABLE          describe table
      \\h, \\help         show this help

    SQL examples:
      CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER);
      INSERT INTO users VALUES (1, 'Alice', 30);
      SELECT * FROM users WHERE age > 25 ORDER BY name;
      UPDATE users SET age = 31 WHERE id = 1;
      DELETE FROM users WHERE id = 1;
      BEGIN;  INSERT ...;  COMMIT;
      CREATE INDEX idx_age ON users (age);
""")


class REPL:
    def __init__(self, data_dir: str = './data'):
        self.db   = DatabaseEngine(data_dir)
        self.conn = self.db.connect()
        self._buf  = []          # multi-line SQL accumulation

    def run(self):
        print(BANNER)
        history_file = os.path.expanduser('~/.pydb_history')
        try:
            readline.read_history_file(history_file)
        except FileNotFoundError:
            pass

        while True:
            prompt = 'pydb# ' if not self._buf else '     > '
            try:
                line = input(prompt)
            except (EOFError, KeyboardInterrupt):
                print()
                break

            # Meta-commands
            stripped = line.strip()
            if stripped in (r'\q', r'\quit', 'exit', 'quit'):
                break
            if stripped in (r'\h', r'\help'):
                print(HELP_TEXT)
                self._buf.clear()
                continue
            if stripped == r'\dt':
                for t in self.db.catalog.all_tables():
                    print(f'  {t}')
                self._buf.clear()
                continue
            if stripped.startswith(r'\d '):
                tname = stripped[3:].strip()
                self._describe(tname)
                self._buf.clear()
                continue

            self._buf.append(line)
            full = ' '.join(self._buf)

            # Execute when we see a semicolon (or user hits enter on empty line after content)
            if ';' in full or (stripped == '' and self._buf):
                sql = full.strip()
                self._buf.clear()
                if sql and sql != ';':
                    self._run_sql(sql)
            # else: keep accumulating

        try:
            readline.write_history_file(history_file)
        except Exception:
            pass
        self.db.close()
        print('Goodbye.')

    def _run_sql(self, sql: str):
        # Split on semicolons and run each statement
        stmts = [s.strip() for s in sql.split(';') if s.strip()]
        for stmt in stmts:
            result = self.db.execute(stmt, self.conn)
            print(format_table(result))

    def _describe(self, table: str):
        if not self.db.catalog.table_exists(table):
            print(f"Table '{table}' does not exist.")
            return
        schema = self.db.catalog.get_table(table)
        print(f"\nTable: {table}")
        print(f"{'Column':<20} {'Type':<12} {'Nullable':<10} {'PK'}")
        print('-' * 50)
        for col in schema.columns:
            pk  = 'YES' if col.primary_key else ''
            nul = 'YES' if col.nullable    else 'NO'
            print(f"{col.name:<20} {col.col_type:<12} {nul:<10} {pk}")
        if schema.indices:
            print('\nIndices:')
            for idx in schema.indices:
                print(f"  {idx.index_name} ON ({', '.join(idx.columns)})"
                      + (' UNIQUE' if idx.unique else ''))
        print()


def main():
    parser = argparse.ArgumentParser(description='PyDB Phase-1 REPL')
    parser.add_argument('--data-dir', default='./pydb_data',
                        help='Directory for database files')
    args = parser.parse_args()
    REPL(args.data_dir).run()


if __name__ == '__main__':
    main()
