"""
End-to-end demo for Forge — PostgreSQL-like engine.

Run:  python3 -m forge.demo
"""
import os
import shutil
import sys
import textwrap

# Allow running as a script from the repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from forge.engine import DatabaseEngine
from forge.repl   import format_table

DATA_DIR = '/tmp/pydb_demo'


def banner(title: str):
    print('\n' + '═' * 60)
    print(f'  {title}')
    print('═' * 60)


def run(db, conn, sql: str, header: str = ''):
    if header:
        print(f'\n── {header}')
    # Pretty-print multi-line SQL
    for line in textwrap.dedent(sql).strip().splitlines():
        print(f'  sql> {line}')

    stmts = [s.strip() for s in sql.split(';') if s.strip()]
    for stmt in stmts:
        result = db.execute(stmt, conn)
        print(format_table(result))


def main():
    # ── setup ─────────────────────────────────────────────────────────────────
    if os.path.exists(DATA_DIR):
        shutil.rmtree(DATA_DIR)

    db   = DatabaseEngine(DATA_DIR)
    conn = db.connect()

    # ══════════════════════════════════════════════════════════════════════════
    banner('PHASE 1 — PostgreSQL-like Engine Demo')

    # ── DDL ───────────────────────────────────────────────────────────────────
    banner('1. CREATE TABLE')
    run(db, conn, """
        CREATE TABLE departments (
            dept_id   INTEGER PRIMARY KEY,
            dept_name TEXT    NOT NULL
        )
    """)
    run(db, conn, """
        CREATE TABLE employees (
            id        INTEGER PRIMARY KEY,
            name      TEXT    NOT NULL,
            dept_id   INTEGER,
            salary    FLOAT,
            active    BOOLEAN,
            joined    TIMESTAMP
        )
    """)

    # ── INSERTs ───────────────────────────────────────────────────────────────
    banner('2. INSERT')
    run(db, conn, """
        INSERT INTO departments VALUES (1, 'Engineering');
        INSERT INTO departments VALUES (2, 'Sales');
        INSERT INTO departments VALUES (3, 'HR')
    """, "Insert departments")

    run(db, conn, """
        INSERT INTO employees VALUES (1, 'Alice',   1, 95000.0, true,  '2020-01-15 09:00:00');
        INSERT INTO employees VALUES (2, 'Bob',     2, 72000.0, true,  '2019-06-01 09:00:00');
        INSERT INTO employees VALUES (3, 'Charlie', 1, 88000.0, false, '2021-03-22 09:00:00');
        INSERT INTO employees VALUES (4, 'Diana',   3, 65000.0, true,  '2022-11-01 09:00:00');
        INSERT INTO employees VALUES (5, 'Eve',     1, 102000.0,true,  '2018-08-14 09:00:00')
    """, "Insert employees")

    # ── SELECT ────────────────────────────────────────────────────────────────
    banner('3. SELECT — basic, WHERE, ORDER BY, LIMIT')
    run(db, conn, "SELECT * FROM departments", "All departments")
    run(db, conn, "SELECT * FROM employees WHERE active = true ORDER BY salary DESC",
        "Active employees by salary (desc)")
    run(db, conn, "SELECT name, salary FROM employees WHERE salary > 80000 ORDER BY name LIMIT 3",
        "Top earners (limit 3)")

    # ── Aggregate functions ───────────────────────────────────────────────────
    banner('4. Aggregate functions')
    run(db, conn, """
        SELECT COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary)
        FROM employees WHERE active = true
    """, "Aggregate over active employees")

    run(db, conn, """
        SELECT dept_id, COUNT(*) AS headcount, AVG(salary) AS avg_sal
        FROM employees
        GROUP BY dept_id
        ORDER BY dept_id
    """, "Group by department")

    # ── JOIN ─────────────────────────────────────────────────────────────────
    banner('5. JOIN')
    run(db, conn, """
        SELECT e.name, e.salary, d.dept_name
        FROM employees e
        JOIN departments d ON e.dept_id = d.dept_id
        WHERE e.active = true
        ORDER BY e.name
    """, "Inner join employees ↔ departments")

    run(db, conn, """
        SELECT d.dept_name, COUNT(*) AS headcount
        FROM departments d
        JOIN employees e ON d.dept_id = e.dept_id
        GROUP BY d.dept_name
        ORDER BY headcount DESC
    """, "Department headcounts via join")

    # ── UPDATE ───────────────────────────────────────────────────────────────
    banner('6. UPDATE')
    run(db, conn, "SELECT id, name, salary FROM employees WHERE id = 1", "Before update")
    run(db, conn, "UPDATE employees SET salary = 99000.0 WHERE id = 1")
    run(db, conn, "SELECT id, name, salary FROM employees WHERE id = 1", "After update")

    # ── DELETE ───────────────────────────────────────────────────────────────
    banner('7. DELETE')
    run(db, conn, "SELECT COUNT(*) FROM employees", "Count before delete")
    run(db, conn, "DELETE FROM employees WHERE active = false")
    run(db, conn, "SELECT COUNT(*) FROM employees", "Count after delete")

    # ── INDEX ─────────────────────────────────────────────────────────────────
    banner('8. Index creation & index scan')
    run(db, conn, "CREATE INDEX idx_emp_salary ON employees (salary)")
    run(db, conn, """
        SELECT name, salary FROM employees WHERE salary >= 90000 ORDER BY salary DESC
    """, "Query using index scan on salary")

    # ── LIKE / IN / BETWEEN ──────────────────────────────────────────────────
    banner('9. LIKE, IN, BETWEEN')
    run(db, conn, "SELECT name FROM employees WHERE name LIKE 'A%'", "LIKE 'A%'")
    run(db, conn, "SELECT name, dept_id FROM employees WHERE dept_id IN (1, 3)", "IN list")
    run(db, conn, "SELECT name, salary FROM employees WHERE salary BETWEEN 70000 AND 100000",
        "BETWEEN")

    # ── TRANSACTIONS ─────────────────────────────────────────────────────────
    banner('10. Transactions — COMMIT & ROLLBACK')

    conn2 = db.connect()
    run(db, conn2, "BEGIN")
    run(db, conn2, "INSERT INTO employees VALUES (99, 'Temporary', 1, 50000, true, '2024-01-01')")
    run(db, conn2, "SELECT COUNT(*) FROM employees", "Count in txn (should include row 99)")
    run(db, conn2, "ROLLBACK")
    run(db, conn, "SELECT COUNT(*) FROM employees", "Count after rollback (row 99 gone)")

    conn3 = db.connect()
    run(db, conn3, "BEGIN")
    run(db, conn3, "INSERT INTO departments VALUES (4, 'Legal')")
    run(db, conn3, "COMMIT")
    run(db, conn, "SELECT * FROM departments ORDER BY dept_id", "After commit (Legal added)")

    # ── MVCC isolation ────────────────────────────────────────────────────────
    banner('11. MVCC — concurrent transaction isolation')
    conn_a = db.connect()
    conn_b = db.connect()

    run(db, conn_a, "BEGIN")
    run(db, conn_b, "BEGIN")
    # A inserts, B cannot see it until A commits
    run(db, conn_a, "INSERT INTO departments VALUES (5, 'R&D')")
    run(db, conn_b, "SELECT * FROM departments ORDER BY dept_id",
        "B's view — should NOT see R&D (A not committed)")
    run(db, conn_a, "COMMIT")
    run(db, conn_b, "SELECT * FROM departments ORDER BY dept_id",
        "B's view after A commits — should see R&D")
    run(db, conn_b, "COMMIT")

    # ── NULL handling ─────────────────────────────────────────────────────────
    banner('12. NULL handling')
    run(db, conn, "INSERT INTO employees (id, name, dept_id) VALUES (10, 'NoSalary', 2)")
    run(db, conn, "SELECT name, salary FROM employees WHERE salary IS NULL", "IS NULL")
    run(db, conn, "SELECT name, salary FROM employees WHERE salary IS NOT NULL ORDER BY salary",
        "IS NOT NULL")

    # ── DROP TABLE ────────────────────────────────────────────────────────────
    banner('13. DROP TABLE')
    run(db, conn, "DROP TABLE departments")
    run(db, conn, "DROP TABLE employees")

    # ── Done ─────────────────────────────────────────────────────────────────
    banner('Demo complete!')
    db.close()
    shutil.rmtree(DATA_DIR, ignore_errors=True)
    print('\nAll features demonstrated successfully.\n')


if __name__ == '__main__':
    main()
