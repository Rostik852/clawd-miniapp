import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone

MODULES = ['cash_income', 'card_income', 'coffee_count', 'deposits', 'withdrawals', 'expenses', 'reports']
ADMIN_ID = 199897236


def get_conn():
    return psycopg2.connect(os.environ['DATABASE_URL'], sslmode='require')


def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                role TEXT DEFAULT NULL,
                is_approved INTEGER DEFAULT 0,
                joined_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS module_access (
                user_id BIGINT,
                module TEXT,
                enabled INTEGER DEFAULT 1,
                PRIMARY KEY (user_id, module)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS records (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                date TEXT,
                cash_income REAL DEFAULT 0,
                card_income REAL DEFAULT 0,
                coffee_portions INTEGER DEFAULT 0,
                cash_deposit REAL DEFAULT 0,
                cash_withdrawal REAL DEFAULT 0,
                expenses REAL DEFAULT 0,
                notes TEXT,
                created_at TEXT
            )
        """)
        conn.commit()


def get_user(conn, user_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
        return cur.fetchone()


def get_user_modules(conn, user_id):
    """Return dict of module_name -> bool for the given user.
    Admin always gets all modules enabled."""
    if int(user_id) == ADMIN_ID:
        return {m: True for m in MODULES}

    with conn.cursor() as cur:
        cur.execute(
            "SELECT module, enabled FROM module_access WHERE user_id = %s",
            (user_id,)
        )
        rows = cur.fetchall()

    if rows:
        db_modules = {row[0]: bool(row[1]) for row in rows}
        # Fill missing modules as False
        return {m: db_modules.get(m, False) for m in MODULES}

    # No rows → default: all enabled for approved users
    return {m: True for m in MODULES}


def today_str():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')
