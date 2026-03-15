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


# ── New functions for webhook/serverless use ──────────────────────────────────

def save_user_raw(conn, user_id, username, first_name, last_name):
    """Upsert user record (no approval change)."""
    now = datetime.now(timezone.utc).isoformat()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (id, username, first_name, last_name, joined_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name
        """, (user_id, username, first_name, last_name, now))
    conn.commit()


def add_record(conn, user_id, date_str, **fields):
    """Insert a new cash-flow record. Accepts keyword args matching columns."""
    allowed = {
        'cash_income', 'card_income', 'coffee_portions',
        'cash_deposit', 'cash_withdrawal', 'expenses', 'notes'
    }
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    now = datetime.now(timezone.utc).isoformat()
    cols = ', '.join(['user_id', 'date', 'created_at'] + list(clean.keys()))
    placeholders = ', '.join(['%s'] * (3 + len(clean)))
    values = [user_id, date_str, now] + list(clean.values())
    with conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO records ({cols}) VALUES ({placeholders})",
            values
        )
    conn.commit()


def get_summary_day(conn, date_str):
    """Return aggregated totals for a given date."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COALESCE(SUM(cash_income), 0)      AS cash_income,
                COALESCE(SUM(card_income), 0)      AS card_income,
                COALESCE(SUM(coffee_portions), 0)  AS coffee_portions,
                COALESCE(SUM(cash_deposit), 0)     AS cash_deposit,
                COALESCE(SUM(cash_withdrawal), 0)  AS cash_withdrawal,
                COALESCE(SUM(expenses), 0)         AS expenses
            FROM records
            WHERE date = %s
        """, (date_str,))
        return dict(cur.fetchone())


def set_role(conn, user_id, role):
    """Set role and approve the user."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET role = %s, is_approved = 1 WHERE id = %s",
            (role, user_id)
        )
    conn.commit()


def revoke_access(conn, user_id):
    """Remove role and revoke approval."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET role = NULL, is_approved = 0 WHERE id = %s",
            (user_id,)
        )
    conn.commit()


def set_module_access(conn, user_id, module, enabled):
    """Enable or disable a module for a user."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO module_access (user_id, module, enabled)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id, module) DO UPDATE SET enabled = EXCLUDED.enabled
        """, (user_id, module, 1 if enabled else 0))
    conn.commit()


def get_all_users(conn):
    """Return list of all users as dicts."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM users ORDER BY joined_at DESC")
        return [dict(r) for r in cur.fetchall()]


def get_pending_users(conn):
    """Return users who are not yet approved."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM users WHERE is_approved = 0 ORDER BY joined_at DESC"
        )
        return [dict(r) for r in cur.fetchall()]
