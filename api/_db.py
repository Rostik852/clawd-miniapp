import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone

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
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                user_id BIGINT PRIMARY KEY,
                state TEXT,
                data TEXT,
                updated_at TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS daily_session (
                id SERIAL PRIMARY KEY,
                date TEXT NOT NULL UNIQUE,
                opening_cash REAL DEFAULT 0,
                closing_cash REAL DEFAULT NULL,
                coffee_portions INTEGER DEFAULT 0,
                card_income REAL DEFAULT 0,
                is_finalized INTEGER DEFAULT 0,
                closed_by BIGINT DEFAULT NULL,
                closed_at TEXT DEFAULT NULL,
                notes TEXT DEFAULT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                date TEXT NOT NULL,
                time TEXT NOT NULL,
                cash_amount REAL DEFAULT NULL,
                coffee_portions INTEGER DEFAULT NULL,
                notes TEXT DEFAULT NULL,
                created_at TEXT NOT NULL
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
        return {m: db_modules.get(m, False) for m in MODULES}

    return {m: True for m in MODULES}


def today_str():
    return datetime.now(timezone.utc).strftime('%Y-%m-%d')


# ── User management ───────────────────────────────────────────────────────────

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
    """Insert a new cash-flow record (admin ops: deposits, withdrawals, expenses)."""
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
    """Return aggregated totals from records table for a given date (admin ops)."""
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


DEFAULT_MODULES = {
    'cash_income':  True,
    'card_income':  True,
    'coffee_count': True,
    'deposits':     True,
    'withdrawals':  True,
    'expenses':     True,
    'reports':      False,
}


def set_role(conn, user_id, role):
    """Set role, approve the user and apply default module permissions."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE users SET role = %s, is_approved = 1 WHERE id = %s",
            (role, user_id)
        )
        cur.execute("SELECT COUNT(*) FROM module_access WHERE user_id = %s", (user_id,))
        count = cur.fetchone()[0]
        if count == 0:
            for module, enabled in DEFAULT_MODULES.items():
                cur.execute("""
                    INSERT INTO module_access (user_id, module, enabled)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (user_id, module) DO NOTHING
                """, (user_id, module, 1 if enabled else 0))
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


# ── Daily session functions ───────────────────────────────────────────────────

def get_or_create_session(conn, date_str):
    """Get today's session or create with opening_cash from yesterday's closing."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM daily_session WHERE date = %s", (date_str,))
        session = cur.fetchone()
        if session:
            return dict(session)

        # Find yesterday's closing_cash
        yesterday = (
            datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
        ).strftime('%Y-%m-%d')
        cur.execute(
            "SELECT closing_cash FROM daily_session WHERE date = %s AND closing_cash IS NOT NULL",
            (yesterday,)
        )
        yesterday_row = cur.fetchone()
        opening_cash = yesterday_row['closing_cash'] if yesterday_row else 0

        cur.execute("""
            INSERT INTO daily_session (date, opening_cash)
            VALUES (%s, %s)
            ON CONFLICT (date) DO NOTHING
        """, (date_str, opening_cash))
        conn.commit()

        cur.execute("SELECT * FROM daily_session WHERE date = %s", (date_str,))
        new_session = cur.fetchone()
        return dict(new_session)


def update_session(conn, date_str, **fields):
    """Update daily_session fields for given date."""
    allowed = {
        'opening_cash', 'closing_cash', 'coffee_portions', 'card_income',
        'is_finalized', 'closed_by', 'closed_at', 'notes'
    }
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    sets = ', '.join(f"{k} = %s" for k in clean)
    values = list(clean.values()) + [date_str]
    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE daily_session SET {sets} WHERE date = %s",
            values
        )
    conn.commit()


def get_session(conn, date_str):
    """Get daily session dict or None."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM daily_session WHERE date = %s", (date_str,))
        row = cur.fetchone()
        return dict(row) if row else None


def add_snapshot(conn, user_id, date_str, time_str, cash_amount=None, coffee_portions=None, notes=None):
    """Insert a snapshot record."""
    now = datetime.now(timezone.utc).isoformat()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO snapshots (user_id, date, time, cash_amount, coffee_portions, notes, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (user_id, date_str, time_str, cash_amount, coffee_portions, notes, now))
    conn.commit()


def get_snapshots(conn, date_str):
    """Get all snapshots for date ordered by time."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT * FROM snapshots WHERE date = %s ORDER BY time ASC",
            (date_str,)
        )
        return [dict(r) for r in cur.fetchall()]


def get_daily_summary(conn, date_str):
    """
    Returns complete daily summary dict:
    {
      date, opening_cash, closing_cash, is_finalized,
      cash_income, card_income, coffee_portions,
      admin_deposits, admin_withdrawals, expenses,
      net_cash, snapshots
    }
    """
    session = get_session(conn, date_str)
    if not session:
        session = {
            'date': date_str,
            'opening_cash': 0,
            'closing_cash': None,
            'coffee_portions': 0,
            'card_income': 0,
            'is_finalized': 0,
            'closed_by': None,
            'closed_at': None,
            'notes': None,
        }

    # Admin ops from records table
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                COALESCE(SUM(cash_deposit), 0)     AS admin_deposits,
                COALESCE(SUM(cash_withdrawal), 0)  AS admin_withdrawals,
                COALESCE(SUM(expenses), 0)         AS expenses
            FROM records
            WHERE date = %s
        """, (date_str,))
        admin_row = dict(cur.fetchone())

    opening = session.get('opening_cash') or 0
    closing = session.get('closing_cash')
    admin_deposits = float(admin_row['admin_deposits'])
    admin_withdrawals = float(admin_row['admin_withdrawals'])
    expenses = float(admin_row['expenses'])

    if closing is not None:
        cash_income = closing - opening + admin_withdrawals - admin_deposits
        net_cash = closing
    else:
        cash_income = 0
        net_cash = 0

    snaps = get_snapshots(conn, date_str)

    return {
        'date': date_str,
        'opening_cash': opening,
        'closing_cash': closing,
        'is_finalized': bool(session.get('is_finalized')),
        'closed_at': session.get('closed_at'),
        'cash_income': round(cash_income, 2),
        'card_income': float(session.get('card_income') or 0),
        'coffee_portions': int(session.get('coffee_portions') or 0),
        'admin_deposits': admin_deposits,
        'admin_withdrawals': admin_withdrawals,
        'expenses': expenses,
        'net_cash': round(net_cash, 2),
        'snapshots': snaps,
    }


def is_admin(conn, user_id):
    """Returns True if user is super-admin (199897236) or has role='admin'."""
    if int(user_id) == ADMIN_ID:
        return True
    user = get_user(conn, user_id)
    return user is not None and user.get('role') == 'admin'


def get_period_summary(conn, period: str, ref_date: str) -> dict:
    """
    period: 'day' | 'week' | 'month'
    ref_date: 'YYYY-MM-DD'

    Returns aggregated data from daily_session for the period.
    For 'day': single day data with snapshots
    For 'week': last 7 days, list of daily rows + totals
    For 'month': current month days + totals
    """
    from datetime import datetime as _dt, timedelta as _td

    ref = _dt.strptime(ref_date, '%Y-%m-%d').date()

    if period == 'day':
        session = get_session(conn, ref_date)
        if not session:
            return {'period': 'day', 'date': ref_date, 'rows': [], 'totals': {}}
        cash_income = max(0, (session.get('closing_cash') or 0) - (session.get('opening_cash') or 0))
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(SUM(cash_deposit),0), COALESCE(SUM(cash_withdrawal),0), COALESCE(SUM(expenses),0)
                FROM records WHERE date = %s
            """, (ref_date,))
            dep, wit, exp = cur.fetchone()
        row = {
            'date': ref_date,
            'cash_income': cash_income,
            'card_income': session.get('card_income') or 0,
            'coffee_portions': session.get('coffee_portions') or 0,
            'opening_cash': session.get('opening_cash') or 0,
            'closing_cash': session.get('closing_cash'),
            'is_finalized': bool(session.get('is_finalized')),
            'admin_deposits': float(dep),
            'admin_withdrawals': float(wit),
            'expenses': float(exp),
            'closed_at': session.get('closed_at'),
        }
        snaps = get_snapshots(conn, ref_date)
        row['snapshots'] = [dict(s) for s in snaps]
        totals = _calc_totals([row])
        return {'period': 'day', 'date': ref_date, 'rows': [row], 'totals': totals}

    elif period == 'week':
        dates = [str(ref - _td(days=i)) for i in range(6, -1, -1)]
        return _get_rows_for_dates(conn, dates, 'week', ref_date)

    elif period == 'month':
        import calendar as _cal
        year, month = ref.year, ref.month
        days_in_month = _cal.monthrange(year, month)[1]
        dates = [f"{year}-{month:02d}-{d:02d}" for d in range(1, min(ref.day + 1, days_in_month + 1))]
        return _get_rows_for_dates(conn, dates, 'month', ref_date)

    return {'period': period, 'date': ref_date, 'rows': [], 'totals': {}}


def _get_rows_for_dates(conn, dates, period, ref_date):
    rows = []
    for d in dates:
        session = get_session(conn, d)
        if not session:
            rows.append({
                'date': d, 'cash_income': 0, 'card_income': 0,
                'coffee_portions': 0, 'opening_cash': 0,
                'closing_cash': None, 'is_finalized': False,
            })
            continue
        cash_income = max(0, (session.get('closing_cash') or 0) - (session.get('opening_cash') or 0))
        rows.append({
            'date': d,
            'cash_income': cash_income,
            'card_income': session.get('card_income') or 0,
            'coffee_portions': session.get('coffee_portions') or 0,
            'opening_cash': session.get('opening_cash') or 0,
            'closing_cash': session.get('closing_cash'),
            'is_finalized': bool(session.get('is_finalized')),
        })
    totals = _calc_totals(rows)
    return {'period': period, 'date': ref_date, 'rows': rows, 'totals': totals}


def _calc_totals(rows):
    total_cash = sum(r.get('cash_income', 0) for r in rows)
    total_card = sum(r.get('card_income', 0) for r in rows)
    total_coffee = sum(r.get('coffee_portions', 0) for r in rows)
    has_card = any(r.get('card_income', 0) > 0 for r in rows)
    return {
        'total_cash_income': total_cash,
        'total_card_income': total_card,
        'total_income': total_cash + total_card,
        'total_coffee': total_coffee,
        'has_card_data': has_card,
        'avg_price_cash': round(total_cash / total_coffee, 2) if total_coffee > 0 else None,
        'avg_price_total': round((total_cash + total_card) / total_coffee, 2) if total_coffee > 0 else None,
    }


def verify_tg_signature(init_data: str, bot_token: str) -> bool:
    """Verify Telegram WebApp initData HMAC-SHA256 signature."""
    import hmac, hashlib
    from urllib.parse import parse_qsl
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop('hash', None)
        if not received_hash:
            return False
        data_check_string = '\n'.join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b'WebAppData', bot_token.encode(), hashlib.sha256).digest()
        computed = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()
        return hmac.compare_digest(computed, received_hash)
    except Exception:
        return False


def get_weekly_data(conn):
    """Returns last 7 days of daily_session data for chart."""
    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
    start_date = dates[0]
    end_date = dates[-1]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT date, opening_cash, closing_cash, coffee_portions, card_income, is_finalized
            FROM daily_session
            WHERE date >= %s AND date <= %s
            ORDER BY date ASC
        """, (start_date, end_date))
        rows = {row['date']: dict(row) for row in cur.fetchall()}

    result = []
    for d in dates:
        if d in rows:
            row = rows[d]
            opening = float(row.get('opening_cash') or 0)
            closing = row.get('closing_cash')
            cash_income = (float(closing) - opening) if closing is not None else 0
            result.append({
                'date': d,
                'cash_income': round(cash_income, 2),
                'card_income': float(row.get('card_income') or 0),
                'coffee_portions': int(row.get('coffee_portions') or 0),
                'closing_cash': float(closing) if closing is not None else None,
                'is_finalized': bool(row.get('is_finalized')),
            })
        else:
            result.append({
                'date': d,
                'cash_income': 0.0,
                'card_income': 0.0,
                'coffee_portions': 0,
                'closing_cash': None,
                'is_finalized': False,
            })

    return result
