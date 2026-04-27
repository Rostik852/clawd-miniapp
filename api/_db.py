import os
import psycopg2
import psycopg2.extras
from datetime import datetime, timedelta, timezone

MODULES = ['cash_income', 'card_income', 'coffee_count', 'deposits', 'withdrawals', 'expenses', 'reports', 'shifts']
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
                event_time TEXT DEFAULT NULL,
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
                closed_time TEXT DEFAULT NULL,
                notes TEXT DEFAULT NULL,
                avg_price_cash REAL DEFAULT NULL,
                avg_price_total REAL DEFAULT NULL
            )
        """)
        # Notification settings
        cur.execute("""
            CREATE TABLE IF NOT EXISTS notification_settings (
                user_id BIGINT PRIMARY KEY,
                on_snapshot INTEGER DEFAULT 0,
                on_close_day INTEGER DEFAULT 0,
                on_swap_request INTEGER DEFAULT 0,
                remind_snapshot INTEGER DEFAULT 0,
                notify_shift_assigned INTEGER DEFAULT 0,
                on_shift_assigned INTEGER DEFAULT 0,
                updated_at TEXT DEFAULT (NOW() AT TIME ZONE 'UTC')
            )
        """)
        # Shifts schedule
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shifts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                date TEXT NOT NULL,
                shift_num INTEGER NOT NULL DEFAULT 1,
                time_start TEXT DEFAULT NULL,
                time_end TEXT DEFAULT NULL,
                notes TEXT DEFAULT NULL,
                created_by BIGINT,
                created_at TEXT DEFAULT (NOW() AT TIME ZONE 'UTC'),
                UNIQUE(date, shift_num)
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS shift_swaps (
                id SERIAL PRIMARY KEY,
                requester_id BIGINT NOT NULL,
                target_id BIGINT NOT NULL,
                date TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                notes TEXT,
                responded_at TEXT,
                created_at TEXT DEFAULT (NOW() AT TIME ZONE 'UTC')
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS day_off_requests (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                date TEXT NOT NULL,
                notes TEXT DEFAULT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                source TEXT NOT NULL DEFAULT 'request',
                requested_by BIGINT DEFAULT NULL,
                responded_by BIGINT DEFAULT NULL,
                responded_at TEXT DEFAULT NULL,
                created_at TEXT DEFAULT (NOW() AT TIME ZONE 'UTC'),
                UNIQUE(user_id, date)
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
        # Migrate: add columns if missing
        for table, col, typ in [
            ('daily_session', 'avg_price_cash', 'REAL'),
            ('daily_session', 'avg_price_total', 'REAL'),
            ('daily_session', 'closed_time', 'TEXT'),
            ('records', 'event_time', 'TEXT'),
            ('notification_settings', 'on_shift_assigned', 'INTEGER DEFAULT 0'),
        ]:
            try:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {typ}")
            except Exception:
                pass
        for col in ['on_snapshot', 'on_close_day', 'on_swap_request', 'remind_snapshot', 'notify_shift_assigned', 'on_shift_assigned']:
            try:
                cur.execute(f"ALTER TABLE notification_settings ALTER COLUMN {col} SET DEFAULT 0")
            except Exception:
                pass
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


def get_bot_state(conn, user_id):
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM bot_state WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
    return dict(row) if row else None


def set_bot_state(conn, user_id, state, data=None):
    now = datetime.now(timezone.utc).isoformat()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bot_state (user_id, state, data, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                state = EXCLUDED.state,
                data = EXCLUDED.data,
                updated_at = EXCLUDED.updated_at
        """, (user_id, state, data, now))
    conn.commit()


def _time_to_minutes(time_str):
    if not time_str:
        return None
    try:
        hh, mm = str(time_str).split(':')[:2]
        return int(hh) * 60 + int(mm)
    except Exception:
        return None


def _minutes_to_label(minutes):
    if minutes is None:
        return ''
    minutes = max(0, min(int(minutes), 23 * 60 + 59))
    return f'{minutes // 60:02d}:{minutes % 60:02d}'


def _event_minutes(event):
    mins = _time_to_minutes(event.get('time'))
    if mins is not None:
        return mins
    created_at = event.get('created_at') or ''
    if len(created_at) >= 16:
        return _time_to_minutes(created_at[11:16])
    return None


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
        'event_time', 'cash_income', 'card_income', 'coffee_portions',
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


def get_summary_day(conn, date_str, cutoff_time=None, after_cutoff=False):
    """Return aggregated totals from records table for a given date.

    When cutoff_time is provided, records are split by local event_time:
    - after_cutoff=False  -> records at or before cutoff_time
    - after_cutoff=True   -> records after cutoff_time
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        sql = """
            SELECT
                COALESCE(SUM(cash_income), 0)      AS cash_income,
                COALESCE(SUM(card_income), 0)      AS card_income,
                COALESCE(SUM(coffee_portions), 0)  AS coffee_portions,
                COALESCE(SUM(cash_deposit), 0)     AS cash_deposit,
                COALESCE(SUM(cash_withdrawal), 0)  AS cash_withdrawal,
                COALESCE(SUM(expenses), 0)         AS expenses
            FROM records
            WHERE date = %s
        """
        params = [date_str]
        if cutoff_time:
            comparator = '>' if after_cutoff else '<='
            sql += f" AND COALESCE(event_time, '00:00') {comparator} %s"
            params.append(cutoff_time)
        cur.execute(sql, params)
        return dict(cur.fetchone())


def get_carryover_cash(conn, date_str):
    """Compute opening cash for date_str from the previous day's finalized closing.

    Carryover = previous closing cash
                - withdrawals to safe
                + deposits from safe
                - expenses
    where post-close operations from the previous day are applied.
    """
    previous_day = (
        datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
    ).strftime('%Y-%m-%d')
    previous_session = get_session(conn, previous_day)
    if previous_session:
        if previous_session.get('closing_cash') is not None:
            carryover = float(previous_session.get('closing_cash') or 0)
            closed_time = previous_session.get('closed_time')
            if closed_time:
                post_close_ops = get_summary_day(conn, previous_day, cutoff_time=closed_time, after_cutoff=True)
                carryover = round(
                    carryover
                    - float(post_close_ops.get('cash_withdrawal') or 0)
                    + float(post_close_ops.get('cash_deposit') or 0)
                    - float(post_close_ops.get('expenses') or 0),
                    2
                )
            return carryover

    snapshots = get_snapshots(conn, previous_day)
    last_with_cash = next((s for s in reversed(snapshots) if s.get('cash_amount') is not None), None)
    if last_with_cash:
        return round(float(last_with_cash.get('cash_amount') or 0), 2)
    return 0.0


def apply_snapshot_auto_close(conn, date_str):
    """Use the last snapshot of the day as an automatic closing reference.

    This does not formally finalize the day. It only persists the latest known
    cash/time/coffee into daily_session so the next day can inherit a stable
    opening cash value, while still allowing admins to manually close or edit
    the previous day later.
    """
    snapshots = get_snapshots(conn, date_str)
    if not snapshots:
        return None

    last_with_cash = next((s for s in reversed(snapshots) if s.get('cash_amount') is not None), None)
    last_with_coffee = next((s for s in reversed(snapshots) if s.get('coffee_portions') is not None), None)
    if not last_with_cash and not last_with_coffee:
        return None

    session = get_session(conn, date_str)
    if not session:
        opening_cash = get_carryover_cash(conn, date_str)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO daily_session (date, opening_cash)
                VALUES (%s, %s)
                ON CONFLICT (date) DO NOTHING
            """, (date_str, opening_cash))
        conn.commit()
        session = get_session(conn, date_str)

    updates = {}
    if last_with_cash:
        last_cash = round(float(last_with_cash.get('cash_amount') or 0), 2)
        last_time = last_with_cash.get('time')
        if float(session.get('closing_cash') or 0) != last_cash or session.get('closing_cash') is None:
            updates['closing_cash'] = last_cash
        if last_time and session.get('closed_time') != last_time:
            updates['closed_time'] = last_time

    if last_with_coffee:
        last_coffee = int(last_with_coffee.get('coffee_portions') or 0)
        if int(session.get('coffee_portions') or 0) != last_coffee:
            updates['coffee_portions'] = last_coffee

    if updates:
        update_session(conn, date_str, **updates)
        session = get_session(conn, date_str)
    return session


def session_has_activity(conn, date_str):
    """Return True if the session date already has local activity."""
    session = get_session(conn, date_str)
    if session and any([
        float(session.get('closing_cash') or 0) != 0,
        int(session.get('coffee_portions') or 0) != 0,
        float(session.get('card_income') or 0) != 0,
        bool(session.get('is_finalized')),
    ]):
        return True

    if get_snapshots(conn, date_str):
        return True

    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*)
            FROM records
            WHERE date = %s
              AND (
                COALESCE(cash_income, 0) <> 0 OR
                COALESCE(card_income, 0) <> 0 OR
                COALESCE(coffee_portions, 0) <> 0 OR
                COALESCE(cash_deposit, 0) <> 0 OR
                COALESCE(cash_withdrawal, 0) <> 0 OR
                COALESCE(expenses, 0) <> 0
              )
        """, (date_str,))
        return (cur.fetchone()[0] or 0) > 0


def get_effective_summary_day(conn, date_str, session=None):
    """Return totals that belong to the operational day only."""
    if session is None:
        session = get_session(conn, date_str)
    cutoff_time = None
    if session and session.get('is_finalized') and session.get('closed_time'):
        cutoff_time = session.get('closed_time')
    return get_summary_day(conn, date_str, cutoff_time=cutoff_time)


def get_safe_balance(conn, date_str):
    """Return cumulative safe balance through the end of the given date."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT
                COALESCE(SUM(cash_withdrawal), 0) - COALESCE(SUM(cash_deposit), 0) AS safe_balance
            FROM records
            WHERE date <= %s
        """, (date_str,))
        row = cur.fetchone()
    return round(float(row[0] or 0), 2)


DEFAULT_MODULES = {
    'cash_income':  True,
    'card_income':  True,
    'coffee_count': True,
    'deposits':     True,
    'withdrawals':  True,
    'expenses':     True,
    'reports':      False,
    'shifts':       False,
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
    """Get today's session or create with opening cash from the previous day.

    Before opening a new day, the previous day's last snapshot is persisted as
    an automatic closing reference. That value becomes the carryover cash for
    the next day until a manual close overrides it.
    """
    previous_day = (
        datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=1)
    ).strftime('%Y-%m-%d')
    apply_snapshot_auto_close(conn, previous_day)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM daily_session WHERE date = %s", (date_str,))
        session = cur.fetchone()
        if session:
            session_dict = dict(session)
            if not session_dict.get('is_finalized') and not session_has_activity(conn, date_str):
                opening_cash = get_carryover_cash(conn, date_str)
                if float(session_dict.get('opening_cash') or 0) != float(opening_cash or 0):
                    cur.execute(
                        "UPDATE daily_session SET opening_cash = %s WHERE date = %s",
                        (opening_cash, date_str)
                    )
                    conn.commit()
                    session_dict['opening_cash'] = opening_cash
            return session_dict

        opening_cash = get_carryover_cash(conn, date_str)

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
        'is_finalized', 'closed_by', 'closed_at', 'closed_time', 'notes',
        'avg_price_cash', 'avg_price_total'
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
    """Get all snapshots for date ordered by time, including worker name."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT s.*, u.first_name, u.last_name, u.username, u.role
            FROM snapshots s
            LEFT JOIN users u ON s.user_id = u.id
            WHERE s.date = %s ORDER BY s.time ASC
        """, (date_str,))
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            # Build display name
            name_parts = [r['first_name'] or '', r['last_name'] or '']
            d['worker_name'] = ' '.join(p for p in name_parts if p).strip() or r['username'] or f"#{r['user_id']}"
            rows.append(d)
        return rows


def delete_snapshot(conn, snapshot_id, user_id=None):
    """Delete a snapshot by ID. If user_id provided, verify ownership (admins can delete any)."""
    with conn.cursor() as cur:
        if user_id and int(user_id) != ADMIN_ID:
            cur.execute("DELETE FROM snapshots WHERE id = %s AND user_id = %s", (snapshot_id, user_id))
        else:
            cur.execute("DELETE FROM snapshots WHERE id = %s", (snapshot_id,))
    conn.commit()


def update_snapshot(conn, snapshot_id, cash_amount=None, coffee_portions=None, notes=None):
    """Update a snapshot's values."""
    fields = []
    vals = []
    if cash_amount is not None:
        fields.append("cash_amount = %s"); vals.append(cash_amount)
    if coffee_portions is not None:
        fields.append("coffee_portions = %s"); vals.append(coffee_portions)
    if notes is not None:
        fields.append("notes = %s"); vals.append(notes)
    if not fields:
        return
    vals.append(snapshot_id)
    with conn.cursor() as cur:
        cur.execute(f"UPDATE snapshots SET {', '.join(fields)} WHERE id = %s", vals)
    conn.commit()


def delete_daily_session(conn, date_str):
    """Delete a daily session (admin only)."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM daily_session WHERE date = %s", (date_str,))
    conn.commit()


def update_daily_session_field(conn, date_str, **fields):
    """Update specific fields of a daily session."""
    allowed = {'opening_cash', 'closing_cash', 'coffee_portions', 'card_income', 'is_finalized', 'notes'}
    updates = {k: v for k, v in fields.items() if k in allowed}
    if not updates:
        return
    set_clause = ', '.join(f"{k} = %s" for k in updates)
    vals = list(updates.values()) + [date_str]
    with conn.cursor() as cur:
        cur.execute(f"UPDATE daily_session SET {set_clause} WHERE date = %s", vals)
    conn.commit()


def get_record_operations(conn, date_str):
    """Return normalized record-based operations for the given date."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                r.id,
                r.user_id,
                r.date,
                r.event_time,
                r.cash_income,
                r.card_income,
                r.coffee_portions,
                r.cash_deposit,
                r.cash_withdrawal,
                r.expenses,
                r.notes,
                r.created_at,
                u.first_name,
                u.last_name,
                u.username,
                u.role
            FROM records r
            LEFT JOIN users u ON u.id = r.user_id
            WHERE r.date = %s
            ORDER BY COALESCE(r.event_time, SUBSTRING(r.created_at, 12, 5)) ASC, r.created_at ASC, r.id ASC
        """, (date_str,))
        rows = [dict(r) for r in cur.fetchall()]

    operations = []
    for row in rows:
        worker_name = (
            ((row.get('first_name') or '') + ' ' + (row.get('last_name') or '')).strip()
            or row.get('username')
            or f"#{row['user_id']}"
        )
        event_time = row.get('event_time') or ((row.get('created_at') or '')[11:16] if row.get('created_at') else '')
        amount_map = [
            ('cash_deposit', 'deposit', 'Вплата', row.get('cash_deposit')),
            ('cash_withdrawal', 'withdrawal', 'Виплата', row.get('cash_withdrawal')),
            ('expenses', 'expense', 'Витрата', row.get('expenses')),
            ('cash_income', 'cash_income', 'Готівка', row.get('cash_income')),
            ('card_income', 'card_income', 'Картка', row.get('card_income')),
        ]
        for field, op_type, label, raw_amount in amount_map:
            amount = float(raw_amount or 0)
            if amount == 0:
                continue
            operations.append({
                'id': f"record-{row['id']}-{field}",
                'source_id': row['id'],
                'type': op_type,
                'label': label,
                'time': event_time,
                'created_at': row.get('created_at'),
                'user_id': row['user_id'],
                'worker_name': worker_name,
                'amount': amount,
                'coffee_portions': None,
                'notes': row.get('notes'),
            })
        coffee_portions = row.get('coffee_portions')
        if coffee_portions:
            operations.append({
                'id': f"record-{row['id']}-coffee",
                'source_id': row['id'],
                'type': 'coffee_count',
                'label': 'Порції кави',
                'time': event_time,
                'created_at': row.get('created_at'),
                'user_id': row['user_id'],
                'worker_name': worker_name,
                'amount': None,
                'coffee_portions': int(coffee_portions),
                'notes': row.get('notes'),
            })
    return operations


def get_daily_activity(conn, date_str):
    """Return merged timeline for snapshots, cash ops and close-day action."""
    activity = []

    for snap in get_snapshots(conn, date_str):
        activity.append({
            'id': f"snapshot-{snap['id']}",
            'source_id': snap['id'],
            'type': 'snapshot',
            'label': 'Snapshot',
            'time': snap.get('time'),
            'created_at': snap.get('created_at'),
            'user_id': snap.get('user_id'),
            'worker_name': snap.get('worker_name'),
            'amount': float(snap['cash_amount']) if snap.get('cash_amount') is not None else None,
            'coffee_portions': int(snap['coffee_portions']) if snap.get('coffee_portions') is not None else None,
            'notes': snap.get('notes'),
        })

    activity.extend(get_record_operations(conn, date_str))

    session = get_session(conn, date_str)
    if session and session.get('is_finalized') and session.get('closing_cash') is not None:
        closer = get_user(conn, session.get('closed_by')) if session.get('closed_by') else None
        worker_name = 'Адмін'
        if closer:
            worker_name = (
                ((closer.get('first_name') or '') + ' ' + (closer.get('last_name') or '')).strip()
                or closer.get('username')
                or f"#{session.get('closed_by')}"
            )
        activity.append({
            'id': f"close-{date_str}",
            'source_id': date_str,
            'type': 'close_day',
            'label': 'Закриття дня',
            'time': session.get('closed_time') or ((session.get('closed_at') or '')[11:16] if session.get('closed_at') else ''),
            'created_at': session.get('closed_at'),
            'user_id': session.get('closed_by'),
            'worker_name': worker_name,
            'amount': float(session.get('closing_cash') or 0),
            'coffee_portions': int(session.get('coffee_portions') or 0),
            'notes': session.get('notes'),
        })

    activity.sort(
        key=lambda item: (
            _event_minutes(item) if _event_minutes(item) is not None else -1,
            item.get('created_at') or '',
            item.get('id') or '',
        ),
        reverse=True,
    )
    return activity


def get_hourly_sales_series(conn, date_str, opening_cash, closing_cash=None, closed_time=None):
    """Estimate hourly cash revenue from cash measurements adjusted by admin operations."""
    measurements = []
    for snap in get_snapshots(conn, date_str):
        if snap.get('cash_amount') is None:
            continue
        measurements.append({
            'time': snap.get('time'),
            'created_at': snap.get('created_at'),
            'cash_amount': float(snap.get('cash_amount') or 0),
        })

    if closing_cash is not None:
        measurements.append({
            'time': closed_time,
            'created_at': None,
            'cash_amount': float(closing_cash),
        })

    measurements = [m for m in measurements if _event_minutes(m) is not None]
    measurements.sort(key=lambda item: (_event_minutes(item), item.get('created_at') or ''))
    if not measurements:
        return []

    record_ops = get_record_operations(conn, date_str)
    deposits_by_minute = {}
    withdrawals_by_minute = {}
    expenses_by_minute = {}
    for op in record_ops:
        mins = _event_minutes(op)
        if mins is None:
            continue
        if op['type'] == 'deposit':
            deposits_by_minute[mins] = deposits_by_minute.get(mins, 0) + float(op['amount'] or 0)
        elif op['type'] == 'withdrawal':
            withdrawals_by_minute[mins] = withdrawals_by_minute.get(mins, 0) + float(op['amount'] or 0)
        elif op['type'] == 'expense':
            expenses_by_minute[mins] = expenses_by_minute.get(mins, 0) + float(op['amount'] or 0)

    prev_cash = float(opening_cash or 0)
    prev_minutes = -1
    hourly = {}

    for point in measurements:
        point_minutes = _event_minutes(point)
        dep = sum(v for m, v in deposits_by_minute.items() if prev_minutes < m <= point_minutes)
        wit = sum(v for m, v in withdrawals_by_minute.items() if prev_minutes < m <= point_minutes)
        exp = sum(v for m, v in expenses_by_minute.items() if prev_minutes < m <= point_minutes)
        cash_delta = float(point['cash_amount']) - prev_cash
        sales_delta = round(cash_delta - dep + wit + exp, 2)
        hour_key = point_minutes // 60
        hourly[hour_key] = round(hourly.get(hour_key, 0) + sales_delta, 2)
        prev_cash = float(point['cash_amount'])
        prev_minutes = point_minutes

    if not hourly:
        return []

    start_hour = max(min(hourly.keys()) - 1, 6)
    end_hour = min(max(hourly.keys()) + 1, 23)
    return [
        {
            'hour': hour,
            'label': f'{hour:02d}:00',
            'cash_income': round(hourly.get(hour, 0), 2),
        }
        for hour in range(start_hour, end_hour + 1)
    ]


def get_avg_price_context(conn, date_str, cash_income, card_income, coffee_portions):
    """Return current and comparative average-price metrics."""
    if not coffee_portions:
        return None

    current_cash = round(float(cash_income) / coffee_portions, 2)
    current_total = round((float(cash_income) + float(card_income)) / coffee_portions, 2)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT
                ds.date,
                ds.opening_cash,
                ds.closing_cash,
                ds.card_income,
                ds.coffee_portions,
                COALESCE(SUM(r.cash_deposit), 0)    AS admin_deposits,
                COALESCE(SUM(r.cash_withdrawal), 0) AS admin_withdrawals,
                COALESCE(SUM(r.expenses), 0)        AS expenses
            FROM daily_session ds
            LEFT JOIN records r ON r.date = ds.date
            WHERE ds.date < %s
              AND ds.closing_cash IS NOT NULL
              AND COALESCE(ds.coffee_portions, 0) > 0
            GROUP BY ds.date, ds.opening_cash, ds.closing_cash, ds.card_income, ds.coffee_portions
            ORDER BY ds.date DESC
            LIMIT 14
        """, (date_str,))
        history_rows = [dict(row) for row in cur.fetchall()]

    comparisons = []
    for row in history_rows:
        portions = int(row.get('coffee_portions') or 0)
        if portions <= 0:
            continue
        cash_val = round(
            float(row.get('closing_cash') or 0)
            - float(row.get('opening_cash') or 0)
            - float(row.get('admin_deposits') or 0)
            + float(row.get('admin_withdrawals') or 0)
            + float(row.get('expenses') or 0),
            2,
        )
        total_val = round(cash_val + float(row.get('card_income') or 0), 2)
        comparisons.append({
            'date': row['date'],
            'avg_price_cash': round(cash_val / portions, 2),
            'avg_price_total': round(total_val / portions, 2),
        })

    previous_day = comparisons[0] if comparisons else None
    trailing = comparisons[:7]
    trailing_cash = round(sum(item['avg_price_cash'] for item in trailing) / len(trailing), 2) if trailing else None
    trailing_total = round(sum(item['avg_price_total'] for item in trailing) / len(trailing), 2) if trailing else None

    return {
        'current_cash': current_cash,
        'current_total': current_total,
        'previous_day': previous_day,
        'vs_previous_cash': round(current_cash - previous_day['avg_price_cash'], 2) if previous_day else None,
        'vs_previous_total': round(current_total - previous_day['avg_price_total'], 2) if previous_day else None,
        'trailing_7_cash': trailing_cash,
        'trailing_7_total': trailing_total,
        'vs_trailing_7_cash': round(current_cash - trailing_cash, 2) if trailing_cash is not None else None,
        'vs_trailing_7_total': round(current_total - trailing_total, 2) if trailing_total is not None else None,
    }


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

    admin_row = get_effective_summary_day(conn, date_str, session)

    opening = float(session.get('opening_cash') or 0)
    closing = session.get('closing_cash')
    admin_deposits = float(admin_row.get('cash_deposit') or 0)
    admin_withdrawals = float(admin_row.get('cash_withdrawal') or 0)
    expenses = float(admin_row.get('expenses') or 0)

    snaps = get_snapshots(conn, date_str)

    # If no formal closing — use last snapshot cash as effective closing
    effective_closing = closing
    last_snap_cash = None
    if snaps:
        last_with_cash = next(
            (s for s in reversed(snaps) if s.get('cash_amount') is not None), None
        )
        if last_with_cash:
            last_snap_cash = float(last_with_cash['cash_amount'])
    if effective_closing is None and last_snap_cash is not None:
        effective_closing = last_snap_cash

    latest_coffee = next((int(s['coffee_portions']) for s in reversed(snaps) if s.get('coffee_portions') is not None), 0)
    coffee_portions = int(session.get('coffee_portions') or 0) or latest_coffee
    card_income = float(session.get('card_income') or 0)

    if effective_closing is not None:
        cash_income = float(effective_closing) - opening - admin_deposits + admin_withdrawals + expenses
        net_cash = float(effective_closing)
    else:
        cash_income = 0.0
        net_cash = 0.0

    hourly_sales = get_hourly_sales_series(
        conn,
        date_str,
        opening_cash=opening,
        closing_cash=closing,
        closed_time=session.get('closed_time'),
    )
    avg_price_context = get_avg_price_context(conn, date_str, cash_income, card_income, coffee_portions)
    activity_log = get_daily_activity(conn, date_str)
    safe_balance = get_safe_balance(conn, date_str)

    return {
        'date': date_str,
        'opening_cash': opening,
        'closing_cash': closing,
        'is_finalized': bool(session.get('is_finalized')),
        'closed_at': session.get('closed_at'),
        'closed_time': session.get('closed_time'),
        'cash_income': round(cash_income, 2),
        'card_income': card_income,
        'coffee_portions': coffee_portions,
        'admin_deposits': admin_deposits,
        'admin_withdrawals': admin_withdrawals,
        'expenses': expenses,
        'net_cash': round(net_cash, 2),
        'effective_closing': effective_closing,
        'closing_from_snapshot': closing is None and last_snap_cash is not None,
        'avg_price_cash': round(cash_income / coffee_portions, 2) if coffee_portions > 0 else None,
        'avg_price_total': round((cash_income + card_income) / coffee_portions, 2) if coffee_portions > 0 else None,
        'saved_avg_price_cash': session.get('avg_price_cash'),
        'saved_avg_price_total': session.get('avg_price_total'),
        'avg_price_context': avg_price_context,
        'snapshots': snaps,
        'hourly_sales': hourly_sales,
        'activity_log': activity_log,
        'safe_balance': safe_balance,
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
            return {'period': 'day', 'date': ref_date, 'rows': [], 'totals': {}, 'activity_log': []}
        summary_row = get_effective_summary_day(conn, ref_date, session)
        dep = float(summary_row.get('cash_deposit') or 0)
        wit = float(summary_row.get('cash_withdrawal') or 0)
        exp = float(summary_row.get('expenses') or 0)
        _closing = session.get('closing_cash')
        _opening = session.get('opening_cash') or 0
        if _closing is not None:
            cash_income = _closing - _opening - float(dep) + float(wit) + float(exp)
        else:
            cash_income = 0
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
            'closed_time': session.get('closed_time'),
        }
        if row['coffee_portions']:
            row['avg_price_cash'] = round(float(cash_income) / int(row['coffee_portions']), 2)
            row['avg_price_total'] = round((float(cash_income) + float(row['card_income'] or 0)) / int(row['coffee_portions']), 2)
        else:
            row['avg_price_cash'] = None
            row['avg_price_total'] = None
        snaps = get_snapshots(conn, ref_date)
        row['snapshots'] = [dict(s) for s in snaps]
        totals = _calc_totals([row])
        return {'period': 'day', 'date': ref_date, 'rows': [row], 'totals': totals, 'activity_log': get_daily_activity(conn, ref_date)}

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
    """Batch-fetch sessions and compute day-safe-aware summaries."""
    if not dates:
        return {'period': period, 'date': ref_date, 'rows': [], 'totals': _calc_totals([])}

    start_date, end_date = dates[0], dates[-1]

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT date, opening_cash, closing_cash, coffee_portions, card_income,
                   is_finalized, closed_at, closed_time
            FROM daily_session
            WHERE date >= %s AND date <= %s
        """, (start_date, end_date))
        sessions = {row['date']: dict(row) for row in cur.fetchall()}

    rows = []
    for d in dates:
        session = sessions.get(d)
        if not session:
            rows.append({
                'date': d, 'cash_income': 0, 'card_income': 0,
                'coffee_portions': 0, 'opening_cash': 0,
                'closing_cash': None, 'is_finalized': False,
                'admin_deposits': 0, 'admin_withdrawals': 0, 'expenses': 0,
                'avg_price_cash': None, 'avg_price_total': None,
            })
            continue
        ops = get_effective_summary_day(conn, d, session)
        admin_dep = float(ops.get('cash_deposit') or 0)
        admin_wit = float(ops.get('cash_withdrawal') or 0)
        exp = float(ops.get('expenses') or 0)
        _closing = session.get('closing_cash')
        _opening = float(session.get('opening_cash') or 0)
        # Correct formula: виручка = closing - opening - вплати + виплати + витрати
        if _closing is not None:
            cash_income = float(_closing) - _opening - admin_dep + admin_wit + exp
        else:
            cash_income = 0.0
        rows.append({
            'date': d,
            'cash_income': round(cash_income, 2),
            'card_income': float(session.get('card_income') or 0),
            'coffee_portions': int(session.get('coffee_portions') or 0),
            'opening_cash': _opening,
            'closing_cash': float(_closing) if _closing is not None else None,
            'is_finalized': bool(session.get('is_finalized')),
            'admin_deposits': admin_dep,
            'admin_withdrawals': admin_wit,
            'expenses': exp,
            'closed_at': session.get('closed_at'),
            'avg_price_cash': round(float(cash_income) / int(session.get('coffee_portions') or 0), 2) if int(session.get('coffee_portions') or 0) > 0 else None,
            'avg_price_total': round((float(cash_income) + float(session.get('card_income') or 0)) / int(session.get('coffee_portions') or 0), 2) if int(session.get('coffee_portions') or 0) > 0 else None,
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
            SELECT date, opening_cash, closing_cash, coffee_portions, card_income, is_finalized, closed_time
            FROM daily_session
            WHERE date >= %s AND date <= %s
            ORDER BY date ASC
        """, (start_date, end_date))
        rows = {row['date']: dict(row) for row in cur.fetchall()}

    result = []
    for d in dates:
        if d in rows:
            row = rows[d]
            ops = get_effective_summary_day(conn, d, row)
            opening = float(row.get('opening_cash') or 0)
            closing = row.get('closing_cash')
            admin_dep = float(ops.get('cash_deposit') or 0)
            admin_wit = float(ops.get('cash_withdrawal') or 0)
            exp = float(ops.get('expenses') or 0)
            # Correct formula: виручка = closing - opening - вплати + виплати + витрати
            if closing is not None:
                cash_income = float(closing) - opening - admin_dep + admin_wit + exp
            else:
                cash_income = 0.0
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


# ── Notifications ─────────────────────────────────────────────────────────────

def get_notification_settings(conn, user_id: int) -> dict:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM notification_settings WHERE user_id = %s", (user_id,))
        row = cur.fetchone()
        if row:
            return dict(row)
        # Defaults
        return {
            'user_id': user_id,
            'on_snapshot': 0,
            'on_close_day': 0,
            'on_swap_request': 0,
            'on_shift_assigned': 0,
            'remind_snapshot': 0,
            'notify_shift_assigned': 0,
        }


def set_notification_settings(conn, user_id: int, **fields):
    allowed = {'on_snapshot', 'on_close_day', 'on_swap_request', 'on_shift_assigned', 'remind_snapshot', 'notify_shift_assigned'}
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    now = datetime.now(timezone.utc).isoformat()
    cols = ', '.join(['user_id', 'updated_at'] + list(clean.keys()))
    vals = ', '.join(['%s'] * (2 + len(clean)))
    updates = ', '.join(f"{k} = EXCLUDED.{k}" for k in clean)
    with conn.cursor() as cur:
        cur.execute(f"""
            INSERT INTO notification_settings (user_id, updated_at, {', '.join(clean.keys())})
            VALUES (%s, %s, {', '.join(['%s']*len(clean))})
            ON CONFLICT (user_id) DO UPDATE SET {updates}, updated_at = EXCLUDED.updated_at
        """, [user_id, now] + list(clean.values()))
    conn.commit()


def get_all_admin_ids(conn) -> list:
    """Return all approved admin/super_admin user IDs."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM users
            WHERE is_approved = 1 AND role IN ('admin', 'super_admin')
        """)
        return [row[0] for row in cur.fetchall()] + [ADMIN_ID]


def send_tg_message(chat_id: int, text: str, bot_token: str = None):
    """Send Telegram message via Bot API."""
    import requests as req
    token = bot_token or os.environ.get('BOT_TOKEN', '')
    if not token:
        return
    try:
        req.post(
            f'https://api.telegram.org/bot{token}/sendMessage',
            json={'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'},
            timeout=5
        )
    except Exception:
        pass


def notify_admins(conn, text: str, setting_key: str = None):
    """Send notification to all admins who have the setting enabled."""
    token = os.environ.get('BOT_TOKEN', '')
    admin_ids = get_all_admin_ids(conn)
    for aid in set(admin_ids):
        if setting_key:
            s = get_notification_settings(conn, aid)
            if not s.get(setting_key, 1):
                continue
        send_tg_message(aid, text, token)


def notify_user(conn, user_id: int, text: str, setting_key: str = None):
    """Send notification to a specific user if setting allows."""
    token = os.environ.get('BOT_TOKEN', '')
    if setting_key:
        s = get_notification_settings(conn, user_id)
        if not s.get(setting_key, 1):
            return
    send_tg_message(user_id, text, token)


# ── Shifts schedule ───────────────────────────────────────────────────────────

def get_shifts_range(conn, date_from: str, date_to: str) -> list:
    """Get all shifts in date range with worker info."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT s.*, u.first_name, u.last_name, u.username, u.role
            FROM shifts s
            LEFT JOIN users u ON s.user_id = u.id
            WHERE s.date >= %s AND s.date <= %s
            ORDER BY s.date ASC, s.shift_num ASC
        """, (date_from, date_to))
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            name = ((r['first_name'] or '') + ' ' + (r['last_name'] or '')).strip()
            d['worker_name'] = name or r['username'] or f"#{r['user_id']}"
            rows.append(d)
        return rows


def set_shift(conn, date: str, shift_num: int, user_id: int, time_start: str = None,
              time_end: str = None, notes: str = None, created_by: int = None) -> dict:
    """Create or update a shift slot."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO shifts (date, shift_num, user_id, time_start, time_end, notes, created_by)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (date, shift_num) DO UPDATE SET
                user_id = EXCLUDED.user_id,
                time_start = EXCLUDED.time_start,
                time_end = EXCLUDED.time_end,
                notes = EXCLUDED.notes,
                created_by = EXCLUDED.created_by
            RETURNING *
        """, (date, shift_num, user_id, time_start, time_end, notes, created_by))
        conn.commit()
        return dict(cur.fetchone())


def delete_shift(conn, date: str, shift_num: int):
    """Remove a shift."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM shifts WHERE date = %s AND shift_num = %s", (date, shift_num))
    conn.commit()


def clear_worker_shifts(conn, date: str, user_id: int):
    """Remove all shifts for a worker on a specific date."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM shifts WHERE date = %s AND user_id = %s", (date, user_id))
    conn.commit()


def get_workers(conn) -> list:
    """Get all approved workers."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT id, first_name, last_name, username, role
            FROM users WHERE is_approved = 1
            ORDER BY first_name, last_name
        """)
        rows = []
        for r in cur.fetchall():
            d = dict(r)
            name = ((r['first_name'] or '') + ' ' + (r['last_name'] or '')).strip()
            d['display_name'] = name or r['username'] or f"#{r['id']}"
            rows.append(d)
        return rows


def get_day_off_requests_range(conn, date_from: str, date_to: str, user_id: int = None,
                               statuses=None) -> list:
    """Get day-off requests with worker info for a date range."""
    statuses = statuses or ['pending', 'approved']
    where = ["d.date >= %s", "d.date <= %s"]
    params = [date_from, date_to]
    if user_id is not None:
        where.append("d.user_id = %s")
        params.append(user_id)
    if statuses:
        where.append("d.status = ANY(%s)")
        params.append(statuses)

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                d.*,
                u.first_name,
                u.last_name,
                u.username,
                u.role
            FROM day_off_requests d
            LEFT JOIN users u ON d.user_id = u.id
            WHERE {' AND '.join(where)}
            ORDER BY d.date ASC, d.created_at ASC
        """, params)
        rows = []
        for r in cur.fetchall():
            row = dict(r)
            name = ((row.get('first_name') or '') + ' ' + (row.get('last_name') or '')).strip()
            row['worker_name'] = name or row.get('username') or f"#{row['user_id']}"
            rows.append(row)
        return rows


def upsert_day_off_request(conn, user_id: int, date: str, notes: str = None,
                           requested_by: int = None, status: str = 'pending',
                           source: str = 'request'):
    """Create or update a day-off request/approval record."""
    now = datetime.now(timezone.utc).isoformat()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO day_off_requests (user_id, date, notes, status, source, requested_by, responded_by, responded_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (user_id, date) DO UPDATE SET
                notes = EXCLUDED.notes,
                status = EXCLUDED.status,
                source = EXCLUDED.source,
                requested_by = EXCLUDED.requested_by,
                responded_by = EXCLUDED.responded_by,
                responded_at = EXCLUDED.responded_at
            RETURNING *
        """, (
            user_id, date, notes, status, source, requested_by,
            requested_by if status == 'approved' else None,
            now if status == 'approved' else None,
            now
        ))
        row = dict(cur.fetchone())
    conn.commit()
    return row


def respond_day_off_request(conn, request_id: int, status: str, responder_id: int):
    """Approve or reject a day-off request."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            UPDATE day_off_requests
            SET status = %s, responded_by = %s, responded_at = %s
            WHERE id = %s
            RETURNING *
        """, (status, responder_id, datetime.now(timezone.utc).isoformat(), request_id))
        row = cur.fetchone()
    conn.commit()
    return dict(row) if row else None


def delete_day_off_request(conn, user_id: int, date: str):
    """Remove day-off record for a worker/date."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM day_off_requests WHERE user_id = %s AND date = %s", (user_id, date))
    conn.commit()


def request_swap(conn, requester_id: int, target_id: int, date: str, notes: str = None):
    """Request a shift swap."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            INSERT INTO shift_swaps (requester_id, target_id, date, notes)
            VALUES (%s, %s, %s, %s) RETURNING *
        """, (requester_id, target_id, date, notes))
        conn.commit()
        return dict(cur.fetchone())


def respond_swap(conn, swap_id: int, user_id: int, accept: bool):
    """Accept or decline a swap request."""
    status = 'accepted' if accept else 'declined'
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            UPDATE shift_swaps SET status = %s, responded_at = %s
            WHERE id = %s AND target_id = %s RETURNING *
        """, (status, datetime.now(timezone.utc).isoformat(), swap_id, user_id))
        row = cur.fetchone()
        if row and accept:
            # Perform the actual swap
            d = dict(row)
            cur.execute("""
                UPDATE shifts SET user_id = %s
                WHERE date = %s AND user_id = %s
            """, (d['requester_id'], d['date'], d['target_id']))
            cur.execute("""
                UPDATE shifts SET user_id = %s
                WHERE date = %s AND user_id = %s
            """, (d['target_id'], d['date'], d['requester_id']))
        conn.commit()
        return dict(row) if row else None


def get_pending_swaps(conn, user_id: int) -> list:
    """Get pending swap requests for a user."""
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("""
            SELECT ss.*, 
                   u1.first_name AS req_first, u1.last_name AS req_last, u1.username AS req_username,
                   u2.first_name AS tgt_first, u2.last_name AS tgt_last
            FROM shift_swaps ss
            LEFT JOIN users u1 ON ss.requester_id = u1.id
            LEFT JOIN users u2 ON ss.target_id = u2.id
            WHERE (ss.requester_id = %s OR ss.target_id = %s)
              AND ss.status = 'pending'
            ORDER BY ss.created_at DESC
        """, (user_id, user_id))
        return [dict(r) for r in cur.fetchall()]
