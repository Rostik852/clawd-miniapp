import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timezone

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, get_user, today_str, ADMIN_ID, verify_tg_signature


def _json_response(handler_obj, status, data):
    body = json.dumps(data).encode()
    handler_obj.send_response(status)
    handler_obj.send_header('Content-Type', 'application/json')
    add_cors(handler_obj)
    handler_obj.end_headers()
    handler_obj.wfile.write(body)


# Frontend field name → DB column name
FIELD_MAP = {
    'cash_income':   'cash_income',
    'card_income':   'card_income',
    'coffee_count':  'coffee_portions',
    'deposits':      'cash_deposit',
    'withdrawals':   'cash_withdrawal',
    'expenses':      'expenses',
}


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_POST(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except (ValueError, json.JSONDecodeError):
            _json_response(self, 400, {"error": "Invalid JSON body"})
            return

        user_id = body.get('user_id')
        field = body.get('field')
        value = body.get('value')
        notes = body.get('notes', None)
        date = body.get('date', today_str())
        event_time = body.get('time')
        init_data = body.get('init_data', '')

        # Validate required fields
        if not user_id:
            _json_response(self, 400, {"error": "user_id required"})
            return
        if not field:
            _json_response(self, 400, {"error": "field required"})
            return
        if value is None:
            _json_response(self, 400, {"error": "value required"})
            return

        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            _json_response(self, 400, {"error": "Invalid user_id"})
            return

        db_column = FIELD_MAP.get(field)
        if not db_column:
            _json_response(self, 400, {"error": f"Unknown field: {field}. Valid: {list(FIELD_MAP.keys())}"})
            return

        # Validate value type
        try:
            if db_column == 'coffee_portions':
                value = int(value)
            else:
                value = float(value)
        except (ValueError, TypeError):
            _json_response(self, 400, {"error": f"Invalid value for field {field}: must be numeric"})
            return

        # Verify Telegram init_data signature if provided and not admin
        if init_data and user_id != ADMIN_ID:
            bot_token = os.environ.get('BOT_TOKEN', '')
            if not verify_tg_signature(init_data, bot_token):
                _json_response(self, 403, {"error": "Invalid Telegram signature"})
                return

        try:
            conn = get_conn()
            ensure_tables(conn)

            # Check user is approved (admin always ok)
            if user_id != ADMIN_ID:
                user = get_user(conn, user_id)
                if not user:
                    conn.close()
                    _json_response(self, 403, {"error": "User not found"})
                    return
                if not user['is_approved']:
                    conn.close()
                    _json_response(self, 403, {"error": "User not approved"})
                    return

            created_at = datetime.now(timezone.utc).isoformat()

            with conn.cursor() as cur:
                # Use parameterized column name safely (validated against FIELD_MAP above)
                sql = f"""
                    INSERT INTO records (user_id, date, event_time, {db_column}, notes, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cur.execute(sql, (user_id, date, event_time, value, notes, created_at))
                record_id = cur.fetchone()[0]
                conn.commit()

            conn.close()
            _json_response(self, 200, {
                "status": "ok",
                "id": record_id,
                "field": field,
                "db_column": db_column,
                "value": value,
                "date": date,
                "time": event_time,
            })

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
