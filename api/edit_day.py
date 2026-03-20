import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import json
from http.server import BaseHTTPRequestHandler
from _db import get_conn, ensure_tables, get_user, is_admin, get_or_create_session, ADMIN_ID
from _cors import add_cors, handle_options

class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def _json(self, status, data):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        add_cors(self)
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get('Content-Length', 0))
        try:
            body = json.loads(self.rfile.read(length))
        except:
            self._json(400, {"error": "invalid json"})
            return

        user_id = body.get('user_id')
        date_str = body.get('date')

        if not user_id or not date_str:
            self._json(400, {"error": "user_id and date required"})
            return

        conn = get_conn()
        try:
            ensure_tables(conn)
            uid = int(user_id)

            # Only admins can edit
            if not is_admin(conn, uid):
                self._json(403, {"error": "forbidden"})
                return

            # Get or create session for that date
            get_or_create_session(conn, date_str)

            # Build update fields
            allowed = {'opening_cash', 'closing_cash', 'coffee_portions', 'card_income'}
            updates = {k: v for k, v in body.items() if k in allowed}

            if not updates:
                self._json(400, {"error": "no fields to update"})
                return

            # Update daily_session
            set_clause = ', '.join(f"{k} = %s" for k in updates)
            vals = list(updates.values()) + [date_str]

            with conn.cursor() as cur:
                cur.execute(f"UPDATE daily_session SET {set_clause} WHERE date = %s", vals)
            conn.commit()

            self._json(200, {"ok": True, "date": date_str, "updated": list(updates.keys())})
        except Exception as e:
            self._json(500, {"error": str(e)})
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        pass
