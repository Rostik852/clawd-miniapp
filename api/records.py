import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs

import psycopg2.extras

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, today_str, get_user, ADMIN_ID


def _json_response(handler_obj, status, data):
    body = json.dumps(data).encode()
    handler_obj.send_response(status)
    handler_obj.send_header('Content-Type', 'application/json')
    add_cors(handler_obj)
    handler_obj.end_headers()
    handler_obj.wfile.write(body)


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def _json_error(self, status, msg):
        body = json.dumps({"error": msg}).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)

        user_id_str = params.get('user_id', [None])[0]
        if not user_id_str:
            self._json_error(401, "user_id required")
            return
        try:
            uid = int(user_id_str)
        except (ValueError, TypeError):
            self._json_error(400, "invalid user_id")
            return

        try:
            limit = int(params.get('limit', ['20'])[0])
            limit = min(limit, 100)  # cap at 100
        except ValueError:
            limit = 20

        date = params.get('date', [today_str()])[0]

        try:
            conn = get_conn()
            ensure_tables(conn)

            # Admin always allowed; others must be approved
            if uid != ADMIN_ID:
                user = get_user(conn, uid)
                if not user or not user.get('is_approved'):
                    conn.close()
                    self._json_error(403, "forbidden")
                    return

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        r.id,
                        r.user_id,
                        r.date,
                        r.cash_income,
                        r.card_income,
                        r.coffee_portions,
                        r.cash_deposit,
                        r.cash_withdrawal,
                        r.expenses,
                        r.notes,
                        r.created_at,
                        u.username,
                        u.first_name,
                        u.last_name,
                        u.role
                    FROM records r
                    LEFT JOIN users u ON r.user_id = u.id
                    WHERE r.date = %s
                    ORDER BY r.created_at DESC
                    LIMIT %s
                """, (date, limit))
                rows = cur.fetchall()

            conn.close()

            result = [
                {
                    "id": row['id'],
                    "user_id": row['user_id'],
                    "date": row['date'],
                    "cash_income": float(row['cash_income'] or 0),
                    "card_income": float(row['card_income'] or 0),
                    "coffee_portions": int(row['coffee_portions'] or 0),
                    "cash_deposit": float(row['cash_deposit'] or 0),
                    "cash_withdrawal": float(row['cash_withdrawal'] or 0),
                    "expenses": float(row['expenses'] or 0),
                    "notes": row['notes'],
                    "created_at": row['created_at'],
                    "username": row['username'],
                    "first_name": row['first_name'],
                    "last_name": row['last_name'],
                    "role": row['role'],
                }
                for row in rows
            ]
            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
