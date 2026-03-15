import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, get_weekly_data, get_user, ADMIN_ID


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
            conn = get_conn()
            ensure_tables(conn)

            # Admin always allowed; others must be approved
            if uid != ADMIN_ID:
                user = get_user(conn, uid)
                if not user or not user.get('is_approved'):
                    conn.close()
                    self._json_error(403, "forbidden")
                    return

            result = get_weekly_data(conn)
            conn.close()
            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
