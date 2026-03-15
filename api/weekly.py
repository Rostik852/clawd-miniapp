import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, get_weekly_data


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

    def do_GET(self):
        try:
            conn = get_conn()
            ensure_tables(conn)
            result = get_weekly_data(conn)
            conn.close()
            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
