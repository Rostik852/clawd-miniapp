"""
Analytics API — Vercel Serverless Function
GET /api/analytics?user_id=...&period=day|week|month&date=YYYY-MM-DD
Admin-only endpoint returning period summary data.
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import json

from _db import get_conn, ensure_tables, is_admin, get_period_summary
from _cors import add_cors, handle_options
from datetime import date as date_cls


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        user_id_str = params.get('user_id', [None])[0]
        period = params.get('period', ['week'])[0]
        ref_date = params.get('date', [date_cls.today().isoformat()])[0]

        if period not in ('day', 'week', 'month'):
            period = 'week'

        conn = get_conn()
        try:
            ensure_tables(conn)

            # Validate and check admin access
            if not user_id_str:
                self._send_json(403, {'error': 'forbidden'})
                return

            try:
                user_id = int(user_id_str)
            except ValueError:
                self._send_json(400, {'error': 'invalid user_id'})
                return

            if not is_admin(conn, user_id):
                self._send_json(403, {'error': 'forbidden'})
                return

            data = get_period_summary(conn, period, ref_date)
            self._send_json(200, data)

        except Exception as e:
            self._send_json(500, {'error': str(e)})
        finally:
            conn.close()

    def _send_json(self, status, payload):
        body = json.dumps(payload, default=str).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        add_cors(self)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass
