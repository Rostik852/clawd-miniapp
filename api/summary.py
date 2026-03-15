import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

import psycopg2.extras

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, today_str


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
        params = parse_qs(urlparse(self.path).query)
        date = params.get('date', [today_str()])[0]

        try:
            conn = get_conn()
            ensure_tables(conn)

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
                """, (date,))
                row = cur.fetchone()

            conn.close()

            result = {
                "date": date,
                "cash_income": float(row['cash_income']),
                "card_income": float(row['card_income']),
                "coffee_portions": int(row['coffee_portions']),
                "cash_deposit": float(row['cash_deposit']),
                "cash_withdrawal": float(row['cash_withdrawal']),
                "expenses": float(row['expenses']),
            }
            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
