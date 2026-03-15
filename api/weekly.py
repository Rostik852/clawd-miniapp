import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from datetime import datetime, timedelta, timezone

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
        try:
            conn = get_conn()
            ensure_tables(conn)

            today = datetime.now(timezone.utc).date()
            dates = [(today - timedelta(days=i)).isoformat() for i in range(6, -1, -1)]
            start_date = dates[0]
            end_date = dates[-1]

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT
                        date,
                        COALESCE(SUM(cash_income), 0)     AS cash_income,
                        COALESCE(SUM(card_income), 0)     AS card_income,
                        COALESCE(SUM(coffee_portions), 0) AS coffee_portions,
                        COALESCE(SUM(expenses), 0)        AS expenses
                    FROM records
                    WHERE date >= %s AND date <= %s
                    GROUP BY date
                    ORDER BY date ASC
                """, (start_date, end_date))
                rows = cur.fetchall()

            conn.close()

            # Index by date so we can fill in missing days with zeros
            by_date = {row['date']: row for row in rows}

            result = []
            for d in dates:
                if d in by_date:
                    row = by_date[d]
                    result.append({
                        "date": d,
                        "cash_income": float(row['cash_income']),
                        "card_income": float(row['card_income']),
                        "coffee_portions": int(row['coffee_portions']),
                        "expenses": float(row['expenses']),
                    })
                else:
                    result.append({
                        "date": d,
                        "cash_income": 0.0,
                        "card_income": 0.0,
                        "coffee_portions": 0,
                        "expenses": 0.0,
                    })

            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
