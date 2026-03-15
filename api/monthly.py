import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone

import psycopg2.extras

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables


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
        now = datetime.now(timezone.utc)
        year = int(params.get('year', [str(now.year)])[0])
        month = int(params.get('month', [str(now.month)])[0])

        # Build YYYY-MM prefix for LIKE query
        month_prefix = f"{year:04d}-{month:02d}"

        try:
            conn = get_conn()
            ensure_tables(conn)

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Monthly totals
                cur.execute("""
                    SELECT
                        COALESCE(SUM(cash_income), 0)      AS cash_income,
                        COALESCE(SUM(card_income), 0)      AS card_income,
                        COALESCE(SUM(coffee_portions), 0)  AS coffee_portions,
                        COALESCE(SUM(cash_deposit), 0)     AS cash_deposit,
                        COALESCE(SUM(cash_withdrawal), 0)  AS cash_withdrawal,
                        COALESCE(SUM(expenses), 0)         AS expenses,
                        COUNT(DISTINCT date)               AS days_count
                    FROM records
                    WHERE date LIKE %s
                """, (month_prefix + '%',))
                totals = cur.fetchone()

                # Daily breakdown for the month
                cur.execute("""
                    SELECT
                        date,
                        COALESCE(SUM(cash_income), 0)     AS cash_income,
                        COALESCE(SUM(card_income), 0)     AS card_income,
                        COALESCE(SUM(coffee_portions), 0) AS coffee_portions,
                        COALESCE(SUM(cash_deposit), 0)    AS cash_deposit,
                        COALESCE(SUM(cash_withdrawal), 0) AS cash_withdrawal,
                        COALESCE(SUM(expenses), 0)        AS expenses
                    FROM records
                    WHERE date LIKE %s
                    GROUP BY date
                    ORDER BY date ASC
                """, (month_prefix + '%',))
                daily_rows = cur.fetchall()

            conn.close()

            daily = [
                {
                    "date": row['date'],
                    "cash_income": float(row['cash_income']),
                    "card_income": float(row['card_income']),
                    "coffee_portions": int(row['coffee_portions']),
                    "cash_deposit": float(row['cash_deposit']),
                    "cash_withdrawal": float(row['cash_withdrawal']),
                    "expenses": float(row['expenses']),
                }
                for row in daily_rows
            ]

            result = {
                "year": year,
                "month": month,
                "month_str": month_prefix,
                "totals": {
                    "cash_income": float(totals['cash_income']),
                    "card_income": float(totals['card_income']),
                    "coffee_portions": int(totals['coffee_portions']),
                    "cash_deposit": float(totals['cash_deposit']),
                    "cash_withdrawal": float(totals['cash_withdrawal']),
                    "expenses": float(totals['expenses']),
                    "days_count": int(totals['days_count']),
                    "total_income": float(totals['cash_income']) + float(totals['card_income']),
                },
                "daily": daily,
            }
            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
