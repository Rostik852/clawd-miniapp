import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from http.server import BaseHTTPRequestHandler
import json
from urllib.parse import urlparse, parse_qs

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, today_str, get_daily_summary


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
            summary = get_daily_summary(conn, date)
            conn.close()

            result = {
                "date": summary["date"],
                "opening_cash": float(summary["opening_cash"] or 0),
                "closing_cash": float(summary["closing_cash"]) if summary["closing_cash"] is not None else None,
                "cash_income": float(summary["cash_income"]),
                "card_income": float(summary["card_income"]),
                "coffee_portions": int(summary["coffee_portions"]),
                "expenses": float(summary["expenses"]),
                "admin_deposits": float(summary["admin_deposits"]),
                "admin_withdrawals": float(summary["admin_withdrawals"]),
                "net_cash": float(summary["net_cash"]),
                "is_finalized": bool(summary["is_finalized"]),
                "closed_at": summary.get("closed_at"),
                "snapshots": summary.get("snapshots", []),
            }
            _json_response(self, 200, result)

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
