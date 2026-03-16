"""
Data API — Vercel Serverless Function
GET /api/data?type=summary|weekly|monthly|records&user_id=...&[date=YYYY-MM-DD]&[limit=20]&[year=YYYY]&[month=MM]

Consolidates: summary.py, weekly.py, monthly.py, records.py
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import date as date_cls, datetime, timezone

import psycopg2.extras

from _db import get_conn, ensure_tables, today_str, get_user, ADMIN_ID, get_daily_summary, get_weekly_data
from _cors import add_cors, handle_options


def _json_response(handler_obj, status, data):
    body = json.dumps(data, default=str).encode()
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
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        user_id_str = params.get('user_id', [None])[0]
        data_type = params.get('type', ['summary'])[0]

        if not user_id_str:
            self._json_error(401, "user_id required")
            return

        try:
            uid = int(user_id_str)
        except (ValueError, TypeError):
            self._json_error(400, "invalid user_id")
            return

        conn = get_conn()
        try:
            ensure_tables(conn)

            # Admin always allowed; others must be approved
            if uid != ADMIN_ID:
                user = get_user(conn, uid)
                if not user or not user.get('is_approved'):
                    self._json_error(403, "forbidden")
                    return

            if data_type == 'summary':
                # --- summary logic (from summary.py) ---
                date = params.get('date', [today_str()])[0]
                try:
                    summary = get_daily_summary(conn, date)
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

            elif data_type == 'weekly':
                # --- weekly logic (from weekly.py) ---
                try:
                    result = get_weekly_data(conn)
                    _json_response(self, 200, result)
                except Exception as e:
                    _json_response(self, 500, {"error": str(e)})

            elif data_type == 'monthly':
                # --- monthly logic (from monthly.py) ---
                now = datetime.now(timezone.utc)
                year = int(params.get('year', [str(now.year)])[0])
                month = int(params.get('month', [str(now.month)])[0])
                month_prefix = f"{year:04d}-{month:02d}"

                try:
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

            elif data_type == 'records':
                # --- records logic (from records.py) ---
                try:
                    limit = int(params.get('limit', ['20'])[0])
                    limit = min(limit, 100)  # cap at 100
                except ValueError:
                    limit = 20

                date = params.get('date', [today_str()])[0]

                try:
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

            else:
                self._json_error(400, "unknown type — use type=summary|weekly|monthly|records")

        finally:
            conn.close()

    def log_message(self, fmt, *args):
        pass
