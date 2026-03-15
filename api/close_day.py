import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import json
from http.server import BaseHTTPRequestHandler
from datetime import datetime, timezone

from _db import (
    get_conn, ensure_tables, get_user, verify_tg_signature,
    get_or_create_session, update_session, ADMIN_ID
)
from _cors import add_cors, handle_options


def _json(handler_obj, status, data):
    body = json.dumps(data).encode()
    handler_obj.send_response(status)
    handler_obj.send_header('Content-Type', 'application/json')
    add_cors(handler_obj)
    handler_obj.end_headers()
    handler_obj.wfile.write(body)


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_POST(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length)
            payload = json.loads(body)
        except Exception:
            _json(self, 400, {'error': 'invalid json'})
            return

        user_id = payload.get('user_id')
        init_data = payload.get('init_data', '')
        closing_cash = payload.get('closing_cash')
        coffee_portions = payload.get('coffee_portions')
        card_income = payload.get('card_income')
        date_str = payload.get('date') or datetime.now(timezone.utc).strftime('%Y-%m-%d')

        if not user_id:
            _json(self, 400, {'error': 'user_id required'})
            return
        if closing_cash is None:
            _json(self, 400, {'error': 'closing_cash required'})
            return

        try:
            user_id = int(user_id)
            closing_cash = float(closing_cash)
        except (ValueError, TypeError):
            _json(self, 400, {'error': 'invalid values'})
            return

        try:
            conn = get_conn()
            ensure_tables(conn)

            # Verify user is approved (admin always allowed)
            if user_id != ADMIN_ID:
                user = get_user(conn, user_id)
                if not user or not user.get('is_approved'):
                    conn.close()
                    _json(self, 403, {'error': 'forbidden'})
                    return

            # Verify Telegram init_data signature (skip if empty or no token)
            if init_data:
                bot_token = os.environ.get('BOT_TOKEN', '')
                if bot_token and not verify_tg_signature(init_data, bot_token):
                    conn.close()
                    _json(self, 403, {'error': 'invalid signature'})
                    return

            # Get or create session for the date
            session = get_or_create_session(conn, date_str)
            opening_cash = float(session.get('opening_cash') or 0)

            # Compute cash income
            cash_income = round(closing_cash - opening_cash, 2)

            # Build update fields
            fields = {
                'closing_cash': closing_cash,
                'is_finalized': 1,
                'closed_by': user_id,
                'closed_at': datetime.now(timezone.utc).isoformat(),
            }
            if coffee_portions is not None:
                fields['coffee_portions'] = int(coffee_portions)
            if card_income is not None:
                fields['card_income'] = float(card_income)

            update_session(conn, date_str, **fields)
            conn.close()

            _json(self, 200, {
                'ok': True,
                'cash_income': cash_income,
                'closing_cash': closing_cash,
                'opening_cash': opening_cash,
            })

        except Exception as e:
            _json(self, 500, {'error': str(e)})

    def log_message(self, fmt, *args):
        pass
