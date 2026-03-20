import sys, os
sys.path.insert(0, os.path.dirname(__file__))
import json
from http.server import BaseHTTPRequestHandler
from datetime import datetime, timezone

from _db import get_conn, ensure_tables, get_user, verify_tg_signature, add_snapshot, delete_snapshot, is_admin, ADMIN_ID
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
        cash_amount = payload.get('cash_amount')
        coffee_portions = payload.get('coffee_portions')
        date_str = payload.get('date') or datetime.now(timezone.utc).strftime('%Y-%m-%d')
        time_str = payload.get('time') or datetime.now(timezone.utc).strftime('%H:%M')

        if not user_id:
            _json(self, 400, {'error': 'user_id required'})
            return

        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            _json(self, 400, {'error': 'invalid user_id'})
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

            # Verify Telegram init_data signature (skip in dev if empty)
            if init_data:
                bot_token = os.environ.get('BOT_TOKEN', '')
                if bot_token and not verify_tg_signature(init_data, bot_token):
                    conn.close()
                    _json(self, 403, {'error': 'invalid signature'})
                    return

            add_snapshot(
                conn,
                user_id=user_id,
                date_str=date_str,
                time_str=time_str,
                cash_amount=float(cash_amount) if cash_amount is not None else None,
                coffee_portions=int(coffee_portions) if coffee_portions is not None else None,
            )
            conn.close()
            _json(self, 200, {'ok': True})

        except Exception as e:
            _json(self, 500, {'error': str(e)})

    def do_DELETE(self):
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)
        snap_id = params.get('id', [None])[0]
        user_id_str = params.get('user_id', [None])[0]

        if not snap_id or not user_id_str:
            _json(self, 400, {'error': 'id and user_id required'})
            return

        conn = get_conn()
        try:
            ensure_tables(conn)
            uid = int(user_id_str)
            user = get_user(conn, uid)
            if not user or not user.get('is_approved'):
                _json(self, 403, {'error': 'forbidden'})
                return
            is_adm = is_admin(conn, uid)
            delete_snapshot(conn, int(snap_id), None if is_adm else uid)
            _json(self, 200, {'ok': True})
        except Exception as e:
            _json(self, 500, {'error': str(e)})
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        pass
