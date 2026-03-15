from http.server import BaseHTTPRequestHandler
import json
import os
import hmac
import hashlib
from urllib.parse import unquote, parse_qsl
from datetime import datetime, timezone

from _cors import add_cors, handle_options
from _db import get_conn, ensure_tables, get_user, get_user_modules, ADMIN_ID, MODULES


def _json_response(handler_obj, status, data):
    body = json.dumps(data).encode()
    handler_obj.send_response(status)
    handler_obj.send_header('Content-Type', 'application/json')
    add_cors(handler_obj)
    handler_obj.end_headers()
    handler_obj.wfile.write(body)


def verify_telegram_init_data(init_data: str, bot_token: str) -> bool:
    """Verify Telegram WebApp initData HMAC-SHA256 signature."""
    try:
        parsed = dict(parse_qsl(init_data, keep_blank_values=True))
        received_hash = parsed.pop('hash', None)
        if not received_hash:
            return False

        # Build data-check-string: sorted key=value lines
        data_check_string = '\n'.join(
            f"{k}={v}" for k, v in sorted(parsed.items())
        )

        secret_key = hmac.new(b'WebAppData', bot_token.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        return hmac.compare_digest(computed_hash, received_hash)
    except Exception:
        return False


def upsert_user(conn, user_id, username, first_name, last_name):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (id, username, first_name, last_name, is_approved, joined_at)
            VALUES (%s, %s, %s, %s, 0, %s)
            ON CONFLICT (id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name
        """, (
            user_id,
            username,
            first_name,
            last_name,
            datetime.now(timezone.utc).isoformat()
        ))
        conn.commit()


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_POST(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length)) if length else {}
        except (ValueError, json.JSONDecodeError):
            _json_response(self, 400, {"error": "Invalid JSON body"})
            return

        init_data = body.get('init_data', '')
        user_id = body.get('user_id')

        if not user_id:
            _json_response(self, 400, {"error": "user_id required"})
            return

        try:
            user_id = int(user_id)
        except (ValueError, TypeError):
            _json_response(self, 400, {"error": "Invalid user_id"})
            return

        # Admin bypass
        is_admin = (user_id == ADMIN_ID)

        # Verify Telegram signature (skip for admin in dev, but still attempt)
        bot_token = os.environ.get('BOT_TOKEN', '')
        if not is_admin and init_data:
            if not verify_telegram_init_data(init_data, bot_token):
                _json_response(self, 401, {"error": "Invalid Telegram signature"})
                return

        try:
            conn = get_conn()
            ensure_tables(conn)

            # Upsert user from initData if available
            if init_data:
                parsed = dict(parse_qsl(init_data, keep_blank_values=True))
                user_json_str = parsed.get('user', '{}')
                try:
                    user_data = json.loads(unquote(user_json_str))
                except Exception:
                    user_data = {}

                upsert_user(
                    conn,
                    user_id,
                    user_data.get('username', ''),
                    user_data.get('first_name', ''),
                    user_data.get('last_name', '')
                )

            user = get_user(conn, user_id)

            if is_admin:
                modules = {m: True for m in MODULES}
                _json_response(self, 200, {
                    "status": "approved",
                    "role": "admin",
                    "modules": modules
                })
                conn.close()
                return

            if not user:
                _json_response(self, 200, {"status": "pending", "role": None, "modules": {}})
                conn.close()
                return

            if not user['is_approved']:
                _json_response(self, 200, {"status": "pending", "role": user.get('role'), "modules": {}})
                conn.close()
                return

            modules = get_user_modules(conn, user_id)
            conn.close()

            _json_response(self, 200, {
                "status": "approved",
                "role": user.get('role'),
                "modules": modules
            })

        except Exception as e:
            _json_response(self, 500, {"error": str(e)})

    def log_message(self, format, *args):
        pass
