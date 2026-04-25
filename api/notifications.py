"""
Notifications API
GET  /api/notifications?user_id=...         -> get settings
GET  /api/notifications?cron=remind         -> cron snapshot reminders
POST /api/notifications
  update_settings: {user_id, on_snapshot?, on_close_day?, on_swap_request?, on_shift_assigned?, remind_snapshot?, notify_shift_assigned?}
  remind_check: called by cron -> checks shifts ending in 10 min and sends reminders
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from _db import (
    get_conn, ensure_tables, is_admin,
    get_notification_settings, set_notification_settings,
    get_shifts_range, notify_user, get_bot_state, set_bot_state
)
from _cors import add_cors, handle_options


LOCAL_TZ = ZoneInfo('Europe/Warsaw')


def _json(h, status, data):
    body = json.dumps(data, default=str).encode()
    h.send_response(status)
    h.send_header('Content-Type', 'application/json')
    add_cors(h)
    h.end_headers()
    h.wfile.write(body)


def _local_now():
    return datetime.now(timezone.utc).astimezone(LOCAL_TZ)


def _remind_snapshot_check(conn):
    local_now = _local_now()
    today_str = local_now.strftime('%Y-%m-%d')
    now_mins = local_now.hour * 60 + local_now.minute
    shifts = get_shifts_range(conn, today_str, today_str)
    sent = 0

    for shift in shifts:
        if not shift.get('time_end'):
            continue
        try:
            h_e, m_e = shift['time_end'].split(':')
            shift_end_mins = int(h_e) * 60 + int(m_e)
        except Exception:
            continue

        # Exact 10 minutes before end of this worker's shift.
        if shift_end_mins - now_mins != 10:
            continue

        reminder_key = f'remind_snapshot:{today_str}:{shift["time_end"]}'
        state = get_bot_state(conn, shift['user_id']) or {}
        if state.get('state') == reminder_key:
            continue

        notify_user(
            conn,
            shift['user_id'],
            f'⏰ <b>Нагадування</b>\nЗміна закінчується о {shift["time_end"]}.\nНе забудь зробити 📸 Snapshot!',
            setting_key='remind_snapshot'
        )
        set_bot_state(conn, shift['user_id'], reminder_key, today_str)
        sent += 1

    return sent


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        if params.get('cron', [None])[0] == 'remind':
            conn = get_conn()
            try:
                ensure_tables(conn)
                _json(self, 200, {'ok': True, 'reminders_sent': _remind_snapshot_check(conn)})
            except Exception as e:
                _json(self, 500, {'error': str(e)})
            finally:
                conn.close()
            return

        uid_str = params.get('user_id', [None])[0]
        if not uid_str:
            _json(self, 400, {'error': 'user_id required'})
            return

        conn = get_conn()
        try:
            ensure_tables(conn)
            settings = get_notification_settings(conn, int(uid_str))
            _json(self, 200, settings)
        except Exception as e:
            _json(self, 500, {'error': str(e)})
        finally:
            conn.close()

    def do_POST(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            body = json.loads(self.rfile.read(length))
        except Exception:
            _json(self, 400, {'error': 'invalid json'})
            return

        action = body.get('action', 'update_settings')
        conn = get_conn()
        try:
            ensure_tables(conn)

            if action == 'update_settings':
                uid = int(body['user_id'])
                requesting_uid = int(body.get('requesting_user_id', uid))
                if requesting_uid != uid and not is_admin(conn, requesting_uid):
                    _json(self, 403, {'error': 'forbidden'})
                    return

                allowed_keys = {
                    'on_snapshot', 'on_close_day', 'on_swap_request', 'on_shift_assigned',
                    'remind_snapshot', 'notify_shift_assigned'
                }
                updates = {k: int(bool(v)) for k, v in body.items() if k in allowed_keys}
                set_notification_settings(conn, uid, **updates)
                _json(self, 200, {'ok': True, 'settings': get_notification_settings(conn, uid)})

            elif action == 'remind_check':
                _json(self, 200, {'ok': True, 'reminders_sent': _remind_snapshot_check(conn)})

            else:
                _json(self, 400, {'error': f'unknown action: {action}'})

        except KeyError as e:
            _json(self, 400, {'error': f'missing field: {e}'})
        except Exception as e:
            _json(self, 500, {'error': str(e)})
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        pass
