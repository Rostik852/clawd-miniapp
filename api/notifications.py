"""
Notifications API
GET  /api/notifications?user_id=...         — get settings
POST /api/notifications   {action: update_settings | remind_check}
  update_settings: {user_id, on_snapshot?, on_close_day?, on_swap_request?, remind_snapshot?, notify_shift_assigned?}
  remind_check: called by cron — checks shifts ending in 10 min and sends reminders
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timezone, timedelta

from _db import (
    get_conn, ensure_tables, get_user, ADMIN_ID, is_admin,
    get_notification_settings, set_notification_settings,
    get_shifts_range, notify_user
)
from _cors import add_cors, handle_options


def _json(h, status, data):
    body = json.dumps(data, default=str).encode()
    h.send_response(status)
    h.send_header('Content-Type', 'application/json')
    add_cors(h)
    h.end_headers()
    h.wfile.write(body)


class handler(BaseHTTPRequestHandler):

    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        # Cron endpoint: /api/notifications?cron=remind
        if params.get('cron', [None])[0] == 'remind':
            conn = get_conn()
            try:
                ensure_tables(conn)
                now_utc = datetime.now(timezone.utc)
                local_now = now_utc + timedelta(hours=2)
                today_str = local_now.strftime('%Y-%m-%d')
                shifts = get_shifts_range(conn, today_str, today_str)
                sent = 0
                for s in shifts:
                    if not s.get('time_end'):
                        continue
                    h_e, m_e = s['time_end'].split(':')
                    shift_end_mins = int(h_e) * 60 + int(m_e)
                    h_n, m_n = local_now.strftime('%H:%M').split(':')
                    now_mins = int(h_n) * 60 + int(m_n)
                    diff = shift_end_mins - now_mins
                    if 8 <= diff <= 14:
                        notify_user(
                            conn, s['user_id'],
                            f'⏰ <b>Нагадування</b>\nЗміна закінчується о {s["time_end"]}.\n'
                            f'Не забудь зробити 📸 Snapshot!',
                            setting_key='remind_snapshot'
                        )
                        sent += 1
                _json(self, 200, {'ok': True, 'reminders_sent': sent})
            except Exception as e:
                _json(self, 500, {'error': str(e)})
            finally:
                conn.close()
            return

        uid_str = params.get('user_id', [None])[0]
        if not uid_str:
            _json(self, 400, {'error': 'user_id required'})
            return
        uid = int(uid_str)
        conn = get_conn()
        try:
            ensure_tables(conn)
            settings = get_notification_settings(conn, uid)
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
                # Admin can update anyone's settings; user can update own
                requesting_uid = int(body.get('requesting_user_id', uid))
                if requesting_uid != uid and not is_admin(conn, requesting_uid):
                    _json(self, 403, {'error': 'forbidden'})
                    return
                allowed_keys = {
                    'on_snapshot', 'on_close_day', 'on_swap_request',
                    'remind_snapshot', 'notify_shift_assigned'
                }
                updates = {k: int(bool(v)) for k, v in body.items() if k in allowed_keys}
                set_notification_settings(conn, uid, **updates)
                _json(self, 200, {'ok': True, 'settings': get_notification_settings(conn, uid)})

            elif action == 'remind_check':
                # Called by cron — find shifts ending in next 10-15 min, send reminders
                now_utc = datetime.now(timezone.utc)
                # Convert to local time (Europe/Warsaw UTC+2 approximate)
                local_now = now_utc + timedelta(hours=2)
                target_end = (local_now + timedelta(minutes=10)).strftime('%H:%M')
                today_str = local_now.strftime('%Y-%m-%d')

                shifts = get_shifts_range(conn, today_str, today_str)
                sent = 0
                for s in shifts:
                    if not s.get('time_end'):
                        continue
                    # Check if shift ends in ~10 minutes (within ±2 min window)
                    [h_e, m_e] = s['time_end'].split(':')
                    shift_end_mins = int(h_e) * 60 + int(m_e)
                    [h_n, m_n] = local_now.strftime('%H:%M').split(':')
                    now_mins = int(h_n) * 60 + int(m_n)
                    diff = shift_end_mins - now_mins
                    if 8 <= diff <= 14:
                        notify_user(
                            conn,
                            s['user_id'],
                            f'⏰ <b>Нагадування</b>\nЗміна закінчується о {s["time_end"]}.\n'
                            f'Не забудь зробити 📸 Snapshot!',
                            setting_key='remind_snapshot'
                        )
                        sent += 1
                _json(self, 200, {'ok': True, 'reminders_sent': sent})

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
