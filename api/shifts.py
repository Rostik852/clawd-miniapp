"""
Shifts API — Vercel Serverless Function
GET  /api/shifts?user_id=...&from=YYYY-MM-DD&to=YYYY-MM-DD
POST /api/shifts   {action: set|delete|swap_request|swap_respond, ...}
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import date as date_cls, timedelta

from _db import (
    get_conn, ensure_tables, get_user, ADMIN_ID,
    get_shifts_range, set_shift, delete_shift,
    get_workers, request_swap, respond_swap, get_pending_swaps, is_admin,
    notify_admins, notify_user, get_notification_settings, set_notification_settings
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
        user_id_str = params.get('user_id', [None])[0]
        if not user_id_str:
            _json(self, 401, {'error': 'user_id required'})
            return
        try:
            uid = int(user_id_str)
        except ValueError:
            _json(self, 400, {'error': 'invalid user_id'})
            return

        conn = get_conn()
        try:
            ensure_tables(conn)
            user = get_user(conn, uid) if uid != ADMIN_ID else {'role': 'super_admin', 'is_approved': 1}
            if not user or not user.get('is_approved'):
                _json(self, 403, {'error': 'forbidden'})
                return

            date_from = params.get('from', [str(date_cls.today())])[0]
            date_to   = params.get('to',   [str(date_cls.today() + timedelta(days=13))])[0]

            shifts  = get_shifts_range(conn, date_from, date_to)
            workers = get_workers(conn)
            swaps   = get_pending_swaps(conn, uid)

            _json(self, 200, {
                'shifts': shifts,
                'workers': workers,
                'pending_swaps': swaps,
                'is_admin': is_admin(conn, uid),
            })
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

        uid_raw = body.get('user_id')
        if not uid_raw:
            _json(self, 400, {'error': 'user_id required'})
            return
        uid = int(uid_raw)
        action = body.get('action', '')

        conn = get_conn()
        try:
            ensure_tables(conn)
            user = get_user(conn, uid) if uid != ADMIN_ID else {'role': 'super_admin', 'is_approved': 1}
            if not user or not user.get('is_approved'):
                _json(self, 403, {'error': 'forbidden'})
                return

            if action == 'set':
                if not is_admin(conn, uid):
                    _json(self, 403, {'error': 'admin only'})
                    return
                worker_id = int(body['worker_id'])
                notify_worker = body.get('notify_worker', True)
                shift = set_shift(
                    conn,
                    date=body['date'],
                    shift_num=int(body.get('shift_num', 1)),
                    user_id=worker_id,
                    time_start=body.get('time_start'),
                    time_end=body.get('time_end'),
                    notes=body.get('notes'),
                    created_by=uid,
                )
                # Get worker name for notification
                worker_user = get_user(conn, worker_id) if worker_id != ADMIN_ID else None
                wname = 'Адмін'
                if worker_user:
                    n = ((worker_user.get('first_name') or '') + ' ' + (worker_user.get('last_name') or '')).strip()
                    wname = n or worker_user.get('username') or f'#{worker_id}'
                ts = body.get('time_start', '')
                te = body.get('time_end', '')
                time_str = f'{ts}–{te}' if ts else ''
                shift_info = (f'📆 {body["date"]}, Зміна {body.get("shift_num",1)}'
                              + (f'\n🕐 {time_str}' if time_str else ''))

                # Notify assigned worker (if different from admin who created, and notify enabled)
                if notify_worker and worker_id != uid:
                    notify_user(conn, worker_id,
                        f'📅 <b>Нова зміна в графіку</b>\n👤 {wname}\n{shift_info}',
                        setting_key='notify_shift_assigned'
                    )

                # Always notify admins about new shifts (including when admin sets own shift)
                notify_admins(conn,
                    f'📅 <b>Зміну виставлено</b>\n👤 {wname}\n{shift_info}',
                    setting_key='notify_shift_assigned'
                )
                _json(self, 200, {'ok': True, 'shift': shift})

            elif action == 'delete':
                if not is_admin(conn, uid):
                    _json(self, 403, {'error': 'admin only'})
                    return
                delete_shift(conn, date=body['date'], shift_num=int(body.get('shift_num', 1)))
                _json(self, 200, {'ok': True})

            elif action == 'swap_request':
                target_id = int(body['target_id'])
                swap = request_swap(
                    conn,
                    requester_id=uid,
                    target_id=target_id,
                    date=body['date'],
                    notes=body.get('notes'),
                )
                # Notify target worker
                req_user = get_user(conn, uid)
                req_name = 'Колега'
                if req_user:
                    n = ((req_user.get('first_name') or '') + ' ' + (req_user.get('last_name') or '')).strip()
                    req_name = n or req_user.get('username') or f'#{uid}'
                note_txt = f'\n💬 {body["notes"]}' if body.get('notes') else ''
                notify_user(conn, target_id,
                    f'🔄 <b>Запит на обмін зміною</b>\n'
                    f'👤 {req_name} хоче помінятись на {body["date"]}{note_txt}\n'
                    f'Відкрий Графік змін щоб відповісти.',
                    setting_key=None  # always deliver swap requests to target
                )
                # Notify admins
                notify_admins(conn,
                    f'🔄 <b>Запит на обмін</b>\n👤 {req_name} → {body["date"]}',
                    setting_key='on_swap_request'
                )
                _json(self, 200, {'ok': True, 'swap': swap})

            elif action == 'swap_respond':
                result = respond_swap(
                    conn,
                    swap_id=int(body['swap_id']),
                    user_id=uid,
                    accept=bool(body.get('accept', False)),
                )
                if result:
                    _json(self, 200, {'ok': True})
                else:
                    _json(self, 404, {'error': 'swap not found or not authorized'})

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
