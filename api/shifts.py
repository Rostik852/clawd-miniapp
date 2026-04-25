"""
Shifts API â€” Vercel Serverless Function
GET  /api/shifts?user_id=...&from=YYYY-MM-DD&to=YYYY-MM-DD
POST /api/shifts   {action: set|batch_set|delete|swap_request|swap_respond, ...}
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import date as date_cls, timedelta

from _db import (
    get_conn, ensure_tables, get_user, ADMIN_ID,
    get_shifts_range, set_shift, delete_shift, clear_worker_shifts,
    get_workers, request_swap, respond_swap, get_pending_swaps, is_admin,
    notify_admins, notify_user, get_notification_settings, set_notification_settings,
    get_user_modules, get_day_off_requests_range, upsert_day_off_request,
    respond_day_off_request, delete_day_off_request
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
            if uid != ADMIN_ID:
                modules = get_user_modules(conn, uid)
                if not modules.get('shifts', False):
                    _json(self, 403, {'error': 'module access denied'})
                    return

            date_from = params.get('from', [str(date_cls.today())])[0]
            date_to   = params.get('to',   [str(date_cls.today() + timedelta(days=13))])[0]

            shifts  = get_shifts_range(conn, date_from, date_to)
            workers = get_workers(conn)
            swaps   = get_pending_swaps(conn, uid)
            day_offs = get_day_off_requests_range(
                conn,
                date_from,
                date_to,
                user_id=None if is_admin(conn, uid) else uid,
                statuses=['pending', 'approved']
            )

            _json(self, 200, {
                'shifts': shifts,
                'workers': workers,
                'pending_swaps': swaps,
                'is_admin': is_admin(conn, uid),
                'day_offs': day_offs,
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
            if uid != ADMIN_ID:
                modules = get_user_modules(conn, uid)
                if not modules.get('shifts', False):
                    _json(self, 403, {'error': 'module access denied'})
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
                delete_day_off_request(conn, worker_id, body['date'])
                # Get worker name for notification
                worker_user = get_user(conn, worker_id) if worker_id != ADMIN_ID else None
                wname = 'Адмін'
                if worker_user:
                    n = ((worker_user.get('first_name') or '') + ' ' + (worker_user.get('last_name') or '')).strip()
                    wname = n or worker_user.get('username') or f'#{worker_id}'
                ts = body.get('time_start', '')
                te = body.get('time_end', '')
                time_str = f'{ts}-{te}' if ts and te else (ts or te or '')
                shift_info = (f'📅 {body["date"]}, зміна {body.get("shift_num",1)}'
                              + (f'\n🕐 {time_str}' if time_str else ''))

                # Notify assigned worker (if different from admin who created, and notify enabled)
                if notify_worker and worker_id != uid:
                    notify_user(
                        conn, worker_id,
                        f'📅 <b>Нова зміна в графіку</b>\n👤 {wname}\n{shift_info}',
                        setting_key='notify_shift_assigned'
                    )
                # Always notify admins about new shifts (including when admin sets own shift)
                notify_admins(
                    conn,
                    f'📅 <b>Зміну виставлено</b>\n👤 {wname}\n{shift_info}',
                    setting_key='on_shift_assigned'
                )
                _json(self, 200, {'ok': True, 'shift': shift})

            elif action == 'batch_set':
                if not is_admin(conn, uid):
                    _json(self, 403, {'error': 'admin only'})
                    return
                worker_id = int(body['worker_id'])
                assignments = body.get('assignments') or []
                if not isinstance(assignments, list) or not assignments:
                    _json(self, 400, {'error': 'assignments required'})
                    return

                notify_worker = body.get('notify_worker', True)
                worker_user = get_user(conn, worker_id) if worker_id != ADMIN_ID else None
                wname = 'Адмін'
                if worker_user:
                    n = ((worker_user.get('first_name') or '') + ' ' + (worker_user.get('last_name') or '')).strip()
                    wname = n or worker_user.get('username') or f'#{worker_id}'

                saved = []
                days_off = 0
                for item in assignments:
                    date_val = item.get('date')
                    shift_num = int(item.get('shift_num', 0) or 0)
                    if not date_val:
                        continue
                    if shift_num == -1:
                        clear_worker_shifts(conn, date_val, worker_id)
                        upsert_day_off_request(
                            conn,
                            user_id=worker_id,
                            date=date_val,
                            notes=item.get('notes'),
                            requested_by=uid,
                            status='approved',
                            source='admin',
                        )
                        days_off += 1
                        continue
                    if shift_num not in (1, 2):
                        continue
                    delete_day_off_request(conn, worker_id, date_val)
                    shift = set_shift(
                        conn,
                        date=date_val,
                        shift_num=shift_num,
                        user_id=worker_id,
                        time_start=item.get('time_start'),
                        time_end=item.get('time_end'),
                        notes=item.get('notes'),
                        created_by=uid,
                    )
                    saved.append(shift)

                if not saved and not days_off:
                    _json(self, 400, {'error': 'no valid assignments'})
                    return

                if notify_worker and worker_id != uid:
                    preview_items = []
                    for item in assignments[:8]:
                        if not item.get('date'):
                            continue
                        marker = 'вихідний' if int(item.get('shift_num', 0) or 0) == -1 else item.get('shift_num', 1)
                        preview_items.append(f"{item['date']} ({marker})")
                    preview = ', '.join(preview_items)
                    if (len(saved) + days_off) > 8:
                        preview += f" +{(len(saved) + days_off) - 8}"
                    notify_user(
                        conn,
                        worker_id,
                        f'📅 <b>Зміни додано пакетно</b>\n👤 {wname}\n{preview}',
                        setting_key='notify_shift_assigned'
                    )

                notify_admins(
                    conn,
                    f'📅 <b>Пакетно додано зміни</b>\n👤 {wname}\nКількість: {len(saved)}\nВихідних: {days_off}',
                    setting_key='on_shift_assigned'
                )
                _json(self, 200, {'ok': True, 'count': len(saved), 'days_off': days_off, 'shifts': saved})

            elif action == 'delete':
                if not is_admin(conn, uid):
                    _json(self, 403, {'error': 'admin only'})
                    return
                delete_shift(conn, date=body['date'], shift_num=int(body.get('shift_num', 1)))
                _json(self, 200, {'ok': True})

            elif action == 'request_day_off':
                existing = get_day_off_requests_range(
                    conn, body['date'], body['date'], user_id=uid, statuses=['approved']
                )
                if existing:
                    _json(self, 400, {'error': 'day off already approved for this date'})
                    return
                req = upsert_day_off_request(
                    conn,
                    user_id=uid,
                    date=body['date'],
                    notes=body.get('notes'),
                    requested_by=uid,
                    status='pending',
                    source='request',
                )
                req_user = get_user(conn, uid)
                req_name = 'ĐźŃ€Đ°Ń†Ń–Đ˛Đ˝Đ¸Đş'
                if req_user:
                    n = ((req_user.get('first_name') or '') + ' ' + (req_user.get('last_name') or '')).strip()
                    req_name = n or req_user.get('username') or f'#{uid}'
                note_txt = f"\nđź’¬ {body['notes']}" if body.get('notes') else ''
                notify_admins(
                    conn,
                    f'đź—“ <b>Đ—Đ°ĐżĐ¸Ń‚ Đ˝Đ° Đ˛Đ¸Ń…Ń–Đ´Đ˝Đ¸Đą</b>\nđź‘¤ {req_name}\nđź“… {body["date"]}{note_txt}',
                    setting_key='on_shift_assigned'
                )
                _json(self, 200, {'ok': True, 'day_off': req})

            elif action == 'set_day_off':
                if not is_admin(conn, uid):
                    _json(self, 403, {'error': 'admin only'})
                    return
                worker_id = int(body['worker_id'])
                clear_worker_shifts(conn, body['date'], worker_id)
                req = upsert_day_off_request(
                    conn,
                    user_id=worker_id,
                    date=body['date'],
                    notes=body.get('notes'),
                    requested_by=uid,
                    status='approved',
                    source='admin',
                )
                notify_user(
                    conn,
                    worker_id,
                    f'đź—“ <b>Đ’Đ¸Ń…Ń–Đ´Đ˝Đ¸Đą ĐżŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¶ĐµĐ˝Đľ</b>\nđź“… {body["date"]}',
                    setting_key='notify_shift_assigned'
                )
                _json(self, 200, {'ok': True, 'day_off': req})

            elif action == 'respond_day_off':
                if not is_admin(conn, uid):
                    _json(self, 403, {'error': 'admin only'})
                    return
                status = 'approved' if body.get('approve') else 'rejected'
                req = respond_day_off_request(conn, int(body['request_id']), status, uid)
                if not req:
                    _json(self, 404, {'error': 'day off request not found'})
                    return
                if status == 'approved':
                    clear_worker_shifts(conn, req['date'], req['user_id'])
                notify_user(
                    conn,
                    req['user_id'],
                    f'đź—“ <b>Đ—Đ°ĐżĐ¸Ń‚ Đ˝Đ° Đ˛Đ¸Ń…Ń–Đ´Đ˝Đ¸Đą { "ĐżŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¶ĐµĐ˝Đľ" if status == "approved" else "Đ˛Ń–Đ´Ń…Đ¸Đ»ĐµĐ˝Đľ" }</b>\nđź“… {req["date"]}',
                    setting_key='notify_shift_assigned'
                )
                _json(self, 200, {'ok': True, 'day_off': req})

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
                req_name = 'ĐšĐľĐ»ĐµĐłĐ°'
                if req_user:
                    n = ((req_user.get('first_name') or '') + ' ' + (req_user.get('last_name') or '')).strip()
                    req_name = n or req_user.get('username') or f'#{uid}'
                note_txt = f'\nđź’¬ {body["notes"]}' if body.get('notes') else ''
                notify_user(conn, target_id,
                    f'đź”„ <b>Đ—Đ°ĐżĐ¸Ń‚ Đ˝Đ° ĐľĐ±ĐĽŃ–Đ˝ Đ·ĐĽŃ–Đ˝ĐľŃŽ</b>\n'
                    f'đź‘¤ {req_name} Ń…ĐľŃ‡Đµ ĐżĐľĐĽŃ–Đ˝ŃŹŃ‚Đ¸ŃŃŚ Đ˝Đ° {body["date"]}{note_txt}\n'
                    f'Đ’Ń–Đ´ĐşŃ€Đ¸Đą Đ“Ń€Đ°Ń„Ń–Đş Đ·ĐĽŃ–Đ˝ Ń‰ĐľĐ± Đ˛Ń–Đ´ĐżĐľĐ˛Ń–ŃŃ‚Đ¸.',
                    setting_key=None  # always deliver swap requests to target
                )
                # Notify admins
                notify_admins(conn,
                    f'đź”„ <b>Đ—Đ°ĐżĐ¸Ń‚ Đ˝Đ° ĐľĐ±ĐĽŃ–Đ˝</b>\nđź‘¤ {req_name} â†’ {body["date"]}',
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
