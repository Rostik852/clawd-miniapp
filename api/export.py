import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import csv
import io
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import date as date_cls, datetime, timedelta

from _db import get_conn, ensure_tables, is_admin, get_user, get_period_summary, get_shifts_range
from _cors import add_cors, handle_options

CURRENCY = "zł"

def build_csv(data: dict) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)

    period = data.get('period', 'day')
    rows = data.get('rows', [])
    totals = data.get('totals', {})

    writer.writerow([f"Звіт за період: {period.upper()} | {data.get('date','')}"])
    writer.writerow([])
    writer.writerow([
        "Дата",
        f"Розмінка ({CURRENCY})",
        f"Закриття каси ({CURRENCY})",
        f"Виручка готівка ({CURRENCY})",
        f"Виручка картка ({CURRENCY})",
        "Порції кави (шт)",
        "Закрито",
    ])

    for r in rows:
        writer.writerow([
            r.get('date', ''),
            f"{r.get('opening_cash', 0):.2f}",
            f"{r.get('closing_cash') or 0:.2f}",
            f"{r.get('cash_income', 0):.2f}",
            f"{r.get('card_income', 0):.2f}",
            r.get('coffee_portions', 0),
            "Так" if r.get('is_finalized') else "Ні",
        ])

    writer.writerow([])
    writer.writerow([
        "РАЗОМ", "", "",
        f"{totals.get('total_cash_income', 0):.2f}",
        f"{totals.get('total_card_income', 0):.2f}",
        totals.get('total_coffee', 0),
        "",
    ])
    writer.writerow([])

    if totals.get('avg_price_cash') is not None:
        writer.writerow([f"Середня ціна порції (готівкова): {totals['avg_price_cash']:.2f} {CURRENCY}"])
    if totals.get('avg_price_total') is not None:
        writer.writerow([f"Середня ціна порції (загальна):  {totals['avg_price_total']:.2f} {CURRENCY}"])

    return output.getvalue().encode('utf-8-sig')  # utf-8-sig for Excel compatibility


def build_html(data: dict) -> bytes:
    period = data.get('period', 'day')
    rows = data.get('rows', [])
    totals = data.get('totals', {})
    ref_date = data.get('date', '')

    period_labels = {'day': 'День', 'week': 'Тиждень', 'month': 'Місяць'}
    period_label = period_labels.get(period, period)

    rows_html = ""
    for r in rows:
        status = "✅" if r.get('is_finalized') else "🔴"
        rows_html += f"""
        <tr>
            <td>{r.get('date','')}</td>
            <td>{r.get('opening_cash', 0):.2f}</td>
            <td>{r.get('closing_cash') or '—'}</td>
            <td class="pos">{r.get('cash_income', 0):.2f}</td>
            <td class="pos">{r.get('card_income', 0):.2f}</td>
            <td>{r.get('coffee_portions', 0)}</td>
            <td>{status}</td>
        </tr>"""

    avg_html = ""
    if totals.get('avg_price_cash') is not None:
        note = "" if totals.get('has_card_data') else " <span class='note'>(без урахування картки)</span>"
        avg_html += f"<div class='avg-row'>💵 Готівкова середня: <strong>{totals['avg_price_cash']:.2f} {CURRENCY}</strong>{note}</div>"
    if totals.get('avg_price_total') is not None and totals.get('has_card_data'):
        avg_html += f"<div class='avg-row primary'>💳 Загальна середня: <strong>{totals['avg_price_total']:.2f} {CURRENCY}</strong></div>"

    html = f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<title>Звіт Cafe App</title>
<style>
  body {{ font-family: Arial, sans-serif; max-width: 900px; margin: 40px auto; padding: 0 20px; color: #1a1a2e; }}
  h1 {{ color: #3a86ff; }}
  h2 {{ color: #555; font-size: 1rem; font-weight: normal; margin-top: -10px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 24px 0; }}
  th {{ background: #3a86ff; color: white; padding: 10px 12px; text-align: left; font-size: 0.85rem; }}
  td {{ padding: 9px 12px; border-bottom: 1px solid #eee; font-size: 0.9rem; }}
  tr:hover td {{ background: #f5f8ff; }}
  .pos {{ color: #22c55e; font-weight: 600; }}
  .totals td {{ font-weight: 700; background: #f0f4ff; border-top: 2px solid #3a86ff; }}
  .avg-section {{ background: #f8faff; border: 1px solid #dde; border-radius: 8px; padding: 16px 20px; margin: 16px 0; }}
  .avg-row {{ margin: 6px 0; font-size: 0.95rem; }}
  .avg-row.primary {{ color: #3a86ff; font-size: 1.05rem; }}
  .note {{ color: #999; font-size: 0.8rem; }}
  .footer {{ margin-top: 40px; color: #aaa; font-size: 0.75rem; text-align: center; }}
  @media print {{
    button {{ display: none; }}
    body {{ margin: 20px; }}
  }}
</style>
</head>
<body>
<button onclick="window.print()" style="float:right;padding:8px 16px;background:#3a86ff;color:white;border:none;border-radius:6px;cursor:pointer;font-size:0.9rem;">🖨️ Друк / PDF</button>
<h1>☕ Cafe App — Звіт</h1>
<h2>{period_label} | {ref_date}</h2>

<table>
  <thead>
    <tr>
      <th>Дата</th>
      <th>Розмінка ({CURRENCY})</th>
      <th>Закриття ({CURRENCY})</th>
      <th>Виручка готівка ({CURRENCY})</th>
      <th>Виручка картка ({CURRENCY})</th>
      <th>Порції кави</th>
      <th>Статус</th>
    </tr>
  </thead>
  <tbody>
    {rows_html}
  </tbody>
  <tfoot>
    <tr class="totals">
      <td>РАЗОМ</td>
      <td>—</td>
      <td>—</td>
      <td class="pos">{totals.get('total_cash_income', 0):.2f} {CURRENCY}</td>
      <td class="pos">{totals.get('total_card_income', 0):.2f} {CURRENCY}</td>
      <td>{totals.get('total_coffee', 0)} шт</td>
      <td>—</td>
    </tr>
  </tfoot>
</table>

{f'<div class="avg-section"><strong>☕ Середня ціна порції</strong>{avg_html}</div>' if avg_html else ''}

<div class="footer">Згенеровано Cafe App • {ref_date}</div>
</body>
</html>"""
    return html.encode('utf-8')


def _shifts_range(period: str, ref_date: str):
    base = datetime.strptime(ref_date, '%Y-%m-%d').date()
    if period == 'month':
        start = base.replace(day=1)
        if start.month == 12:
            end = start.replace(year=start.year + 1, month=1, day=1) - timedelta(days=1)
        else:
            end = start.replace(month=start.month + 1, day=1) - timedelta(days=1)
        title = start.strftime('%m.%Y')
    else:
        start = base - timedelta(days=base.weekday())
        end = start + timedelta(days=13)
        title = f"{start.strftime('%d.%m')} - {end.strftime('%d.%m.%Y')}"
    return start.isoformat(), end.isoformat(), title


def build_shifts_html(period: str, ref_date: str, shifts: list) -> bytes:
    date_from, date_to, title = _shifts_range(period, ref_date)
    shift_map = {}
    for shift in shifts:
        shift_map.setdefault(shift['date'], {})[shift['shift_num']] = shift

    rows_html = ""
    current = datetime.strptime(date_from, '%Y-%m-%d').date()
    end = datetime.strptime(date_to, '%Y-%m-%d').date()
    while current <= end:
        date_key = current.isoformat()
        s1 = shift_map.get(date_key, {}).get(1)
        s2 = shift_map.get(date_key, {}).get(2)
        def fmt_shift(shift):
            if not shift:
                return "<span class='off'>-</span>"
            time_text = f"<div class='time'>{shift.get('time_start') or ''}{'–' + shift.get('time_end') if shift.get('time_end') else ''}</div>" if shift.get('time_start') else ""
            return f"<div class='worker'>{shift.get('worker_name') or '-'}</div>{time_text}"
        rows_html += f"""
        <tr>
            <td>{current.strftime('%d.%m.%Y')}</td>
            <td>{current.strftime('%a')}</td>
            <td>{fmt_shift(s1)}</td>
            <td>{fmt_shift(s2)}</td>
        </tr>"""
        current += timedelta(days=1)

    html = f"""<!DOCTYPE html>
<html lang="uk">
<head>
<meta charset="UTF-8">
<title>Графік змін Cafe App</title>
<style>
  body {{ font-family: Arial, sans-serif; max-width: 980px; margin: 40px auto; padding: 0 20px; color: #1a1a2e; }}
  h1 {{ color: #3a86ff; }}
  h2 {{ color: #555; font-size: 1rem; font-weight: normal; margin-top: -10px; }}
  table {{ width: 100%; border-collapse: collapse; margin: 24px 0; }}
  th {{ background: #3a86ff; color: white; padding: 10px 12px; text-align: left; font-size: 0.85rem; }}
  td {{ padding: 10px 12px; border-bottom: 1px solid #eee; vertical-align: top; font-size: 0.9rem; }}
  tr:nth-child(even) td {{ background: #f8fbff; }}
  .worker {{ font-weight: 600; }}
  .time {{ color: #667; font-size: 0.8rem; margin-top: 4px; }}
  .off {{ color: #999; }}
  @media print {{
    button {{ display: none; }}
    body {{ margin: 20px; }}
  }}
</style>
</head>
<body>
<button onclick="window.print()" style="float:right;padding:8px 16px;background:#3a86ff;color:white;border:none;border-radius:6px;cursor:pointer;font-size:0.9rem;">🖨️ Друк / PDF</button>
<h1>📅 Cafe App — Графік змін</h1>
<h2>{'Місяць' if period == 'month' else '2 тижні'} | {title}</h2>
<table>
  <thead>
    <tr>
      <th>Дата</th>
      <th>День</th>
      <th>1 зміна</th>
      <th>2 зміна</th>
    </tr>
  </thead>
  <tbody>{rows_html}</tbody>
</table>
</body>
</html>"""
    return html.encode('utf-8')


class handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        handle_options(self)

    def do_GET(self):
        parsed = urlparse(self.path)
        params = parse_qs(parsed.query)

        user_id_str = params.get('user_id', [None])[0]
        period = params.get('period', ['week'])[0]
        fmt = params.get('format', ['csv'])[0]
        ref_date = params.get('date', [date_cls.today().isoformat()])[0]
        report = params.get('report', ['summary'])[0]

        if period not in ('day', 'week', 'month', 'biweek'):
            period = 'week'
        if fmt not in ('csv', 'html'):
            fmt = 'csv'
        if report not in ('summary', 'shifts'):
            report = 'summary'

        conn = get_conn()
        try:
            ensure_tables(conn)

            if not user_id_str:
                self.send_response(403)
                self.send_header('Content-Type', 'application/json')
                add_cors(self)
                self.end_headers()
                self.wfile.write(b'{"error":"forbidden"}')
                return

            uid = int(user_id_str)
            # Check role — barista/trainee forbidden; admin/chef/super_admin allowed
            user = get_user(conn, uid)
            if not user or not user.get('is_approved'):
                self.send_response(403)
                self.send_header('Content-Type', 'application/json')
                add_cors(self)
                self.end_headers()
                self.wfile.write(b'{"error":"forbidden"}')
                return
            if uid != 199897236 and user.get('role') not in ('admin', 'super_admin', 'chef'):
                self.send_response(403)
                self.send_header('Content-Type', 'application/json')
                add_cors(self)
                self.end_headers()
                self.wfile.write(b'{"error":"forbidden"}')
                return

            if report == 'shifts':
                if fmt != 'html':
                    fmt = 'html'
                shifts_period = 'month' if period == 'month' else 'biweek'
                date_from, date_to, _ = _shifts_range(shifts_period, ref_date)
                shifts = get_shifts_range(conn, date_from, date_to)
                content = build_shifts_html(shifts_period, ref_date, shifts)
                self.send_response(200)
                self.send_header('Content-Type', 'text/html; charset=utf-8')
                add_cors(self)
                self.end_headers()
            else:
                data = get_period_summary(conn, period if period != 'biweek' else 'week', ref_date)

                if fmt == 'csv':
                    content = build_csv(data)
                    filename = f"cafe_report_{period}_{ref_date}.csv"
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/csv; charset=utf-8-sig')
                    self.send_header('Content-Disposition', f'attachment; filename="{filename}"')
                    add_cors(self)
                    self.end_headers()
                else:
                    content = build_html(data)
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    add_cors(self)
                    self.end_headers()

            self.wfile.write(content)
        finally:
            conn.close()

    def log_message(self, fmt, *args):
        pass
