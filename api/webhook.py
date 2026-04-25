"""
Telegram Bot Webhook Handler â€” Vercel Serverless Function
Cafe Cash Flow Management Bot
"""

import sys, os
sys.path.insert(0, os.path.dirname(__file__))

import json
import re
from http.server import BaseHTTPRequestHandler
from datetime import datetime, timezone

import requests as req

from _db import (
    get_conn, ensure_tables, get_user, get_user_modules, today_str,
    save_user_raw, add_record, get_summary_day,
    set_role, revoke_access, set_module_access,
    get_all_users, get_pending_users, MODULES, ADMIN_ID,
    get_or_create_session, get_session, update_session,
    add_snapshot, get_snapshots, get_daily_summary, is_admin,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
CURRENCY = "zĹ‚"
DENOMINATIONS = [500, 100, 50, 20, 10, 5, 2, 1]

# â”€â”€â”€ Telegram API helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def tg_send(method, **kwargs):
    r = req.post(
        f"https://api.telegram.org/bot{BOT_TOKEN}/{method}",
        json=kwargs,
        timeout=10,
    )
    return r.json()


def send_message(chat_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_send("sendMessage", **payload)


def edit_message(chat_id, message_id, text, reply_markup=None):
    payload = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return tg_send("editMessageText", **payload)


def answer_callback(callback_id, text=""):
    return tg_send("answerCallbackQuery", callback_query_id=callback_id, text=text)

# â”€â”€â”€ Conversation State â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_state(conn, user_id):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT state, data FROM bot_state WHERE user_id = %s", (user_id,)
        )
        row = cur.fetchone()
    if row:
        state, data_str = row
        data = {}
        if data_str:
            try:
                data = json.loads(data_str)
            except Exception:
                pass
        return {"state": state, "data": data}
    return {"state": None, "data": {}}


def set_state(conn, user_id, state, data=None):
    if data is None:
        data = {}
    now = datetime.now(timezone.utc).isoformat()
    data_str = json.dumps(data)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bot_state (user_id, state, data, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id) DO UPDATE SET
                state = EXCLUDED.state,
                data = EXCLUDED.data,
                updated_at = EXCLUDED.updated_at
        """, (user_id, state, data_str, now))
    conn.commit()


def clear_state(conn, user_id):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bot_state WHERE user_id = %s", (user_id,))
    conn.commit()

# â”€â”€â”€ Bill parser â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def parse_denominations(text):
    """
    ĐźĐ°Ń€ŃĐ¸Ń‚ŃŚ Ń„ĐľŃ€ĐĽĐ°Ń‚: '500x1 100x3 50x2' Đ°Đ±Đľ '500 1\n100 3'
    ĐźĐľĐ˛ĐµŃ€Ń‚Đ°Ń” (total, breakdown_str) Đ°Đ±Đľ (None, error_str)
    """
    text = re.sub(r'[xX\*=]', ' ', text)
    pairs = re.findall(r'(\d+)\s+(\d+)', text)
    if not pairs:
        return None, "ĐťĐµ Đ˛Đ´Đ°Đ»ĐľŃŃŚ Ń€ĐľĐ·ĐżŃ–Đ·Đ˝Đ°Ń‚Đ¸. Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń Ń„ĐľŃ€ĐĽĐ°Ń‚Ń–:\n500x1 100x3 50x2 20x5 10x10"

    total = 0
    lines = []
    for denom_str, qty_str in pairs:
        denom = int(denom_str)
        qty = int(qty_str)
        if denom not in DENOMINATIONS:
            return None, f"ĐťĐµĐ˛Ń–Đ´ĐľĐĽĐ¸Đą Đ˝ĐľĐĽŃ–Đ˝Đ°Đ»: {denom} {CURRENCY}\nĐ”ĐľĐ·Đ˛ĐľĐ»ĐµĐ˝Ń–: {', '.join(str(d) for d in DENOMINATIONS)}"
        subtotal = denom * qty
        total += subtotal
        lines.append(f"  {denom} {CURRENCY} x {qty} = {subtotal} {CURRENCY}")

    breakdown = "\n".join(lines)
    return total, breakdown

# â”€â”€â”€ Keyboards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main_menu_keyboard(modules: dict):
    """New main menu: snapshot + close day + optional report."""
    buttons = [
        [
            {"text": "đź“¸ ĐźĐľŃ‚ĐľŃ‡Đ˝Đ¸Đą Đ·Ń€Ń–Đ·", "callback_data": "snap:start"},
            {"text": "đź”’ Đ—Đ°ĐşŃ€Đ¸Ń‚Đ¸ Đ´ĐµĐ˝ŃŚ", "callback_data": "close:start"},
        ]
    ]
    if modules.get("reports"):
        buttons.append([{"text": "đź“Š ĐśŃ–Đą Đ·Đ˛Ń–Ń‚", "callback_data": "report:today"}])
    return {"inline_keyboard": buttons}


def admin_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "đź‘Ą ĐšĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡Ń–", "callback_data": "admin:users"}],
            [{"text": "âŹł ĐžŃ‡Ń–ĐşŃŃŽŃ‚ŃŚ",    "callback_data": "admin:pending"}],
            [{"text": "đź“Š Đ—Đ˛Ń–Ń‚ ŃŃŚĐľĐłĐľĐ´Đ˝Ń–", "callback_data": "admin:report"}],
            [{"text": "đź’µ Đ’ĐżĐ»Đ°Ń‚Đ°",       "callback_data": "admin:deposit"},
             {"text": "đź’¸ Đ’Đ¸ĐżĐ»Đ°Ń‚Đ°",      "callback_data": "admin:withdrawal"}],
            [{"text": "đź§ľ Đ’Đ¸Ń‚Ń€Đ°Ń‚Đ¸",      "callback_data": "admin:expenses"}],
        ]
    }


def user_manage_keyboard(uid):
    return {
        "inline_keyboard": [
            [
                {"text": "đź‘¨â€ŤđźŤł Chef",    "callback_data": f"setrole:{uid}:chef"},
                {"text": "â• Barista", "callback_data": f"setrole:{uid}:barista"},
                {"text": "đźŚ± Young",  "callback_data": f"setrole:{uid}:young"},
                {"text": "đź”‘ Admin",  "callback_data": f"setrole:{uid}:admin"},
            ],
            [{"text": "đź”§ ĐśĐľĐ´ŃĐ»Ń–",       "callback_data": f"modules:{uid}"}],
            [{"text": "đźš« Đ’Ń–Đ´ĐşĐ»Đ¸ĐşĐ°Ń‚Đ¸",   "callback_data": f"revoke:{uid}"}],
            [{"text": "â—€ď¸Ź ĐťĐ°Đ·Đ°Đ´",        "callback_data": "admin:users"}],
        ]
    }


def modules_keyboard(uid, current_modules: dict):
    label_map = {
        "cash_income":  "💵 Готівка",
        "card_income":  "💳 Картка",
        "coffee_count": "☕ Порції",
        "deposits":     "📥 Вплата",
        "withdrawals":  "📤 Виплата",
        "expenses":     "🧾 Витрати",
        "reports":      "📊 Звіт",
        "shifts":       "📅 Графік змін",
    }
    rows = []
    for mod, label in label_map.items():
        enabled = current_modules.get(mod, False)
        icon = "✅" if enabled else "❌"
        rows.append([{
            "text": f"{icon} {label}",
            "callback_data": f"togglemod:{uid}:{mod}:{0 if enabled else 1}",
        }])
    rows.append([{"text": "◀️ Назад", "callback_data": f"user:{uid}"}])
    return {"inline_keyboard": rows}

def pending_role_keyboard(uid):
    return {
        "inline_keyboard": [
            [
                {"text": "đź‘¨â€ŤđźŤł Chef",    "callback_data": f"setrole:{uid}:chef"},
                {"text": "â• Barista", "callback_data": f"setrole:{uid}:barista"},
                {"text": "đźŚ± Young",  "callback_data": f"setrole:{uid}:young"},
                {"text": "đź”‘ Admin",  "callback_data": f"setrole:{uid}:admin"},
            ],
            [{"text": "đźš« Đ’Ń–Đ´Ń…Đ¸Đ»Đ¸Ń‚Đ¸", "callback_data": f"revoke:{uid}"}],
        ]
    }


def cancel_keyboard():
    return {"inline_keyboard": [[{"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸", "callback_data": "menu:cancel"}]]}


def skip_cancel_keyboard():
    return {"inline_keyboard": [
        [{"text": "âŹ­ ĐźŃ€ĐľĐżŃŃŃ‚Đ¸Ń‚Đ¸", "callback_data": "menu:skip"},
         {"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸", "callback_data": "menu:cancel"}]
    ]}

# â”€â”€â”€ Summary formatters â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def format_daily_summary(s: dict) -> str:
    date_str = s.get('date', '')
    lines = [f"Đ—Đ˛Ń–Ń‚ Đ·Đ° {date_str}", ""]
    opening = s.get('opening_cash', 0) or 0
    closing = s.get('closing_cash')
    cash_income = s.get('cash_income', 0) or 0
    card_income = s.get('card_income', 0) or 0
    coffee = s.get('coffee_portions', 0) or 0
    expenses = s.get('expenses', 0) or 0
    is_finalized = s.get('is_finalized', False)

    lines.append(f"Đ ĐľĐ·ĐĽŃ–Đ˝ĐşĐ°: {opening:.2f} {CURRENCY}")
    if closing is not None:
        lines.append(f"ĐšĐ°ŃĐ° (Đ·Đ°ĐşŃ€Đ¸Ń‚Đ°): {closing:.2f} {CURRENCY}")
        lines.append(f"Đ’Đ¸Ń€ŃŃ‡ĐşĐ° ĐłĐľŃ‚Ń–Đ˛ĐşĐ°: {cash_income:.2f} {CURRENCY}")
    else:
        lines.append("ĐšĐ°ŃĐ°: Đ˝Đµ Đ·Đ°ĐşŃ€Đ¸Ń‚Đľ")
    lines.append(f"ĐšĐ°Ń€Ń‚ĐşĐ°: {card_income:.2f} {CURRENCY}")
    lines.append(f"ĐźĐľŃ€Ń†Ń–Ń— ĐşĐ°Đ˛Đ¸: {coffee} ŃŃ‚")
    if expenses:
        lines.append(f"Đ’Đ¸Ń‚Ń€Đ°Ń‚Đ¸: {expenses:.2f} {CURRENCY}")

    snapshots = s.get('snapshots', [])
    if snapshots:
        lines.append("")
        lines.append(f"Đ—Ń€Ń–Đ·Đ¸ ({len(snapshots)}):")
        for snap in snapshots:
            parts = [snap.get('time', '')]
            if snap.get('cash_amount') is not None:
                parts.append(f"ĐşĐ°ŃĐ° {snap['cash_amount']:.0f} {CURRENCY}")
            if snap.get('coffee_portions') is not None:
                parts.append(f"ĐşĐ°Đ˛Đ° {snap['coffee_portions']} ŃŃ‚")
            lines.append("  " + " | ".join(parts))

    lines.append("")
    if is_finalized:
        closed_at = s.get('closed_at', '')
        lines.append(f"Đ”ĐµĐ˝ŃŚ Đ·Đ°ĐşŃ€Đ¸Ń‚Đľ {closed_at[:16] if closed_at else ''}")
    else:
        lines.append("Đ”ĐµĐ˝ŃŚ Ń‰Đµ Đ˝Đµ Đ·Đ°ĐşŃ€Đ¸Ń‚Đľ")
    return "\n".join(lines)


def format_close_confirm(session: dict, date_str: str) -> str:
    opening = session.get('opening_cash', 0) or 0
    closing = session.get('closing_cash', 0) or 0
    cash_income = closing - opening
    coffee = session.get('coffee_portions', 0) or 0
    card = session.get('card_income', 0) or 0
    return (
        f"Đ—Đ°ĐşŃ€Đ¸Ń‚Ń‚ŃŹ Đ´Đ˝ŃŹ {date_str}\n\n"
        f"Đ“ĐľŃ‚Ń–Đ˛ĐşĐ° Đ˛ ĐşĐ°ŃŃ–: {closing:.2f} {CURRENCY}\n"
        f"Đ’Đ¸Ń€ŃŃ‡ĐşĐ° ĐłĐľŃ‚Ń–Đ˛ĐşĐ°: {cash_income:.2f} {CURRENCY} (ĐşĐ°ŃĐ° - Ń€ĐľĐ·ĐĽŃ–Đ˝ĐşĐ°)\n"
        f"ĐźĐľŃ€Ń†Ń–Ń— ĐşĐ°Đ˛Đ¸: {coffee} ŃŃ‚\n"
        f"ĐšĐ°Ń€Ń‚ĐşĐ°: {card:.2f} {CURRENCY}"
    )

# â”€â”€â”€ Command handlers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def handle_start(conn, user_id, username, first_name, last_name, chat_id):
    save_user_raw(conn, user_id, username, first_name, last_name)
    ensure_tables(conn)
    clear_state(conn, user_id)

    user = get_user(conn, user_id)
    if user and user["is_approved"] and user["role"]:
        modules = get_user_modules(conn, user_id)
        send_message(
            chat_id,
            f"ĐźŃ€Đ¸Đ˛Ń–Ń‚, {first_name}! ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:",
            reply_markup=main_menu_keyboard(modules),
        )
    else:
        send_message(
            chat_id,
            "Đ’Đ°Ń Đ·Đ°ĐżĐ¸Ń‚ Đ˝Đ° Đ´ĐľŃŃ‚ŃĐż Đ˝Đ°Đ´Ń–ŃĐ»Đ°Đ˝Đľ Đ°Đ´ĐĽŃ–Đ˝Ń–ŃŃ‚Ń€Đ°Ń‚ĐľŃ€Ń. Đ—Đ°Ń‡ĐµĐşĐ°ĐąŃ‚Đµ ĐżŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¶ĐµĐ˝Đ˝ŃŹ.",
        )
        display = f"@{username}" if username else f"{first_name or ''} {last_name or ''}".strip()
        tg_send(
            "sendMessage",
            chat_id=ADMIN_ID,
            text=f"ĐťĐľĐ˛Đ¸Đą ĐşĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡ Ń…ĐľŃ‡Đµ Đ´ĐľŃŃ‚ŃĐż:\n{display} (id: {user_id})\nĐźŃ€Đ¸Đ·Đ˝Đ°Ń‡Ń‚Đµ Ń€ĐľĐ»ŃŚ:",
            reply_markup=pending_role_keyboard(user_id),
        )


def handle_admin(conn, user_id, chat_id):
    if not is_admin(conn, user_id):
        send_message(chat_id, "ĐťĐµĐĽĐ°Ń” Đ´ĐľŃŃ‚ŃĐżŃ.")
        return
    send_message(chat_id, "ĐĐ´ĐĽŃ–Đ˝ ĐżĐ°Đ˝ĐµĐ»ŃŚ:", reply_markup=admin_keyboard())

# â”€â”€â”€ Snapshot flow helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def start_snapshot_flow(conn, chat_id, message_id=None):
    keyboard = {"inline_keyboard": [
        [{"text": "â• Đ˘Ń–Đ»ŃŚĐşĐ¸ ĐżĐľŃ€Ń†Ń–Ń— ĐşĐ°Đ˛Đ¸", "callback_data": "snap:coffee_only"}],
        [{"text": "đź’° ĐšĐ°ŃĐ° + ĐżĐľŃ€Ń†Ń–Ń—",      "callback_data": "snap:cash_and_coffee"}],
        [{"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸",          "callback_data": "menu:cancel"}],
    ]}
    if message_id:
        edit_message(chat_id, message_id, "Đ©Đľ Đ˛Đ˝ĐľŃĐ¸ĐĽĐľ?", reply_markup=keyboard)
    else:
        send_message(chat_id, "Đ©Đľ Đ˛Đ˝ĐľŃĐ¸ĐĽĐľ?", reply_markup=keyboard)


def start_close_flow(conn, user_id, chat_id, message_id=None):
    date_str = today_str()
    session = get_session(conn, date_str)

    if session and session.get('is_finalized'):
        # Already finalized â€” show current values with edit option
        text = format_close_confirm(session, date_str)
        text += "\n\nĐ”ĐµĐ˝ŃŚ Đ˛Đ¶Đµ Đ·Đ°ĐşŃ€Đ¸Ń‚Đľ."
        keyboard = {"inline_keyboard": [
            [{"text": "âśŹď¸Ź Đ—ĐĽŃ–Đ˝Đ¸Ń‚Đ¸", "callback_data": "close:edit"}],
            [{"text": "â—€ď¸Ź ĐťĐ°Đ·Đ°Đ´",   "callback_data": "menu:cancel"}],
        ]}
        if message_id:
            edit_message(chat_id, message_id, text, reply_markup=keyboard)
        else:
            send_message(chat_id, text, reply_markup=keyboard)
        return

    keyboard = {"inline_keyboard": [
        [{"text": "đź’µ Đ’Đ˛ĐµŃŃ‚Đ¸ ŃŃĐĽŃ",          "callback_data": "close:cash_sum"}],
        [{"text": "đźŞ™ ĐźĐľŃ€Đ°Ń…ŃĐ˛Đ°Ń‚Đ¸ ĐşŃĐżŃŽŃ€Đ¸",     "callback_data": "close:cash_bills"}],
        [{"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸",             "callback_data": "menu:cancel"}],
    ]}
    text = "Đ—Đ°ĐşŃ€Đ¸Ń‚Ń‚ŃŹ Đ´Đ˝ŃŹ\n\nĐ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ ĐłĐľŃ‚Ń–Đ˛ĐşĐ¸ Đ˛ ĐşĐ°ŃŃ–:"
    if message_id:
        edit_message(chat_id, message_id, text, reply_markup=keyboard)
    else:
        send_message(chat_id, text, reply_markup=keyboard)

# â”€â”€â”€ Callback query handler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def handle_callback(conn, callback_id, user_id, chat_id, message_id, data):
    answer_callback(callback_id)

    # â”€â”€ Admin panel â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if data == "admin:users":
        if not is_admin(conn, user_id):
            return
        # Only super-admin can see user management; admins see report only
        if int(user_id) != ADMIN_ID:
            edit_message(chat_id, message_id, "ĐťĐµĐĽĐ°Ń” Đ´ĐľŃŃ‚ŃĐżŃ Đ´Đľ ŃĐżĐ¸ŃĐşŃ ĐşĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡Ń–Đ˛.", reply_markup=admin_keyboard())
            return
        users = get_all_users(conn)
        if not users:
            edit_message(chat_id, message_id, "ĐťĐµĐĽĐ°Ń” ĐşĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡Ń–Đ˛.", reply_markup=admin_keyboard())
            return
        rows = []
        for u in users:
            display = f"@{u['username']}" if u.get("username") else (u.get("first_name") or str(u["id"]))
            role = u.get("role") or "â€”"
            rows.append([{"text": f"{display} [{role}]", "callback_data": f"user:{u['id']}"}])
        rows.append([{"text": "â—€ď¸Ź ĐťĐ°Đ·Đ°Đ´", "callback_data": "admin:back"}])
        edit_message(chat_id, message_id, "Đ’ŃŃ– ĐşĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡Ń–:", reply_markup={"inline_keyboard": rows})

    elif data == "admin:pending":
        if not is_admin(conn, user_id):
            return
        if int(user_id) != ADMIN_ID:
            edit_message(chat_id, message_id, "ĐťĐµĐĽĐ°Ń” Đ´ĐľŃŃ‚ŃĐżŃ.", reply_markup=admin_keyboard())
            return
        users = get_pending_users(conn)
        if not users:
            edit_message(chat_id, message_id, "ĐťĐµĐĽĐ°Ń” ĐľŃ‡Ń–ĐşŃŃŽŃ‡Đ¸Ń….", reply_markup=admin_keyboard())
            return
        rows = []
        for u in users:
            display = f"@{u['username']}" if u.get("username") else (u.get("first_name") or str(u["id"]))
            rows.append([{"text": display, "callback_data": f"user:{u['id']}"}])
        rows.append([{"text": "â—€ď¸Ź ĐťĐ°Đ·Đ°Đ´", "callback_data": "admin:back"}])
        edit_message(chat_id, message_id, "ĐžŃ‡Ń–ĐşŃŃŽŃ‚ŃŚ Đ´ĐľŃŃ‚ŃĐżŃ:", reply_markup={"inline_keyboard": rows})

    elif data == "admin:report":
        if not is_admin(conn, user_id):
            return
        date_str = today_str()
        s = get_daily_summary(conn, date_str)
        text = format_daily_summary(s)
        edit_message(chat_id, message_id, text, reply_markup=admin_keyboard())

    elif data == "admin:back":
        if not is_admin(conn, user_id):
            return
        edit_message(chat_id, message_id, "ĐĐ´ĐĽŃ–Đ˝ ĐżĐ°Đ˝ĐµĐ»ŃŚ:", reply_markup=admin_keyboard())

    # â”€â”€ Admin: deposit / withdrawal / expenses â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif data == "admin:deposit":
        if int(user_id) != ADMIN_ID:
            return
        set_state(conn, user_id, "admin_deposit")
        edit_message(chat_id, message_id, f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ Đ˛ĐżĐ»Đ°Ń‚Đ¸ Đ˛ ĐşĐ°ŃŃ ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "admin:withdrawal":
        if int(user_id) != ADMIN_ID:
            return
        set_state(conn, user_id, "admin_withdrawal")
        edit_message(chat_id, message_id, f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ Đ˛Đ¸ĐżĐ»Đ°Ń‚Đ¸ Đ· ĐşĐ°ŃĐ¸ ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "admin:expenses":
        if int(user_id) != ADMIN_ID:
            return
        set_state(conn, user_id, "admin_expenses")
        edit_message(chat_id, message_id, f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ Đ˛Đ¸Ń‚Ń€Đ°Ń‚ ({CURRENCY}):", reply_markup=cancel_keyboard())

    # â”€â”€ User detail â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif data.startswith("user:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        u = get_user(conn, uid)
        if not u:
            edit_message(chat_id, message_id, "ĐšĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡Đ° Đ˝Đµ Đ·Đ˝Đ°ĐąĐ´ĐµĐ˝Đľ.", reply_markup=admin_keyboard())
            return
        display = f"@{u['username']}" if u.get("username") else (u.get("first_name") or str(uid))
        role = u.get("role") or "â€”"
        approved = "Đ˘Đ°Đş" if u.get("is_approved") else "ĐťŃ–"
        text = f"ĐšĐľŃ€Đ¸ŃŃ‚ŃĐ˛Đ°Ń‡: {display}\nĐ ĐľĐ»ŃŚ: {role}\nĐźŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¶ĐµĐ˝Đľ: {approved}"
        edit_message(chat_id, message_id, text, reply_markup=user_manage_keyboard(uid))

    elif data.startswith("setrole:"):
        if int(user_id) != ADMIN_ID:
            return
        _, uid_str, role = data.split(":")
        uid = int(uid_str)
        set_role(conn, uid, role)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else uid_str
        role_labels = {"chef": "Đ¨ĐµŃ„", "barista": "Đ‘Đ°Ń€Ń–ŃŃ‚Đ°", "young": "ĐˇŃ‚Đ°Đ¶ĐµŃ€", "admin": "ĐĐ´ĐĽŃ–Đ˝"}
        role_label = role_labels.get(role, role)
        edit_message(chat_id, message_id,
            f"Đ ĐľĐ»ŃŚ {role_label} ĐżŃ€Đ¸Đ·Đ˝Đ°Ń‡ĐµĐ˝Đľ Đ´Đ»ŃŹ {display}.",
            reply_markup=user_manage_keyboard(uid))
        tg_send("sendMessage", chat_id=uid, text=f"Đ’Đ°Ń Đ´ĐľŃŃ‚ŃĐż ĐżŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¶ĐµĐ˝Đľ. Đ ĐľĐ»ŃŚ: {role_label}.\nĐťĐ°Ń‚Đ¸ŃĐ˝Ń–Ń‚ŃŚ /start")

    elif data.startswith("revoke:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        revoke_access(conn, uid)
        edit_message(chat_id, message_id, "Đ”ĐľŃŃ‚ŃĐż Đ˛Ń–Đ´ĐşĐ»Đ¸ĐşĐ°Đ˝Đľ.", reply_markup=admin_keyboard())
        tg_send("sendMessage", chat_id=uid, text="Đ’Đ°Ń Đ´ĐľŃŃ‚ŃĐż Đ±ŃĐ»Đľ Đ˛Ń–Đ´ĐşĐ»Đ¸ĐşĐ°Đ˝Đľ.")

    elif data.startswith("modules:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        mods = get_user_modules(conn, uid)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else str(uid)
        edit_message(chat_id, message_id, f"ĐśĐľĐ´ŃĐ»Ń– Đ´Đ»ŃŹ {display}:", reply_markup=modules_keyboard(uid, mods))

    elif data.startswith("togglemod:"):
        if int(user_id) != ADMIN_ID:
            return
        _, uid_str, module, val_str = data.split(":")
        uid = int(uid_str)
        enabled = int(val_str) == 1
        set_module_access(conn, uid, module, enabled)
        mods = get_user_modules(conn, uid)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else uid_str
        edit_message(chat_id, message_id, f"ĐśĐľĐ´ŃĐ»Ń– Đ´Đ»ŃŹ {display}:", reply_markup=modules_keyboard(uid, mods))

    # â”€â”€ Cancel / Skip â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif data == "menu:cancel":
        clear_state(conn, user_id)
        user = get_user(conn, user_id)
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id, "ĐˇĐşĐ°ŃĐľĐ˛Đ°Đ˝Đľ. ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))

    elif data == "menu:skip":
        st_info = get_state(conn, user_id)
        state = st_info.get("state")
        state_data = st_info.get("data", {})
        modules = get_user_modules(conn, user_id)
        _handle_skip(conn, user_id, chat_id, message_id, state, state_data, modules)

    # â”€â”€ Report: today â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif data == "report:today":
        date_str = today_str()
        s = get_daily_summary(conn, date_str)
        text = format_daily_summary(s)
        edit_message(chat_id, message_id, text, reply_markup={
            "inline_keyboard": [[{"text": "â—€ď¸Ź ĐśĐµĐ˝ŃŽ", "callback_data": "menu:back"}]]
        })

    elif data == "menu:back":
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))

    # â”€â”€ Snapshot flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif data == "snap:start":
        user = get_user(conn, user_id)
        if not user or not user["is_approved"]:
            send_message(chat_id, "ĐŁ Đ˛Đ°Ń Đ˝ĐµĐĽĐ°Ń” Đ´ĐľŃŃ‚ŃĐżŃ.")
            return
        start_snapshot_flow(conn, chat_id, message_id)

    elif data == "snap:coffee_only":
        set_state(conn, user_id, "snapshot_coffee", {"type": "coffee_only"})
        edit_message(chat_id, message_id, "ĐšŃ–Đ»ŃŚĐşŃ–ŃŃ‚ŃŚ ĐżĐľŃ€Ń†Ń–Đą ĐşĐ°Đ˛Đ¸:", reply_markup=cancel_keyboard())

    elif data == "snap:cash_and_coffee":
        set_state(conn, user_id, "snapshot_cash_method", {"type": "cash_and_coffee"})
        keyboard = {"inline_keyboard": [
            [{"text": "đź’µ Đ’Đ˛ĐµŃŃ‚Đ¸ ŃŃĐĽŃ",      "callback_data": "snap:cash_sum"}],
            [{"text": "đźŞ™ ĐźĐľŃ€Đ°Ń…ŃĐ˛Đ°Ń‚Đ¸ ĐşŃĐżŃŽŃ€Đ¸", "callback_data": "snap:cash_bills"}],
            [{"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸",         "callback_data": "menu:cancel"}],
        ]}
        edit_message(chat_id, message_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ ŃĐżĐľŃŃ–Đ± Đ˛Đ˛ĐµĐ´ĐµĐ˝Đ˝ŃŹ ĐłĐľŃ‚Ń–Đ˛ĐşĐ¸:", reply_markup=keyboard)

    elif data == "snap:cash_sum":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["cash_method"] = "sum"
        set_state(conn, user_id, "snapshot_cash_amount", state_data)
        edit_message(chat_id, message_id, f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ ĐłĐľŃ‚Ń–Đ˛ĐşĐ¸ ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "snap:cash_bills":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["cash_method"] = "bills"
        set_state(conn, user_id, "snapshot_cash_bills", state_data)
        edit_message(chat_id, message_id,
            f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ĐşŃĐżŃŽŃ€Đ¸ Ń Ń„ĐľŃ€ĐĽĐ°Ń‚Ń–:\n500x1 100x3 50x2\n\nĐťĐľĐĽŃ–Đ˝Đ°Đ»Đ¸: {', '.join(str(d) for d in DENOMINATIONS)} {CURRENCY}",
            reply_markup=cancel_keyboard())

    elif data == "snap:bills_confirm":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["confirmed_cash"] = state_data.get("pending_cash", 0)
        set_state(conn, user_id, "snapshot_coffee", state_data)
        edit_message(chat_id, message_id,
            "ĐšŃ–Đ»ŃŚĐşŃ–ŃŃ‚ŃŚ ĐżĐľŃ€Ń†Ń–Đą ĐşĐ°Đ˛Đ¸ (Đ°Đ±Đľ ĐżŃ€ĐľĐżŃŃŃ‚Ń–Ń‚ŃŚ):",
            reply_markup=skip_cancel_keyboard())

    # â”€â”€ Close day flow â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif data == "close:start":
        user = get_user(conn, user_id)
        if not user or not user["is_approved"]:
            send_message(chat_id, "ĐŁ Đ˛Đ°Ń Đ˝ĐµĐĽĐ°Ń” Đ´ĐľŃŃ‚ŃĐżŃ.")
            return
        get_or_create_session(conn, today_str())
        start_close_flow(conn, user_id, chat_id, message_id)

    elif data == "close:edit":
        # Re-enter close wizard even if already finalized
        keyboard = {"inline_keyboard": [
            [{"text": "đź’µ Đ’Đ˛ĐµŃŃ‚Đ¸ ŃŃĐĽŃ",      "callback_data": "close:cash_sum"}],
            [{"text": "đźŞ™ ĐźĐľŃ€Đ°Ń…ŃĐ˛Đ°Ń‚Đ¸ ĐşŃĐżŃŽŃ€Đ¸", "callback_data": "close:cash_bills"}],
            [{"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸",         "callback_data": "menu:cancel"}],
        ]}
        edit_message(chat_id, message_id, "Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ ĐłĐľŃ‚Ń–Đ˛ĐşĐ¸ Đ˛ ĐşĐ°ŃŃ–:", reply_markup=keyboard)

    elif data == "close:cash_sum":
        set_state(conn, user_id, "close_cash_amount", {})
        edit_message(chat_id, message_id, f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ŃŃĐĽŃ ĐłĐľŃ‚Ń–Đ˛ĐşĐ¸ Đ˛ ĐşĐ°ŃŃ– ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "close:cash_bills":
        set_state(conn, user_id, "close_cash_bills", {})
        edit_message(chat_id, message_id,
            f"Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ĐşŃĐżŃŽŃ€Đ¸ Ń Ń„ĐľŃ€ĐĽĐ°Ń‚Ń–:\n500x1 100x3 50x2\n\nĐťĐľĐĽŃ–Đ˝Đ°Đ»Đ¸: {', '.join(str(d) for d in DENOMINATIONS)} {CURRENCY}",
            reply_markup=cancel_keyboard())

    elif data == "close:bills_confirm":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["closing_cash"] = state_data.get("pending_cash", 0)
        set_state(conn, user_id, "close_coffee", state_data)
        edit_message(chat_id, message_id,
            "ĐšŃ–Đ»ŃŚĐşŃ–ŃŃ‚ŃŚ ĐżĐľŃ€Ń†Ń–Đą ĐşĐ°Đ˛Đ¸ Đ·Đ° Đ´ĐµĐ˝ŃŚ (Đ°Đ±Đľ ĐżŃ€ĐľĐżŃŃŃ‚Ń–Ń‚ŃŚ):",
            reply_markup=skip_cancel_keyboard())

    elif data == "close:confirm":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        date_str = today_str()
        now = datetime.now(timezone.utc).isoformat()
        update_session(conn, date_str,
            closing_cash=state_data.get("closing_cash", 0),
            coffee_portions=state_data.get("coffee_portions", 0),
            card_income=state_data.get("card_income", 0),
            is_finalized=1,
            closed_by=user_id,
            closed_at=now,
        )
        clear_state(conn, user_id)
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id,
            f"Đ”ĐµĐ˝ŃŚ {date_str} Đ·Đ°ĐşŃ€Đ¸Ń‚Đľ.",
            reply_markup=main_menu_keyboard(modules))

    elif data == "close:change":
        # Go back to the beginning of close wizard
        start_close_flow(conn, user_id, chat_id, message_id)

    else:
        # Unknown callback â€” show menu
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))


def _handle_skip(conn, user_id, chat_id, message_id, state, state_data, modules):
    """Handle skip button presses for optional steps."""
    if state == "snapshot_coffee":
        # Skip coffee in snapshot â€” save snapshot now
        _save_snapshot_and_finish(conn, user_id, chat_id, message_id, state_data, coffee=None, modules=modules)

    elif state == "close_coffee":
        state_data["coffee_portions"] = 0
        set_state(conn, user_id, "close_card", state_data)
        if message_id:
            edit_message(chat_id, message_id,
                f"ĐˇŃĐĽĐ° ĐżĐľ ĐşĐ°Ń€Ń‚Ń– ({CURRENCY}):",
                reply_markup=skip_cancel_keyboard())
        else:
            send_message(chat_id, f"ĐˇŃĐĽĐ° ĐżĐľ ĐşĐ°Ń€Ń‚Ń– ({CURRENCY}):", reply_markup=skip_cancel_keyboard())

    elif state == "close_card":
        state_data["card_income"] = 0
        set_state(conn, user_id, "close_confirm", state_data)
        _show_close_confirm(conn, user_id, chat_id, message_id, state_data)

    else:
        clear_state(conn, user_id)
        if message_id:
            edit_message(chat_id, message_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))
        else:
            send_message(chat_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))


def _save_snapshot_and_finish(conn, user_id, chat_id, message_id, state_data, coffee, modules):
    date_str = today_str()
    time_str = datetime.now(timezone.utc).strftime('%H:%M')
    cash = state_data.get("confirmed_cash") or state_data.get("cash_amount")
    coffee_val = coffee if coffee is not None else state_data.get("coffee_portions")
    add_snapshot(conn, user_id, date_str, time_str,
                 cash_amount=cash,
                 coffee_portions=int(coffee_val) if coffee_val is not None else None)
    clear_state(conn, user_id)
    parts = []
    if cash is not None:
        parts.append(f"ĐşĐ°ŃĐ° {cash:.0f} {CURRENCY}")
    if coffee_val is not None:
        parts.append(f"ĐşĐ°Đ˛Đ° {coffee_val} ŃŃ‚")
    text = "Đ—Ń€Ń–Đ· Đ·Đ±ĐµŃ€ĐµĐ¶ĐµĐ˝Đľ: " + (", ".join(parts) if parts else "â€”") + "\n\nĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:"
    if message_id:
        edit_message(chat_id, message_id, text, reply_markup=main_menu_keyboard(modules))
    else:
        send_message(chat_id, text, reply_markup=main_menu_keyboard(modules))


def _show_close_confirm(conn, user_id, chat_id, message_id, state_data):
    date_str = today_str()
    session = get_session(conn, date_str) or {}
    opening = session.get('opening_cash', 0) or 0
    closing = state_data.get("closing_cash", 0) or 0
    cash_income = closing - opening
    coffee = state_data.get("coffee_portions", 0) or 0
    card = state_data.get("card_income", 0) or 0

    text = (
        f"Đ—Đ°ĐşŃ€Đ¸Ń‚Ń‚ŃŹ Đ´Đ˝ŃŹ {date_str}\n\n"
        f"Đ“ĐľŃ‚Ń–Đ˛ĐşĐ° Đ˛ ĐşĐ°ŃŃ–: {closing:.2f} {CURRENCY}\n"
        f"Đ’Đ¸Ń€ŃŃ‡ĐşĐ° ĐłĐľŃ‚Ń–Đ˛ĐşĐ°: {cash_income:.2f} {CURRENCY} (ĐşĐ°ŃĐ° - Ń€ĐľĐ·ĐĽŃ–Đ˝ĐşĐ°)\n"
        f"ĐźĐľŃ€Ń†Ń–Ń— ĐşĐ°Đ˛Đ¸: {coffee} ŃŃ‚\n"
        f"ĐšĐ°Ń€Ń‚ĐşĐ°: {card:.2f} {CURRENCY}"
    )
    keyboard = {"inline_keyboard": [
        [{"text": "âś… ĐźŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¸Ń‚Đ¸", "callback_data": "close:confirm"},
         {"text": "âśŹď¸Ź Đ—ĐĽŃ–Đ˝Đ¸Ń‚Đ¸",    "callback_data": "close:change"}],
    ]}
    if message_id:
        edit_message(chat_id, message_id, text, reply_markup=keyboard)
    else:
        send_message(chat_id, text, reply_markup=keyboard)

# â”€â”€â”€ Text / number input handler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def handle_text(conn, user_id, chat_id, text):
    st_info = get_state(conn, user_id)
    state = st_info.get("state")
    state_data = st_info.get("data", {})

    if state is None:
        user = get_user(conn, user_id)
        if user and user["is_approved"] and user["role"]:
            modules = get_user_modules(conn, user_id)
            send_message(chat_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))
        else:
            send_message(chat_id, "ĐťĐ°Ń‚Đ¸ŃĐ˝Ń–Ń‚ŃŚ /start")
        return

    modules = get_user_modules(conn, user_id)

    # â”€â”€ Snapshot: cash amount (sum method) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if state == "snapshot_cash_amount":
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Đ‘ŃĐ´ŃŚ Đ»Đ°ŃĐşĐ°, Đ˛Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=cancel_keyboard())
            return
        state_data["confirmed_cash"] = value
        set_state(conn, user_id, "snapshot_coffee", state_data)
        send_message(chat_id, "ĐšŃ–Đ»ŃŚĐşŃ–ŃŃ‚ŃŚ ĐżĐľŃ€Ń†Ń–Đą ĐşĐ°Đ˛Đ¸ (Đ°Đ±Đľ ĐżŃ€ĐľĐżŃŃŃ‚Ń–Ń‚ŃŚ):", reply_markup=skip_cancel_keyboard())

    # â”€â”€ Snapshot: cash bills â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "snapshot_cash_bills":
        total, breakdown = parse_denominations(text)
        if total is None:
            send_message(chat_id, f"ĐźĐľĐĽĐ¸Đ»ĐşĐ°: {breakdown}", reply_markup=cancel_keyboard())
            return
        state_data["pending_cash"] = total
        set_state(conn, user_id, "snapshot_cash_bills_confirm", state_data)
        keyboard = {"inline_keyboard": [
            [{"text": "âś… ĐźŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¸Ń‚Đ¸", "callback_data": "snap:bills_confirm"},
             {"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸",  "callback_data": "menu:cancel"}]
        ]}
        send_message(chat_id,
            f"Đ ĐľĐ·Ń€Đ°Ń…ŃĐ˝ĐľĐş ĐşŃĐżŃŽŃ€:\n{breakdown}\n\nĐ Đ°Đ·ĐľĐĽ: {total} {CURRENCY}",
            reply_markup=keyboard)

    # â”€â”€ Snapshot: coffee count â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "snapshot_coffee":
        try:
            coffee = int(float(text.replace(",", ".")))
        except ValueError:
            send_message(chat_id, "Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń†Ń–Đ»Đµ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=skip_cancel_keyboard())
            return
        _save_snapshot_and_finish(conn, user_id, chat_id, None, state_data, coffee=coffee, modules=modules)

    # â”€â”€ Close: cash amount (sum method) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_cash_amount":
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Đ‘ŃĐ´ŃŚ Đ»Đ°ŃĐşĐ°, Đ˛Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=cancel_keyboard())
            return
        state_data["closing_cash"] = value
        set_state(conn, user_id, "close_coffee", state_data)
        send_message(chat_id, "ĐšŃ–Đ»ŃŚĐşŃ–ŃŃ‚ŃŚ ĐżĐľŃ€Ń†Ń–Đą ĐşĐ°Đ˛Đ¸ Đ·Đ° Đ´ĐµĐ˝ŃŚ (Đ°Đ±Đľ ĐżŃ€ĐľĐżŃŃŃ‚Ń–Ń‚ŃŚ):", reply_markup=skip_cancel_keyboard())

    # â”€â”€ Close: cash bills â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_cash_bills":
        total, breakdown = parse_denominations(text)
        if total is None:
            send_message(chat_id, f"ĐźĐľĐĽĐ¸Đ»ĐşĐ°: {breakdown}", reply_markup=cancel_keyboard())
            return
        state_data["pending_cash"] = total
        set_state(conn, user_id, "close_cash_bills_confirm", state_data)
        keyboard = {"inline_keyboard": [
            [{"text": "âś… ĐźŃ–Đ´Ń‚Đ˛ĐµŃ€Đ´Đ¸Ń‚Đ¸", "callback_data": "close:bills_confirm"},
             {"text": "âťŚ ĐˇĐşĐ°ŃŃĐ˛Đ°Ń‚Đ¸",  "callback_data": "menu:cancel"}]
        ]}
        send_message(chat_id,
            f"Đ ĐľĐ·Ń€Đ°Ń…ŃĐ˝ĐľĐş ĐşŃĐżŃŽŃ€:\n{breakdown}\n\nĐ Đ°Đ·ĐľĐĽ: {total} {CURRENCY}",
            reply_markup=keyboard)

    # â”€â”€ Close: coffee count â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_coffee":
        try:
            coffee = int(float(text.replace(",", ".")))
        except ValueError:
            send_message(chat_id, "Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń†Ń–Đ»Đµ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=skip_cancel_keyboard())
            return
        state_data["coffee_portions"] = coffee
        set_state(conn, user_id, "close_card", state_data)
        send_message(chat_id, f"ĐˇŃĐĽĐ° ĐżĐľ ĐşĐ°Ń€Ń‚Ń– ({CURRENCY}):", reply_markup=skip_cancel_keyboard())

    # â”€â”€ Close: card amount â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_card":
        try:
            card = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Đ‘ŃĐ´ŃŚ Đ»Đ°ŃĐşĐ°, Đ˛Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=skip_cancel_keyboard())
            return
        state_data["card_income"] = card
        set_state(conn, user_id, "close_confirm", state_data)
        _show_close_confirm(conn, user_id, chat_id, None, state_data)

    # â”€â”€ Admin: deposit â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "admin_deposit":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Đ‘ŃĐ´ŃŚ Đ»Đ°ŃĐşĐ°, Đ˛Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=cancel_keyboard())
            return
        add_record(conn, user_id, today_str(), cash_deposit=value)
        clear_state(conn, user_id)
        send_message(chat_id, f"Đ’ĐżĐ»Đ°Ń‚Đ° {value:.2f} {CURRENCY} Đ·Đ±ĐµŃ€ĐµĐ¶ĐµĐ˝Đ°.", reply_markup=admin_keyboard())

    # â”€â”€ Admin: withdrawal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "admin_withdrawal":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Đ‘ŃĐ´ŃŚ Đ»Đ°ŃĐşĐ°, Đ˛Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=cancel_keyboard())
            return
        add_record(conn, user_id, today_str(), cash_withdrawal=value)
        clear_state(conn, user_id)
        send_message(chat_id, f"Đ’Đ¸ĐżĐ»Đ°Ń‚Đ° {value:.2f} {CURRENCY} Đ·Đ±ĐµŃ€ĐµĐ¶ĐµĐ˝Đ°.", reply_markup=admin_keyboard())

    # â”€â”€ Admin: expenses â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "admin_expenses":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Đ‘ŃĐ´ŃŚ Đ»Đ°ŃĐşĐ°, Đ˛Đ˛ĐµĐ´Ń–Ń‚ŃŚ Ń‡Đ¸ŃĐ»Đľ.", reply_markup=cancel_keyboard())
            return
        set_state(conn, user_id, "admin_expense_note", {"amount": value})
        send_message(chat_id, "Đ’Đ˛ĐµĐ´Ń–Ń‚ŃŚ ĐżŃ€Đ¸ĐĽŃ–Ń‚ĐşŃ Đ´Đľ Đ˛Đ¸Ń‚Ń€Đ°Ń‚:", reply_markup=cancel_keyboard())

    elif state == "admin_expense_note":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        amount = state_data.get("amount", 0)
        add_record(conn, user_id, today_str(), expenses=amount, notes=text)
        clear_state(conn, user_id)
        send_message(chat_id,
            f"Đ’Đ¸Ń‚Ń€Đ°Ń‚Đ¸ {amount:.2f} {CURRENCY} Đ·Đ±ĐµŃ€ĐµĐ¶ĐµĐ˝Đľ.\nĐźŃ€Đ¸ĐĽŃ–Ń‚ĐşĐ°: {text}",
            reply_markup=admin_keyboard())

    else:
        clear_state(conn, user_id)
        send_message(chat_id, "ĐžĐ±ĐµŃ€Ń–Ń‚ŃŚ Đ´Ń–ŃŽ:", reply_markup=main_menu_keyboard(modules))

# â”€â”€â”€ Main update dispatcher â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def process_update(update: dict):
    conn = get_conn()
    try:
        ensure_tables(conn)

        if "callback_query" in update:
            cq = update["callback_query"]
            user = cq["from"]
            user_id = user["id"]
            chat_id = cq["message"]["chat"]["id"]
            message_id = cq["message"]["message_id"]
            data = cq.get("data", "")
            handle_callback(conn, cq["id"], user_id, chat_id, message_id, data)
            return

        if "message" not in update:
            return

        msg = update["message"]
        user = msg.get("from", {})
        user_id = user.get("id")
        chat_id = msg["chat"]["id"]
        text = msg.get("text", "")

        if not user_id or not text:
            return

        username   = user.get("username", "")
        first_name = user.get("first_name", "")
        last_name  = user.get("last_name", "")

        if text.startswith("/start"):
            handle_start(conn, user_id, username, first_name, last_name, chat_id)
        elif text.startswith("/admin"):
            handle_admin(conn, user_id, chat_id)
        else:
            handle_text(conn, user_id, chat_id, text)

    finally:
        conn.close()

# â”€â”€â”€ Vercel handler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            update = json.loads(body)
            process_update(update)
        except Exception as e:
            print(f"Error processing update: {e}")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, fmt, *args):
        pass
