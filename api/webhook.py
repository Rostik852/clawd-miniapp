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
CURRENCY = "zł"
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
    Parse format like '500x1 100x3 50x2' or '500 1\\n100 3'.
    Returns (total, breakdown_str) or (None, error_str).
    """
    text = re.sub(r'[xX\*=]', ' ', text)
    pairs = re.findall(r'(\d+)\s+(\d+)', text)
    if not pairs:
        return None, "Не вдалося розпізнати. Введіть у форматі:\n500x1 100x3 50x2 20x5 10x10"

    total = 0
    lines = []
    for denom_str, qty_str in pairs:
        denom = int(denom_str)
        qty = int(qty_str)
        if denom not in DENOMINATIONS:
            return None, f"Невідомий номінал: {denom} {CURRENCY}\nДозволені: {', '.join(str(d) for d in DENOMINATIONS)}"
        subtotal = denom * qty
        total += subtotal
        lines.append(f"  {denom} {CURRENCY} x {qty} = {subtotal} {CURRENCY}")

    breakdown = "\n".join(lines)
    return total, breakdown

# â”€â”€â”€ Keyboards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main_menu_keyboard(modules: dict):
    buttons = [
        [
            {"text": "📸 Поточний зріз", "callback_data": "snap:start"},
            {"text": "🔒 Закрити день", "callback_data": "close:start"},
        ]
    ]
    if modules.get("reports"):
        buttons.append([{"text": "📊 Мій звіт", "callback_data": "report:today"}])
    return {"inline_keyboard": buttons}


def admin_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "👥 Користувачі", "callback_data": "admin:users"}],
            [{"text": "⏳ Очікують", "callback_data": "admin:pending"}],
            [{"text": "📊 Звіт сьогодні", "callback_data": "admin:report"}],
            [{"text": "📥 Вплата", "callback_data": "admin:deposit"},
             {"text": "📤 Виплата", "callback_data": "admin:withdrawal"}],
            [{"text": "🧾 Витрати", "callback_data": "admin:expenses"}],
        ]
    }


def user_manage_keyboard(uid):
    return {
        "inline_keyboard": [
            [
                {"text": "👨‍🍳 Chef", "callback_data": f"setrole:{uid}:chef"},
                {"text": "☕ Barista", "callback_data": f"setrole:{uid}:barista"},
                {"text": "🌱 Young", "callback_data": f"setrole:{uid}:young"},
                {"text": "🔑 Admin", "callback_data": f"setrole:{uid}:admin"},
            ],
            [{"text": "🔧 Модулі", "callback_data": f"modules:{uid}"}],
            [{"text": "🚫 Відкликати", "callback_data": f"revoke:{uid}"}],
            [{"text": "◀️ Назад", "callback_data": "admin:users"}],
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
                {"text": "👨‍🍳 Chef", "callback_data": f"setrole:{uid}:chef"},
                {"text": "☕ Barista", "callback_data": f"setrole:{uid}:barista"},
                {"text": "🌱 Young", "callback_data": f"setrole:{uid}:young"},
                {"text": "🔑 Admin", "callback_data": f"setrole:{uid}:admin"},
            ],
            [{"text": "🚫 Відхилити", "callback_data": f"revoke:{uid}"}],
        ]
    }


def cancel_keyboard():
    return {"inline_keyboard": [[{"text": "❌ Скасувати", "callback_data": "menu:cancel"}]]}


def skip_cancel_keyboard():
    return {"inline_keyboard": [
        [{"text": "⏭ Пропустити", "callback_data": "menu:skip"},
         {"text": "❌ Скасувати", "callback_data": "menu:cancel"}]
    ]}

# â”€â”€â”€ Summary formatters â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def format_daily_summary(s: dict) -> str:
    date_str = s.get('date', '')
    lines = [f"Звіт за {date_str}", ""]
    opening = s.get('opening_cash', 0) or 0
    closing = s.get('closing_cash')
    cash_income = s.get('cash_income', 0) or 0
    card_income = s.get('card_income', 0) or 0
    coffee = s.get('coffee_portions', 0) or 0
    expenses = s.get('expenses', 0) or 0
    is_finalized = s.get('is_finalized', False)

    lines.append(f"Розмінка: {opening:.2f} {CURRENCY}")
    if closing is not None:
        lines.append(f"Каса (закрита): {closing:.2f} {CURRENCY}")
        lines.append(f"Виручка готівка: {cash_income:.2f} {CURRENCY}")
    else:
        lines.append("Каса: не закрито")
    lines.append(f"Картка: {card_income:.2f} {CURRENCY}")
    lines.append(f"Порції кави: {coffee} шт")
    if expenses:
        lines.append(f"Витрати: {expenses:.2f} {CURRENCY}")

    snapshots = s.get('snapshots', [])
    if snapshots:
        lines.append("")
        lines.append(f"Зрізи ({len(snapshots)}):")
        for snap in snapshots:
            parts = [snap.get('time', '')]
            if snap.get('cash_amount') is not None:
                parts.append(f"каса {snap['cash_amount']:.0f} {CURRENCY}")
            if snap.get('coffee_portions') is not None:
                parts.append(f"кава {snap['coffee_portions']} шт")
            lines.append("  " + " | ".join(parts))

    lines.append("")
    if is_finalized:
        closed_at = s.get('closed_at', '')
        lines.append(f"День закрито {closed_at[:16] if closed_at else ''}")
    else:
        lines.append("День ще не закрито")
    return "\n".join(lines)


def format_close_confirm(session: dict, date_str: str) -> str:
    opening = session.get('opening_cash', 0) or 0
    closing = session.get('closing_cash', 0) or 0
    cash_income = closing - opening
    coffee = session.get('coffee_portions', 0) or 0
    card = session.get('card_income', 0) or 0
    return (
        f"Закриття дня {date_str}\n\n"
        f"Готівка в касі: {closing:.2f} {CURRENCY}\n"
        f"Виручка готівка: {cash_income:.2f} {CURRENCY} (каса - розмінка)\n"
        f"Порції кави: {coffee} шт\n"
        f"Картка: {card:.2f} {CURRENCY}"
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
            f"Привіт, {first_name}! Оберіть дію:",
            reply_markup=main_menu_keyboard(modules),
        )
    else:
        send_message(
            chat_id,
            "Ваш запит на доступ надіслано адміністратору. Зачекайте підтвердження.",
        )
        display = f"@{username}" if username else f"{first_name or ''} {last_name or ''}".strip()
        tg_send(
            "sendMessage",
            chat_id=ADMIN_ID,
            text=f"Новий користувач хоче доступ:\n{display} (id: {user_id})\nПризначте роль:",
            reply_markup=pending_role_keyboard(user_id),
        )


def handle_admin(conn, user_id, chat_id):
    if not is_admin(conn, user_id):
        send_message(chat_id, "Немає доступу.")
        return
    send_message(chat_id, "Адмін панель:", reply_markup=admin_keyboard())

# â”€â”€â”€ Snapshot flow helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def start_snapshot_flow(conn, chat_id, message_id=None):
    keyboard = {"inline_keyboard": [
        [{"text": "☕ Тільки порції кави", "callback_data": "snap:coffee_only"}],
        [{"text": "💰 Каса + порції", "callback_data": "snap:cash_and_coffee"}],
        [{"text": "❌ Скасувати", "callback_data": "menu:cancel"}],
    ]}
    if message_id:
        edit_message(chat_id, message_id, "Що вносимо?", reply_markup=keyboard)
    else:
        send_message(chat_id, "Що вносимо?", reply_markup=keyboard)


def start_close_flow(conn, user_id, chat_id, message_id=None):
    date_str = today_str()
    session = get_session(conn, date_str)

    if session and session.get('is_finalized'):
        # Already finalized â€” show current values with edit option
        text = format_close_confirm(session, date_str)
        text += "\n\nДень вже закрито."
        keyboard = {"inline_keyboard": [
            [{"text": "✏️ Змінити", "callback_data": "close:edit"}],
            [{"text": "◀️ Назад", "callback_data": "menu:cancel"}],
        ]}
        if message_id:
            edit_message(chat_id, message_id, text, reply_markup=keyboard)
        else:
            send_message(chat_id, text, reply_markup=keyboard)
        return

    keyboard = {"inline_keyboard": [
        [{"text": "💵 Ввести суму", "callback_data": "close:cash_sum"}],
        [{"text": "🪙 Порахувати купюри", "callback_data": "close:cash_bills"}],
        [{"text": "❌ Скасувати", "callback_data": "menu:cancel"}],
    ]}
    text = "Закриття дня\n\nВведіть суму готівки в касі:"
    if message_id:
        edit_message(chat_id, message_id, text, reply_markup=keyboard)
    else:
        send_message(chat_id, text, reply_markup=keyboard)

# â”€â”€â”€ Callback query handler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def handle_callback(conn, callback_id, user_id, chat_id, message_id, data):
    answer_callback(callback_id)

    # Admin panel
    if data == "admin:users":
        if not is_admin(conn, user_id):
            return
        if int(user_id) != ADMIN_ID:
            edit_message(chat_id, message_id, "Немає доступу до списку користувачів.", reply_markup=admin_keyboard())
            return
        users = get_all_users(conn)
        if not users:
            edit_message(chat_id, message_id, "Немає користувачів.", reply_markup=admin_keyboard())
            return
        rows = []
        for u in users:
            display = f"@{u['username']}" if u.get("username") else (u.get("first_name") or str(u["id"]))
            role = u.get("role") or "—"
            rows.append([{"text": f"{display} [{role}]", "callback_data": f"user:{u['id']}"}])
        rows.append([{"text": "◀️ Назад", "callback_data": "admin:back"}])
        edit_message(chat_id, message_id, "Всі користувачі:", reply_markup={"inline_keyboard": rows})

    elif data == "admin:pending":
        if not is_admin(conn, user_id):
            return
        if int(user_id) != ADMIN_ID:
            edit_message(chat_id, message_id, "Немає доступу.", reply_markup=admin_keyboard())
            return
        users = get_pending_users(conn)
        if not users:
            edit_message(chat_id, message_id, "Немає очікуючих.", reply_markup=admin_keyboard())
            return
        rows = []
        for u in users:
            display = f"@{u['username']}" if u.get("username") else (u.get("first_name") or str(u["id"]))
            rows.append([{"text": display, "callback_data": f"user:{u['id']}"}])
        rows.append([{"text": "◀️ Назад", "callback_data": "admin:back"}])
        edit_message(chat_id, message_id, "Очікують доступу:", reply_markup={"inline_keyboard": rows})

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
        edit_message(chat_id, message_id, "Адмін панель:", reply_markup=admin_keyboard())

    # Admin: deposit / withdrawal / expenses
    elif data == "admin:deposit":
        if int(user_id) != ADMIN_ID:
            return
        set_state(conn, user_id, "admin_deposit")
        edit_message(chat_id, message_id, f"Введіть суму вплати в касу ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "admin:withdrawal":
        if int(user_id) != ADMIN_ID:
            return
        set_state(conn, user_id, "admin_withdrawal")
        edit_message(chat_id, message_id, f"Введіть суму виплати з каси ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "admin:expenses":
        if int(user_id) != ADMIN_ID:
            return
        set_state(conn, user_id, "admin_expenses")
        edit_message(chat_id, message_id, f"Введіть суму витрат ({CURRENCY}):", reply_markup=cancel_keyboard())

    # User detail
    elif data.startswith("user:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        u = get_user(conn, uid)
        if not u:
            edit_message(chat_id, message_id, "Користувача не знайдено.", reply_markup=admin_keyboard())
            return
        display = f"@{u['username']}" if u.get("username") else (u.get("first_name") or str(uid))
        role = u.get("role") or "—"
        approved = "Так" if u.get("is_approved") else "Ні"
        text = f"Користувач: {display}\nРоль: {role}\nПідтверджено: {approved}"
        edit_message(chat_id, message_id, text, reply_markup=user_manage_keyboard(uid))

    elif data.startswith("setrole:"):
        if int(user_id) != ADMIN_ID:
            return
        _, uid_str, role = data.split(":")
        uid = int(uid_str)
        set_role(conn, uid, role)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else uid_str
        role_labels = {"chef": "Шеф", "barista": "Баріста", "young": "Стажер", "admin": "Адмін"}
        role_label = role_labels.get(role, role)
        edit_message(chat_id, message_id,
            f"Роль {role_label} призначено для {display}.",
            reply_markup=user_manage_keyboard(uid))
        tg_send("sendMessage", chat_id=uid, text=f"Ваш доступ підтверджено. Роль: {role_label}.\nНатисніть /start")

    elif data.startswith("revoke:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        revoke_access(conn, uid)
        edit_message(chat_id, message_id, "Доступ відкликано.", reply_markup=admin_keyboard())
        tg_send("sendMessage", chat_id=uid, text="Ваш доступ було відкликано.")

    elif data.startswith("modules:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        mods = get_user_modules(conn, uid)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else str(uid)
        edit_message(chat_id, message_id, f"Модулі для {display}:", reply_markup=modules_keyboard(uid, mods))

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
        edit_message(chat_id, message_id, f"Модулі для {display}:", reply_markup=modules_keyboard(uid, mods))

    # Cancel / Skip
    elif data == "menu:cancel":
        clear_state(conn, user_id)
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id, "Скасовано. Оберіть дію:", reply_markup=main_menu_keyboard(modules))

    elif data == "menu:skip":
        st_info = get_state(conn, user_id)
        state = st_info.get("state")
        state_data = st_info.get("data", {})
        modules = get_user_modules(conn, user_id)
        _handle_skip(conn, user_id, chat_id, message_id, state, state_data, modules)

    # Report: today
    elif data == "report:today":
        date_str = today_str()
        s = get_daily_summary(conn, date_str)
        text = format_daily_summary(s)
        edit_message(chat_id, message_id, text, reply_markup={
            "inline_keyboard": [[{"text": "◀️ Меню", "callback_data": "menu:back"}]]
        })

    elif data == "menu:back":
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id, "Оберіть дію:", reply_markup=main_menu_keyboard(modules))

    # Snapshot flow
    elif data == "snap:start":
        user = get_user(conn, user_id)
        if not user or not user["is_approved"]:
            send_message(chat_id, "У вас немає доступу.")
            return
        start_snapshot_flow(conn, chat_id, message_id)

    elif data == "snap:coffee_only":
        set_state(conn, user_id, "snapshot_coffee", {"type": "coffee_only"})
        edit_message(chat_id, message_id, "Кількість порцій кави:", reply_markup=cancel_keyboard())

    elif data == "snap:cash_and_coffee":
        set_state(conn, user_id, "snapshot_cash_method", {"type": "cash_and_coffee"})
        keyboard = {"inline_keyboard": [
            [{"text": "💵 Ввести суму", "callback_data": "snap:cash_sum"}],
            [{"text": "🪙 Порахувати купюри", "callback_data": "snap:cash_bills"}],
            [{"text": "❌ Скасувати", "callback_data": "menu:cancel"}],
        ]}
        edit_message(chat_id, message_id, "Оберіть спосіб введення готівки:", reply_markup=keyboard)

    elif data == "snap:cash_sum":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["cash_method"] = "sum"
        set_state(conn, user_id, "snapshot_cash_amount", state_data)
        edit_message(chat_id, message_id, f"Введіть суму готівки ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "snap:cash_bills":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["cash_method"] = "bills"
        set_state(conn, user_id, "snapshot_cash_bills", state_data)
        edit_message(chat_id, message_id,
            f"Введіть купюри у форматі:\n500x1 100x3 50x2\n\nНомінали: {', '.join(str(d) for d in DENOMINATIONS)} {CURRENCY}",
            reply_markup=cancel_keyboard())

    elif data == "snap:bills_confirm":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["confirmed_cash"] = state_data.get("pending_cash", 0)
        set_state(conn, user_id, "snapshot_coffee", state_data)
        edit_message(chat_id, message_id,
            "Кількість порцій кави (або пропустіть):",
            reply_markup=skip_cancel_keyboard())

    # Close day flow
    elif data == "close:start":
        user = get_user(conn, user_id)
        if not user or not user["is_approved"]:
            send_message(chat_id, "У вас немає доступу.")
            return
        get_or_create_session(conn, today_str())
        start_close_flow(conn, user_id, chat_id, message_id)

    elif data == "close:edit":
        keyboard = {"inline_keyboard": [
            [{"text": "💵 Ввести суму", "callback_data": "close:cash_sum"}],
            [{"text": "🪙 Порахувати купюри", "callback_data": "close:cash_bills"}],
            [{"text": "❌ Скасувати", "callback_data": "menu:cancel"}],
        ]}
        edit_message(chat_id, message_id, "Введіть суму готівки в касі:", reply_markup=keyboard)

    elif data == "close:cash_sum":
        set_state(conn, user_id, "close_cash_amount", {})
        edit_message(chat_id, message_id, f"Введіть суму готівки в касі ({CURRENCY}):", reply_markup=cancel_keyboard())

    elif data == "close:cash_bills":
        set_state(conn, user_id, "close_cash_bills", {})
        edit_message(chat_id, message_id,
            f"Введіть купюри у форматі:\n500x1 100x3 50x2\n\nНомінали: {', '.join(str(d) for d in DENOMINATIONS)} {CURRENCY}",
            reply_markup=cancel_keyboard())

    elif data == "close:bills_confirm":
        st_info = get_state(conn, user_id)
        state_data = st_info.get("data", {})
        state_data["closing_cash"] = state_data.get("pending_cash", 0)
        set_state(conn, user_id, "close_coffee", state_data)
        edit_message(chat_id, message_id,
            "Кількість порцій кави за день (або пропустіть):",
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
            f"День {date_str} закрито.",
            reply_markup=main_menu_keyboard(modules))

    elif data == "close:change":
        start_close_flow(conn, user_id, chat_id, message_id)

    else:
        modules = get_user_modules(conn, user_id)
        edit_message(chat_id, message_id, "Оберіть дію:", reply_markup=main_menu_keyboard(modules))


def _handle_skip(conn, user_id, chat_id, message_id, state, state_data, modules):
    """Handle skip button presses for optional steps."""
    if state == "snapshot_coffee":
        _save_snapshot_and_finish(conn, user_id, chat_id, message_id, state_data, coffee=None, modules=modules)

    elif state == "close_coffee":
        state_data["coffee_portions"] = 0
        set_state(conn, user_id, "close_card", state_data)
        if message_id:
            edit_message(chat_id, message_id,
                f"Сума по карті ({CURRENCY}):",
                reply_markup=skip_cancel_keyboard())
        else:
            send_message(chat_id, f"Сума по карті ({CURRENCY}):", reply_markup=skip_cancel_keyboard())

    elif state == "close_card":
        state_data["card_income"] = 0
        set_state(conn, user_id, "close_confirm", state_data)
        _show_close_confirm(conn, user_id, chat_id, message_id, state_data)

    else:
        clear_state(conn, user_id)
        if message_id:
            edit_message(chat_id, message_id, "Оберіть дію:", reply_markup=main_menu_keyboard(modules))
        else:
            send_message(chat_id, "Оберіть дію:", reply_markup=main_menu_keyboard(modules))


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
        parts.append(f"каса {cash:.0f} {CURRENCY}")
    if coffee_val is not None:
        parts.append(f"кава {coffee_val} шт")
    text = "Зріз збережено: " + (", ".join(parts) if parts else "—") + "\n\nОберіть дію:"
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
        f"Закриття дня {date_str}\n\n"
        f"Готівка в касі: {closing:.2f} {CURRENCY}\n"
        f"Виручка готівка: {cash_income:.2f} {CURRENCY} (каса - розмінка)\n"
        f"Порції кави: {coffee} шт\n"
        f"Картка: {card:.2f} {CURRENCY}"
    )
    keyboard = {"inline_keyboard": [
        [{"text": "✅ Підтвердити", "callback_data": "close:confirm"},
         {"text": "✏️ Змінити", "callback_data": "close:change"}],
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
            send_message(chat_id, "Оберіть дію:", reply_markup=main_menu_keyboard(modules))
        else:
            send_message(chat_id, "Натисніть /start")
        return

    modules = get_user_modules(conn, user_id)

    # â”€â”€ Snapshot: cash amount (sum method) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if state == "snapshot_cash_amount":
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Будь ласка, введіть число.", reply_markup=cancel_keyboard())
            return
        state_data["confirmed_cash"] = value
        set_state(conn, user_id, "snapshot_coffee", state_data)
        send_message(chat_id, "Кількість порцій кави (або пропустіть):", reply_markup=skip_cancel_keyboard())

    # â”€â”€ Snapshot: cash bills â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "snapshot_cash_bills":
        total, breakdown = parse_denominations(text)
        if total is None:
            send_message(chat_id, f"Помилка: {breakdown}", reply_markup=cancel_keyboard())
            return
        state_data["pending_cash"] = total
        set_state(conn, user_id, "snapshot_cash_bills_confirm", state_data)
        keyboard = {"inline_keyboard": [
            [{"text": "✅ Підтвердити", "callback_data": "snap:bills_confirm"},
             {"text": "❌ Скасувати",  "callback_data": "menu:cancel"}]
        ]}
        send_message(chat_id,
            f"Розрахунок купюр:\n{breakdown}\n\nРазом: {total} {CURRENCY}",
            reply_markup=keyboard)

    # â”€â”€ Snapshot: coffee count â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "snapshot_coffee":
        try:
            coffee = int(float(text.replace(",", ".")))
        except ValueError:
            send_message(chat_id, "Введіть ціле число.", reply_markup=skip_cancel_keyboard())
            return
        _save_snapshot_and_finish(conn, user_id, chat_id, None, state_data, coffee=coffee, modules=modules)

    # â”€â”€ Close: cash amount (sum method) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_cash_amount":
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Будь ласка, введіть число.", reply_markup=cancel_keyboard())
            return
        state_data["closing_cash"] = value
        set_state(conn, user_id, "close_coffee", state_data)
        send_message(chat_id, "Кількість порцій кави за день (або пропустіть):", reply_markup=skip_cancel_keyboard())

    # â”€â”€ Close: cash bills â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_cash_bills":
        total, breakdown = parse_denominations(text)
        if total is None:
            send_message(chat_id, f"Помилка: {breakdown}", reply_markup=cancel_keyboard())
            return
        state_data["pending_cash"] = total
        set_state(conn, user_id, "close_cash_bills_confirm", state_data)
        keyboard = {"inline_keyboard": [
            [{"text": "✅ Підтвердити", "callback_data": "close:bills_confirm"},
             {"text": "❌ Скасувати",  "callback_data": "menu:cancel"}]
        ]}
        send_message(chat_id,
            f"Розрахунок купюр:\n{breakdown}\n\nРазом: {total} {CURRENCY}",
            reply_markup=keyboard)

    # â”€â”€ Close: coffee count â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_coffee":
        try:
            coffee = int(float(text.replace(",", ".")))
        except ValueError:
            send_message(chat_id, "Введіть ціле число.", reply_markup=skip_cancel_keyboard())
            return
        state_data["coffee_portions"] = coffee
        set_state(conn, user_id, "close_card", state_data)
        send_message(chat_id, f"Сума по карті ({CURRENCY}):", reply_markup=skip_cancel_keyboard())

    # â”€â”€ Close: card amount â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "close_card":
        try:
            card = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Будь ласка, введіть число.", reply_markup=skip_cancel_keyboard())
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
            send_message(chat_id, "Будь ласка, введіть число.", reply_markup=cancel_keyboard())
            return
        add_record(conn, user_id, today_str(), cash_deposit=value)
        clear_state(conn, user_id)
        send_message(chat_id, f"Вплата {value:.2f} {CURRENCY} збережена.", reply_markup=admin_keyboard())

    # â”€â”€ Admin: withdrawal â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "admin_withdrawal":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Будь ласка, введіть число.", reply_markup=cancel_keyboard())
            return
        add_record(conn, user_id, today_str(), cash_withdrawal=value)
        clear_state(conn, user_id)
        send_message(chat_id, f"Виплата {value:.2f} {CURRENCY} збережена.", reply_markup=admin_keyboard())

    # â”€â”€ Admin: expenses â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    elif state == "admin_expenses":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        try:
            value = float(text.replace(",", "."))
        except ValueError:
            send_message(chat_id, "Будь ласка, введіть число.", reply_markup=cancel_keyboard())
            return
        set_state(conn, user_id, "admin_expense_note", {"amount": value})
        send_message(chat_id, "Введіть примітку до витрат:", reply_markup=cancel_keyboard())

    elif state == "admin_expense_note":
        if int(user_id) != ADMIN_ID:
            clear_state(conn, user_id)
            return
        amount = state_data.get("amount", 0)
        add_record(conn, user_id, today_str(), expenses=amount, notes=text)
        clear_state(conn, user_id)
        send_message(chat_id,
            f"Витрати {amount:.2f} {CURRENCY} збережено.\nПримітка: {text}",
            reply_markup=admin_keyboard())

    else:
        clear_state(conn, user_id)
        send_message(chat_id, "Оберіть дію:", reply_markup=main_menu_keyboard(modules))

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
