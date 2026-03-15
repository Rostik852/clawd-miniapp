"""
Telegram Bot Webhook Handler — Vercel Serverless Function
Cafe Cash Flow Management Bot
"""

import json
import os
from http.server import BaseHTTPRequestHandler
from datetime import datetime, timezone

import requests as req

from _db import (
    get_conn, ensure_tables, get_user, get_user_modules, today_str,
    save_user_raw, add_record, get_summary_day,
    set_role, revoke_access, set_module_access,
    get_all_users, get_pending_users, MODULES, ADMIN_ID,
)

BOT_TOKEN = os.environ.get("BOT_TOKEN", "")

# ─── Telegram API helpers ──────────────────────────────────────────────────────

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

# ─── Conversation State (persisted in DB) ─────────────────────────────────────

def _ensure_state_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                user_id BIGINT PRIMARY KEY,
                state TEXT,
                data TEXT,
                updated_at TEXT
            )
        """)
    conn.commit()


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

# ─── Keyboards ────────────────────────────────────────────────────────────────

def main_menu_keyboard(modules: dict):
    buttons = []
    row = []

    mapping = [
        ("cash_income",   "💵 Готівка"),
        ("card_income",   "💳 Картка"),
        ("coffee_count",  "☕ Порції"),
        ("deposits",      "📥 Вплата"),
        ("withdrawals",   "📤 Виплата"),
        ("expenses",      "🧾 Витрати"),
    ]
    for module_key, label in mapping:
        if modules.get(module_key) or modules.get("coffee_portions") if module_key == "coffee_count" else modules.get(module_key):
            row.append({"text": label, "callback_data": f"menu:{module_key}"})
            if len(row) == 2:
                buttons.append(row)
                row = []

    if row:
        buttons.append(row)

    if modules.get("reports", True):
        buttons.append([{"text": "📊 Звіт", "callback_data": "menu:reports"}])

    return {"inline_keyboard": buttons}


def admin_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "👥 Користувачі", "callback_data": "admin:users"}],
            [{"text": "⏳ Очікують",    "callback_data": "admin:pending"}],
            [{"text": "📊 Звіт сьогодні", "callback_data": "admin:report"}],
        ]
    }


def user_manage_keyboard(uid):
    return {
        "inline_keyboard": [
            [
                {"text": "👨‍🍳 Chef",    "callback_data": f"setrole:{uid}:chef"},
                {"text": "☕ Barista", "callback_data": f"setrole:{uid}:barista"},
                {"text": "🧑 Young",  "callback_data": f"setrole:{uid}:young"},
            ],
            [{"text": "🔧 Модулі",       "callback_data": f"modules:{uid}"}],
            [{"text": "🚫 Відкликати",   "callback_data": f"revoke:{uid}"}],
            [{"text": "◀️ Назад",        "callback_data": "admin:users"}],
        ]
    }


def modules_keyboard(uid, current_modules: dict):
    label_map = {
        "cash_income":   "💵 Готівка",
        "card_income":   "💳 Картка",
        "coffee_count":  "☕ Порції",
        "deposits":      "📥 Вплата",
        "withdrawals":   "📤 Виплата",
        "expenses":      "🧾 Витрати",
        "reports":       "📊 Звіт",
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
                {"text": "👨‍🍳 Chef",    "callback_data": f"setrole:{uid}:chef"},
                {"text": "☕ Barista", "callback_data": f"setrole:{uid}:barista"},
                {"text": "🧑 Young",  "callback_data": f"setrole:{uid}:young"},
            ],
            [{"text": "🚫 Відхилити", "callback_data": f"revoke:{uid}"}],
        ]
    }

# ─── Summary formatter ────────────────────────────────────────────────────────

def format_summary(s: dict, date_str: str) -> str:
    total_income = s.get("cash_income", 0) + s.get("card_income", 0)
    net = total_income + s.get("cash_deposit", 0) - s.get("cash_withdrawal", 0) - s.get("expenses", 0)
    return (
        f"📊 Звіт за {date_str}\n\n"
        f"💵 Готівка:    {s.get('cash_income', 0):.2f}\n"
        f"💳 Картка:     {s.get('card_income', 0):.2f}\n"
        f"☕ Порції:     {int(s.get('coffee_portions', 0))}\n"
        f"📥 Вплата:     {s.get('cash_deposit', 0):.2f}\n"
        f"📤 Виплата:    {s.get('cash_withdrawal', 0):.2f}\n"
        f"🧾 Витрати:    {s.get('expenses', 0):.2f}\n"
        f"─────────────────\n"
        f"💰 Дохід:      {total_income:.2f}\n"
        f"📈 Нетто:      {net:.2f}"
    )

# ─── Command handlers ─────────────────────────────────────────────────────────

def handle_start(conn, user_id, username, first_name, last_name, chat_id):
    save_user_raw(conn, user_id, username, first_name, last_name)
    ensure_tables(conn)
    _ensure_state_table(conn)
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
        # Notify admin
        display = f"@{username}" if username else f"{first_name or ''} {last_name or ''}".strip()
        tg_send(
            "sendMessage",
            chat_id=ADMIN_ID,
            text=f"Новий користувач хоче доступ:\n{display} (id: {user_id})\nПризначте роль:",
            reply_markup=pending_role_keyboard(user_id),
        )


def handle_admin(conn, user_id, chat_id):
    if int(user_id) != ADMIN_ID:
        send_message(chat_id, "Немає доступу.")
        return
    send_message(chat_id, "Адмін панель:", reply_markup=admin_keyboard())

# ─── Callback query handlers ──────────────────────────────────────────────────

def handle_callback(conn, callback_id, user_id, chat_id, message_id, data):
    answer_callback(callback_id)

    # ── Admin: users list ──────────────────────────────────────────────────
    if data == "admin:users":
        if int(user_id) != ADMIN_ID:
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

    # ── Admin: pending ─────────────────────────────────────────────────────
    elif data == "admin:pending":
        if int(user_id) != ADMIN_ID:
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

    # ── Admin: report ──────────────────────────────────────────────────────
    elif data == "admin:report":
        if int(user_id) != ADMIN_ID:
            return
        date_str = today_str()
        s = get_summary_day(conn, date_str)
        edit_message(chat_id, message_id, format_summary(s, date_str), reply_markup=admin_keyboard())

    elif data == "admin:back":
        if int(user_id) != ADMIN_ID:
            return
        edit_message(chat_id, message_id, "Адмін панель:", reply_markup=admin_keyboard())

    # ── User detail ────────────────────────────────────────────────────────
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

    # ── Set role ───────────────────────────────────────────────────────────
    elif data.startswith("setrole:"):
        if int(user_id) != ADMIN_ID:
            return
        _, uid_str, role = data.split(":")
        uid = int(uid_str)
        set_role(conn, uid, role)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else uid_str
        edit_message(chat_id, message_id, f"Роль {role} призначено для {display}.", reply_markup=admin_keyboard())
        # Notify user
        tg_send("sendMessage", chat_id=uid, text=f"Ваш доступ підтверджено. Роль: {role}.\nНатисніть /start")

    # ── Revoke ─────────────────────────────────────────────────────────────
    elif data.startswith("revoke:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        revoke_access(conn, uid)
        edit_message(chat_id, message_id, "Доступ відкликано.", reply_markup=admin_keyboard())
        tg_send("sendMessage", chat_id=uid, text="Ваш доступ було відкликано.")

    # ── Modules toggle ─────────────────────────────────────────────────────
    elif data.startswith("modules:"):
        if int(user_id) != ADMIN_ID:
            return
        uid = int(data.split(":")[1])
        mods = get_user_modules(conn, uid)
        u = get_user(conn, uid)
        display = f"@{u['username']}" if u and u.get("username") else str(uid)
        edit_message(
            chat_id, message_id,
            f"Модулі для {display}:",
            reply_markup=modules_keyboard(uid, mods),
        )

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
        edit_message(
            chat_id, message_id,
            f"Модулі для {display}:",
            reply_markup=modules_keyboard(uid, mods),
        )

    # ── Main menu actions ──────────────────────────────────────────────────
    elif data.startswith("menu:"):
        action = data.split(":")[1]
        user = get_user(conn, user_id)
        if not user or not user["is_approved"]:
            send_message(chat_id, "У вас немає доступу.")
            return

        if action == "reports":
            date_str = today_str()
            s = get_summary_day(conn, date_str)
            edit_message(chat_id, message_id, format_summary(s, date_str))
            return

        state_map = {
            "cash_income":  ("waiting_cash",        "Введіть суму готівки (грн):"),
            "card_income":  ("waiting_card",         "Введіть суму картки (грн):"),
            "coffee_count": ("waiting_coffee",       "Введіть кількість порцій:"),
            "deposits":     ("waiting_deposit",      "Введіть суму вплати (грн):"),
            "withdrawals":  ("waiting_withdrawal",   "Введіть суму виплати (грн):"),
            "expenses":     ("waiting_expenses",     "Введіть суму витрат (грн):"),
        }
        if action in state_map:
            st, prompt = state_map[action]
            set_state(conn, user_id, st)
            send_message(chat_id, prompt)

# ─── Text / number input handler ─────────────────────────────────────────────

def handle_text(conn, user_id, chat_id, text):
    st_info = get_state(conn, user_id)
    state = st_info.get("state")
    state_data = st_info.get("data", {})

    if state is None:
        send_message(chat_id, "Оберіть дію з меню або введіть /start")
        return

    # ── Waiting for expense note ───────────────────────────────────────────
    if state == "waiting_expense_note":
        amount = state_data.get("amount", 0)
        add_record(conn, user_id, today_str(), expenses=amount, notes=text)
        clear_state(conn, user_id)
        send_message(chat_id, f"Витрати {amount:.2f} грн збережено. Примітка: {text}")
        return

    # ── Numeric states ─────────────────────────────────────────────────────
    try:
        value = float(text.replace(",", "."))
    except ValueError:
        send_message(chat_id, "Будь ласка, введіть число.")
        return

    field_map = {
        "waiting_cash":       ("cash_income",      "Готівка"),
        "waiting_card":       ("card_income",       "Картка"),
        "waiting_coffee":     ("coffee_portions",   "Порції"),
        "waiting_deposit":    ("cash_deposit",      "Вплата"),
        "waiting_withdrawal": ("cash_withdrawal",   "Виплата"),
    }

    if state in field_map:
        field, label = field_map[state]
        kwargs = {field: int(value) if state == "waiting_coffee" else value}
        add_record(conn, user_id, today_str(), **kwargs)
        clear_state(conn, user_id)
        unit = "порцій" if state == "waiting_coffee" else "грн"
        send_message(chat_id, f"{label} {value:.0f} {unit} збережено.")

    elif state == "waiting_expenses":
        # Need to collect note next
        set_state(conn, user_id, "waiting_expense_note", {"amount": value})
        send_message(chat_id, "Введіть примітку до витрат:")

    else:
        send_message(chat_id, "Невідомий стан. Натисніть /start")
        clear_state(conn, user_id)

# ─── Main update dispatcher ───────────────────────────────────────────────────

def process_update(update: dict):
    conn = get_conn()
    try:
        ensure_tables(conn)
        _ensure_state_table(conn)

        # Callback query
        if "callback_query" in update:
            cq = update["callback_query"]
            user = cq["from"]
            user_id = user["id"]
            chat_id = cq["message"]["chat"]["id"]
            message_id = cq["message"]["message_id"]
            data = cq.get("data", "")
            handle_callback(conn, cq["id"], user_id, chat_id, message_id, data)
            return

        # Message
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

# ─── Vercel handler ───────────────────────────────────────────────────────────

class handler(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length)

        try:
            update = json.loads(body)
            process_update(update)
        except Exception as e:
            # Log but always return 200 so Telegram doesn't retry
            print(f"Error processing update: {e}")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok":true}')

    def log_message(self, fmt, *args):
        # Suppress default HTTP logging noise in Vercel logs
        pass
