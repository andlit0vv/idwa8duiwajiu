# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import os
import random
import sqlite3
import threading
import urllib.request
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    BotCommand,
)
from aiogram.exceptions import TelegramForbiddenError, TelegramBadRequest

try:
    from flask import Flask, redirect, render_template_string, request, url_for

    FLASK_AVAILABLE = True
except Exception:
    FLASK_AVAILABLE = False


# =========================
# Конфигурация
# =========================
BOT_TOKEN = '8786666237:AAGhqr7ijI376l0zdWbrQjRL-9s5ANZbP2k'
CHANNEL_URL = os.getenv("COMPANY_CHANNEL_URL", "https://t.me/your_channel").strip()
CHANNEL_POST_URL = os.getenv("COMPANY_CHANNEL_POST_URL", CHANNEL_URL).strip()
PRIVACY_POLICY_URL = os.getenv("PRIVACY_POLICY_URL", "https://example.com/privacy").strip()
PRIVACY_POLICY_URL_SAFE = escape(PRIVACY_POLICY_URL)

_admin_ids_env = (os.getenv("ADMIN_CHAT_IDS", "") or os.getenv("ADMIN_CHAT_ID", "")).strip()
ADMIN_CHAT_IDS_RAW = _admin_ids_env if _admin_ids_env else "1136263238,947773509"

WEBUI_HOST = os.getenv("WEBUI_HOST", "127.0.0.1").strip()
WEBUI_PORT = int(os.getenv("WEBUI_PORT", "2112"))
WEBUI_ENABLED = os.getenv("WEBUI_ENABLED", "1").strip().lower() not in {"0", "false", "no"}

GOOGLE_SHEETS_WEBHOOK = os.getenv("GOOGLE_SHEETS_WEBHOOK", "").strip()
GOOGLE_SHEETS_ID = os.getenv("GOOGLE_SHEETS_ID", "").strip()
GOOGLE_SHEETS_TAB = os.getenv("GOOGLE_SHEETS_TAB", "Leads").strip()
GOOGLE_SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "").strip()
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()

DB_PATH = os.path.join(os.path.dirname(__file__), "company_bot.db")


# =========================
# Тексты
# =========================
TEXT_MAIN_MENU = """ <tg-emoji emoji-id="5237763160647149111"></tg-emoji> <b>Здравствуйте, {name}!</b>

Мы - команда специалистов, создающая закрытые <b>ИИ-решения для бизнеса</b>. У нас есть опыт работы с лидерами рынка, и теперь мы выходим в публичное поле: внедряем технологии мирового уровня в средние и малые бизнесы - системно и быстро

<b>Наша задача</b> - помочь вам перейти на новые технологические <b>стандарты</b>, которые сегодня активно внедряют ведущие корпорации. Те, кто быстрее их освоит, заберет долю рынка у более медленных конкурентов

Сейчас мы работаем на имя, поэтому первым клиентам готовы провести <b>бесплатный аудит</b> и показать, где вам может быть выгодно использование ИИ

Выберите раздел:
"""

TEXT_SERVICES = """🧩 <b>Услуги</b>

Наша ключевая компетенция - это <b>внедрение ИИ</b>, но мы не ограничиваемся только этим. 

Если вашему бизнесу нужен <b>сайт</b>, <b>мобильное или декстопное приложение</b>, <b>корпоративная CRM</b> или <b>сложный Telegram-бот</b>, мы разработаем и запустим это «под ключ».
 
Фактически, <b>всё, что вы можете открыть со смартфона или ПК</b>, входит в зону нашей экспертизы

Выберите, что вас заинтересовало:
"""

TEXT_DEV = """🛠 <b>Разработка систем и платформ</b>

Возможно, <b>вам вообще не нужен ИИ</b> - иногда ключевую роль играет хорошо проработанная внутренняя система, которая объединяет всё нужное в одном месте.

В таком случае мы можем разработать для вас <b>внутреннее ПО</b>, <b>мобильное приложение</b>, <b>веб-платформу</b> или <b>сложного Telegram-бота</b>
<b>Опыт в этом у нас уже есть:</b>
• <a href="https://telegra.ph/Kejs-Razrabotka-oblachnogo-menedzhera-skriptov-s-veb-interfejsom-CloudBase-02-19">Облачный менеджер скриптов CloudBase</a>
• <a href="https://telegra.ph/Vot-kejs-sostavlennyj-na-osnove-analiza-koda-LeadQuest-AI--DISC-Academy-02-19">Образовательная платформа LeadQuest AI</a>
• <a href="https://telegra.ph/Kejs-Razrabotka-korporativnoj-sistemy-upravleniya-proektami-i-rezervnym-kopirovaniem-PBMS-02-19">Корпоративная система PBMS</a>
"""

TEXT_AUTOMATION = """🤖 <b>Повышение эффективности</b>

<b>Мы не внедряем "ИИ ради ИИ".</b>
Все прошлые проекты приходили к нам по рекомендациям, поэтому для нас важно не просто внедрить технологию, а <b>разобраться в вашем бизнесе</b> и понять, действительно ли она <b>принесёт пользу</b>. Если задача лучше решается обычной автоматизацией или внутренним ПО - <b>мы честно об этом скажем.</b>

<b>Не доверяете ИИ?</b>
Мы понимаем, почему вы так считаете :)
Похожая ситуация была и с интернетом в начало нулевых - сначала бизнес относился к нему скептически, а затем он стал стандартом. 
Поэтому мы предлагаем вам <b>подписочный формат</b>: разрабатываем для вас решение, вы тестируете его 1–2 месяца и оплачиваете по месяцам; если результат не устроит, то просто прекратите подписку.

Примеры таких решений:
• <a href="https://telegra.ph/Kejs-Razrabotka-AI-mikroservisa-dlya-avtomaticheskogo-razbora-zakazov-iz-telefonnyh-zvonkov-02-19">AI-оператор для колл-центров</a>
• <a href="https://telegra.ph/Kejs-Razrabotka-multiagentnoj-sistemy-dlya-avtomatizacii-SMM-v-Telegram-02-19">SMM-агенты в Telegram</a>
• <a href="https://telegra.ph/Kejs-Razrabotka-AI-assistenta-dlya-avtomaticheskoj-generacii-dizajn-specifikacij-LLM-Progger-02-19">Автоматическая генерация ТЗ</a>
"""

TEXT_CASES = """📌 <b>Кейсы</b>

Практический результат — главный показатель нашей компетенции. Примеры проектов:

1) <a href="https://telegra.ph/Kejs-Razrabotka-AI-mikroservisa-dlya-avtomaticheskogo-razbora-zakazov-iz-telefonnyh-zvonkov-02-19">AI-микросервис для колл-центров</a> — нейросеть слушает диалоги в реальном времени, мгновенно разбирает заказы и освобождает менеджеров от рутины.
2) <a href="https://telegra.ph/Vot-kejs-sostavlennyj-na-osnove-analiza-koda-LeadQuest-AI--DISC-Academy-02-19">Образовательная платформа LeadQuest AI</a> — индивидуальный трек обучения с адаптивной аналитикой успеваемости.
3) <a href="https://telegra.ph/Kejs-Razrabotka-AI-sistemy-institucionalnoj-analitiki-kriptovalyutnogo-rynka-02-19">Система институциональной аналитики</a> — анализ больших данных и предиктивная аналитика.
4) <a href="https://telegra.ph/Kejs-Razrabotka-multiagentnoj-sistemy-dlya-avtomatizacii-SMM-v-Telegram-02-19">Мультиагентная система SMM в Telegram</a> — команда виртуальных ботов, которая сама ищет инфоповоды, пишет тексты и публикует посты 24/7.
5) <a href="https://telegra.ph/Kejs-Razrabotka-AI-assistenta-dlya-avtomaticheskoj-generacii-dizajn-specifikacij-LLM-Progger-02-19">AI-ассистент генерации спецификаций</a> — из идей клиента в строгие дизайн-спецификации за секунды.
6) <a href="https://telegra.ph/Kejs-Razrabotka-sredy-dlya-paketnogo-testirovaniya-i-otladki-promptov-lokalnyh-LLM-02-19">Среда отладки локальных LLM</a> — пакетное тестирование промптов и повышение качества локальных нейросетей.
7) <a href="https://telegra.ph/Kejs-Razrabotka-korporativnoj-sistemy-upravleniya-proektami-i-rezervnym-kopirovaniem-PBMS-02-19">Корпоративная система PBMS</a> — единый центр управления проектами с архитектурой резервного копирования.
8) <a href="https://telegra.ph/Kejs-Razrabotka-premialnogo-marketplejsa-iskusstva-s-NFT-verifikaciej-02-19">Премиальный маркетплейс искусства</a> — NFT-верификация, умные контракты и подтверждение подлинности активов.
9) <a href="https://telegra.ph/Kejs-Razrabotka-oblachnogo-menedzhera-skriptov-s-veb-interfejsom-CloudBase-02-19">Облачный менеджер скриптов CloudBase</a> — централизованное управление скриптами через веб-интерфейс.

Оставьте заявку, чтобы реализовать похожий результат для вашей компании.
"""

TEXT_LEAD_PROMPT = "✍️ Кратко опишите вашу задачу, текущую проблему или идею."

TEXT_BUDGET_PROMPT = "💰 Пожалуйста, сориентируйте нас по планируемому бюджету проекта:"

TEXT_CONTACT_PROMPT = f"""🔒 Благодарим за информацию. Пожалуйста, нажмите кнопку ниже, чтобы
поделиться вашим контактом. Мы гарантируем конфиденциальность и используем его
исключительно для связи по вашему проекту. Нажимая кнопку, Вы соглашаетесь с
<a href="{PRIVACY_POLICY_URL_SAFE}">Политикой конфиденциальности</a>"""

TEXT_LEAD_DONE = """✅ Спасибо за доверие! Ваша заявка принята. Мы ознакомимся с деталями и свяжемся с Вами в ближайшее время для предметного обсуждения."""

TEXT_DIRECT_ACK = "📩 Ваше сообщение передано администратору, скоро ответим."

NUDGE_ABANDONED = """Здравствуйте! Видим, что вы начали оформлять заявку, но процесс прервался.
Если вам неудобно оставлять номер телефона или заполнять форму, мы можем обсудить вашу задачу прямо здесь, в текстовом формате. Просто напишите ваш вопрос в ответ на это сообщение, и мы свяжемся с вами в этом чате."""

NUDGE_INTEREST = f"""Добрый день! Вчера вы изучали наши решения и кейсы.
Обычно перед стартом разработки или внедрения ИИ у бизнеса возникает много технических вопросов. Чтобы помочь разобраться, мы публикуем разборы архитектуры и примеры работ в нашем телеграм-канале: {CHANNEL_URL}"""

NUDGE_SILENT = f"""Здравствуйте! Пару дней назад вы запускали нашего бота.
Если у вас пока нет конкретной IT-задачи, но интересна тема автоматизации, нейросетей и разработки сложных веб-платформ — приглашаем в наш Telegram-канал. Мы пишем редко, без воды и только о реальном опыте: {CHANNEL_URL}"""

NUDGE_SLEEP = f"""Краткая новость от нашей команды ⚡
На прошлой неделе мы завершили интересный проект: внедрили систему институциональной крипто-аналитики и ускорили обработку данных в 3 раза.
Полный технический разбор мы выложили в нашем канале: {CHANNEL_POST_URL}"""


# =========================
# Кнопки
# =========================
EMOJI_SERVICES = "5213214428958306222"
EMOJI_CASES = "5303209807879093100"
EMOJI_ABOUT = "5224688446974475279"
EMOJI_CONTACT = "5192842565849725544"
EMOJI_DEV = "5213214428958306222"
EMOJI_AUTOMATION = "5327931798548665621"

BTN_SERVICES = 'Услуги'
BTN_CASES = 'Кейсы'
BTN_ABOUT = '🔵 О нас'
BTN_CONTACT = '🔵 Связаться с нами'
BTN_BACK = "🔴 ⬅️ Назад"

BTN_DEV = 'Разработка под ключ'
BTN_AUTOMATION = 'Внедрение ИИ'
BTN_DISCUSS = "Обсудить задачу"
BTN_SHARE_CONTACT = "Поделиться контактом"

BUDGET_OPTIONS = [
    "до 50 000р",
    "50 000 - 500 000р",
    "более 500 000",
]

# =========================
# Логирование
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("company_bot")


# =========================
# DB helpers
# =========================
DB_LOCK = threading.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ts(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


def _parse_ts(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _db_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _get_table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return {row["name"] for row in cur.fetchall()}


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = _get_table_columns(conn, table)
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")


def init_db() -> None:
    with DB_LOCK:
        conn = _db_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                telegram_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                created_at TEXT,
                updated_at TEXT,
                started_at TEXT,
                last_action_at TEXT,
                last_section TEXT,
                action_count INTEGER DEFAULT 0,
                lead_stage TEXT DEFAULT 'none',
                lead_text TEXT,
                budget TEXT,
                contact_phone TEXT,
                contact_name TEXT,
                contact_user_id INTEGER,
                contact_vcard TEXT,
                funnel_started_at TEXT,
                interest_at TEXT,
                last_message_at TEXT,
                last_message_text TEXT,
                unread_count INTEGER DEFAULT 0,
                nudge_blocked INTEGER DEFAULT 0,
                nudge_abandoned_sent INTEGER DEFAULT 0,
                nudge_interest_sent INTEGER DEFAULT 0,
                nudge_silent_sent INTEGER DEFAULT 0,
                nudge_sleep_sent_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                direction TEXT NOT NULL,
                text TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        _ensure_columns(
            conn,
            "users",
            {
                "last_message_at": "TEXT",
                "last_message_text": "TEXT",
                "unread_count": "INTEGER DEFAULT 0",
                "last_ack_sent_at": "TEXT",
            },
        )
        conn.commit()
        conn.close()


def _ensure_user_sync(telegram_id: int, username: str, first_name: str, last_name: str) -> None:
    now = _ts(_utcnow())
    with DB_LOCK:
        conn = _db_conn()
        cur = conn.execute("SELECT telegram_id FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        if row:
            conn.execute(
                """
                UPDATE users SET username = ?, first_name = ?, last_name = ?, updated_at = ?
                WHERE telegram_id = ?
                """,
                (username, first_name, last_name, now, telegram_id),
            )
        else:
            conn.execute(
                """
                INSERT INTO users (
                    telegram_id, username, first_name, last_name, created_at, updated_at, action_count, lead_stage
                ) VALUES (?, ?, ?, ?, ?, ?, 0, 'none')
                """,
                (telegram_id, username, first_name, last_name, now, now),
            )
        conn.commit()
        conn.close()


def _update_user_sync(telegram_id: int, **fields: Any) -> None:
    if not fields:
        return
    fields["updated_at"] = _ts(_utcnow())
    keys = list(fields.keys())
    values = [fields[k] for k in keys]
    set_clause = ", ".join(f"{k} = ?" for k in keys)
    with DB_LOCK:
        conn = _db_conn()
        conn.execute(f"UPDATE users SET {set_clause} WHERE telegram_id = ?", (*values, telegram_id))
        conn.commit()
        conn.close()


def _increment_action_sync(telegram_id: int, section: str) -> None:
    now = _ts(_utcnow())
    with DB_LOCK:
        conn = _db_conn()
        conn.execute(
            """
            UPDATE users
            SET action_count = COALESCE(action_count, 0) + 1,
                last_action_at = ?,
                last_section = ?,
                updated_at = ?
            WHERE telegram_id = ?
            """,
            (now, section, now, telegram_id),
        )
        conn.commit()
        conn.close()


def _get_user_sync(telegram_id: int) -> Optional[sqlite3.Row]:
    with DB_LOCK:
        conn = _db_conn()
        cur = conn.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
        row = cur.fetchone()
        conn.close()
        return row


def _get_all_users_sync() -> list[sqlite3.Row]:
    with DB_LOCK:
        conn = _db_conn()
        cur = conn.execute("SELECT * FROM users")
        rows = cur.fetchall()
        conn.close()
        return rows


def _log_message_sync(telegram_id: int, direction: str, text: str) -> None:
    now = _ts(_utcnow())
    with DB_LOCK:
        conn = _db_conn()
        conn.execute(
            """
            INSERT INTO messages (telegram_id, direction, text, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (telegram_id, direction, text, now),
        )
        if direction == "in":
            conn.execute(
                """
                UPDATE users
                SET last_message_at = ?, last_message_text = ?, unread_count = COALESCE(unread_count, 0) + 1,
                    updated_at = ?
                WHERE telegram_id = ?
                """,
                (now, text, now, telegram_id),
            )
        else:
            conn.execute(
                """
                UPDATE users
                SET unread_count = 0, updated_at = ?, last_ack_sent_at = ?
                WHERE telegram_id = ?
                """,
                (now, now, telegram_id),
            )
        conn.commit()
        conn.close()


def _get_messages_sync(telegram_id: int, limit: int = 200) -> list[sqlite3.Row]:
    with DB_LOCK:
        conn = _db_conn()
        cur = conn.execute(
            """
            SELECT direction, text, created_at
            FROM messages
            WHERE telegram_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (telegram_id, limit),
        )
        rows = cur.fetchall()
        conn.close()
        return list(reversed(rows))


def _get_users_for_web_sync() -> list[sqlite3.Row]:
    with DB_LOCK:
        conn = _db_conn()
        cur = conn.execute(
            """
            SELECT telegram_id, username, first_name, last_name, lead_stage, budget, contact_phone,
                   last_message_at, last_message_text, unread_count, updated_at
            FROM users
            ORDER BY
                CASE WHEN last_message_at IS NULL THEN 1 ELSE 0 END,
                last_message_at DESC,
                updated_at DESC
            """
        )
        rows = cur.fetchall()
        conn.close()
        return rows


async def ensure_user(message: Message) -> None:
    user = message.from_user
    if not user:
        return
    await asyncio.to_thread(
        _ensure_user_sync,
        user.id,
        user.username or "",
        user.first_name or "",
        user.last_name or "",
    )


async def update_user(telegram_id: int, **fields: Any) -> None:
    await asyncio.to_thread(_update_user_sync, telegram_id, **fields)


async def increment_action(telegram_id: int, section: str) -> None:
    await asyncio.to_thread(_increment_action_sync, telegram_id, section)


async def get_user(telegram_id: int) -> Optional[sqlite3.Row]:
    return await asyncio.to_thread(_get_user_sync, telegram_id)


async def get_all_users() -> list[sqlite3.Row]:
    return await asyncio.to_thread(_get_all_users_sync)


async def log_message(telegram_id: int, direction: str, text: str) -> None:
    await asyncio.to_thread(_log_message_sync, telegram_id, direction, text)


async def get_messages(telegram_id: int, limit: int = 200) -> list[sqlite3.Row]:
    return await asyncio.to_thread(_get_messages_sync, telegram_id, limit)


# =========================
# Утилиты
# =========================
def parse_admin_ids(raw: str) -> list[int]:
    items: list[int] = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            items.append(int(part))
        except ValueError:
            continue
    return items


ADMIN_CHAT_IDS = parse_admin_ids(ADMIN_CHAT_IDS_RAW)
BOT_LOOP: Optional[asyncio.AbstractEventLoop] = None

from aiogram.utils.keyboard import InlineKeyboardBuilder  # Убедитесь, что импорт есть в начале


def main_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    # Кнопки с Custom Emoji ID
    builder.row(
        InlineKeyboardButton(text=BTN_SERVICES, callback_data="menu_services", icon_custom_emoji_id=EMOJI_SERVICES))
    builder.row(InlineKeyboardButton(text=BTN_CASES, callback_data="menu_cases", icon_custom_emoji_id=EMOJI_CASES))
    builder.row(InlineKeyboardButton(text=BTN_ABOUT, callback_data="menu_about", icon_custom_emoji_id=EMOJI_ABOUT))
    builder.row(InlineKeyboardButton(text=BTN_CONTACT, callback_data="menu_contact", icon_custom_emoji_id=EMOJI_CONTACT, style="success"))

    return builder.as_markup()


def services_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.row(InlineKeyboardButton(text=BTN_DEV, callback_data="srv_dev", icon_custom_emoji_id=EMOJI_DEV, style="primary"))
    builder.row(InlineKeyboardButton(text=BTN_AUTOMATION, callback_data="srv_auto",
                                     icon_custom_emoji_id=EMOJI_AUTOMATION, style="success"))
    builder.row(InlineKeyboardButton(text=BTN_BACK, callback_data="back_main", style="danger"))

    return builder.as_markup()

def detail_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=BTN_DISCUSS, callback_data="menu_contact")],
            [InlineKeyboardButton(text=BTN_BACK, callback_data="back_services", style="danger")]
        ]
    )

def cases_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=BTN_DISCUSS, callback_data="menu_contact")],
            [InlineKeyboardButton(text=BTN_BACK, callback_data="back_main", style="danger")]
        ]
    )

def budget_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=opt, callback_data=f"budget_{i}")] for i, opt in enumerate(BUDGET_OPTIONS)
        ]
    )

def contact_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=BTN_SHARE_CONTACT, request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )

def about_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Перейти в канал", url=CHANNEL_URL, style="primary")],
            [InlineKeyboardButton(text=BTN_BACK, callback_data="back_main", style="danger")]
        ]
    )


def is_budget_choice(text: str) -> bool:
    return text.strip() in BUDGET_OPTIONS


def build_admin_lead_text(user: sqlite3.Row, lead_text: str, budget: str, contact_phone: str, contact_name: str) -> str:
    username = escape(user["username"] or "")
    uname = f"@{username}" if username else "—"
    first_name = escape(user["first_name"] or "")
    last_name = escape(user["last_name"] or "")
    safe_lead = escape(lead_text or "")
    safe_budget = escape(budget or "")
    safe_phone = escape(contact_phone or "")
    safe_contact_name = escape(contact_name or "")
    name_line = f"{first_name} {last_name}".strip() or "—"
    return (
        "Новая заявка из бота:\n"
        f"ID: {user['telegram_id']}\n"
        f"Username: {uname}\n"
        f"Имя: {name_line}\n"
        f"Задача: {safe_lead}\n"
        f"Бюджет: {safe_budget}\n"
        f"Контакт: {safe_contact_name} ({safe_phone})"
    )


def build_admin_message_text(user: sqlite3.Row, text: str) -> str:
    username = escape(user["username"] or "")
    uname = f"@{username}" if username else "—"
    first_name = escape(user["first_name"] or "")
    last_name = escape(user["last_name"] or "")
    safe_text = escape(text or "")
    name_line = f"{first_name} {last_name}".strip() or "—"
    return (
        "Сообщение от пользователя:\n"
        f"ID: {user['telegram_id']}\n"
        f"Username: {uname}\n"
        f"Имя: {name_line}\n"
        f"Сообщение: {safe_text}"
    )


def send_webhook(url: str, payload: dict[str, Any]) -> bool:
    try:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception as exc:
        logger.warning("Webhook send failed: %s", exc)
        return False


def append_to_google_sheet(payload: dict[str, Any]) -> bool:
    if GOOGLE_SHEETS_WEBHOOK:
        return send_webhook(GOOGLE_SHEETS_WEBHOOK, payload)
    if not GOOGLE_SHEETS_ID:
        return False
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except Exception as exc:
        logger.warning("gspread not available: %s", exc)
        return False

    try:
        if GOOGLE_SERVICE_ACCOUNT_FILE and os.path.isfile(GOOGLE_SERVICE_ACCOUNT_FILE):
            creds = Credentials.from_service_account_file(
                GOOGLE_SERVICE_ACCOUNT_FILE,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
        elif GOOGLE_SERVICE_ACCOUNT_JSON:
            info = json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)
            creds = Credentials.from_service_account_info(
                info,
                scopes=["https://www.googleapis.com/auth/spreadsheets"],
            )
        else:
            logger.warning("Google service account credentials missing")
            return False

        client = gspread.authorize(creds)
        sheet = client.open_by_key(GOOGLE_SHEETS_ID)
        worksheet = sheet.worksheet(GOOGLE_SHEETS_TAB)
        worksheet.append_row(payload["row"], value_input_option="USER_ENTERED")
        return True
    except Exception as exc:
        logger.warning("Google Sheets append failed: %s", exc)
        return False


async def notify_admins(text: str) -> None:
    if not ADMIN_CHAT_IDS:
        logger.info("ADMIN_CHAT_IDS not configured. Message skipped.")
        return
    for admin_id in ADMIN_CHAT_IDS:
        try:
            await bot.send_message(admin_id, text)
        except TelegramForbiddenError:
            logger.warning("Admin chat %s forbidden.", admin_id)
        except TelegramBadRequest as exc:
            logger.warning("Admin message failed: %s", exc)


# =========================
# Web UI (Flask)
# =========================
def _format_dt(value: Optional[str]) -> str:
    if not value:
        return "—"
    dt = _parse_ts(value)
    if not dt:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone().strftime("%d.%m.%Y %H:%M")


def send_from_webui(telegram_id: int, text: str) -> tuple[bool, str]:
    if not text:
        return False, "Пустое сообщение"
    if BOT_LOOP is None:
        return False, "Бот еще не запущен"
    safe_text = escape(text)
    try:
        future = asyncio.run_coroutine_threadsafe(
            bot.send_message(telegram_id, safe_text),
            BOT_LOOP,
        )
        future.result(timeout=10)
    except Exception as exc:
        logger.warning("WebUI send failed: %s", exc)
        return False, str(exc)
    _log_message_sync(telegram_id, "out", text)
    _update_user_sync(telegram_id, nudge_blocked=1)
    return True, ""


def create_web_app() -> Optional["Flask"]:
    if not FLASK_AVAILABLE:
        return None

    app = Flask(__name__)
    app.config["JSON_AS_ASCII"] = False

    INDEX_TEMPLATE = """
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8">
      <title>Company Bot Console</title>
      <style>
        :root { color-scheme: dark; }
        body { margin: 0; font-family: 'Segoe UI', system-ui, sans-serif; background:#0f172a; color:#e2e8f0; }
        .container { max-width: 1200px; margin: 0 auto; padding: 28px; }
        header { display:flex; align-items:center; justify-content:space-between; margin-bottom: 20px; }
        h1 { margin: 0 0 6px 0; font-size: 24px; }
        .subtitle { color:#94a3b8; font-size: 13px; }
        .badge { background:#22c55e; color:#0f172a; padding:6px 10px; border-radius:999px; font-weight:600; }
        .card { background:#111827; border:1px solid #1f2937; border-radius:16px; padding:16px; margin-bottom:18px; }
        .card h2 { margin:0 0 12px 0; font-size:16px; }
        form { display:flex; gap:12px; flex-wrap:wrap; }
        input, textarea { background:#0b1220; border:1px solid #1f2937; color:#e2e8f0; border-radius:10px; padding:10px; }
        input { width: 200px; }
        textarea { flex:1; min-width:280px; min-height:72px; resize:vertical; }
        button { background:#6366f1; border:none; color:#fff; padding:10px 16px; border-radius:10px; cursor:pointer; }
        table { width:100%; border-collapse: collapse; }
        th, td { text-align:left; padding:10px; border-bottom:1px solid #1f2937; vertical-align:top; }
        th { color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:0.04em; }
        .muted { color:#94a3b8; font-size:12px; }
        .tag { background:#0f172a; border:1px solid #334155; border-radius:8px; padding:4px 8px; font-size:12px; }
        .pill { background:#ef4444; border-radius:999px; padding:2px 8px; font-size:12px; }
        .btn { color:#93c5fd; text-decoration:none; }
      </style>
    </head>
    <body>
      <div class="container">
        <header>
          <div>
            <h1>Company Bot Console</h1>
            <div class="subtitle">Локальная панель ответов • порт {{ port }}</div>
          </div>
          <div class="badge">ONLINE</div>
        </header>

        <section class="card">
          <h2>Быстрый ответ</h2>
          <form method="post" action="{{ url_for('send') }}">
            <input name="telegram_id" placeholder="Telegram ID" required>
            <textarea name="text" placeholder="Введите сообщение..." required></textarea>
            <button type="submit">Отправить</button>
          </form>
        </section>

        <section class="card">
          <h2>Клиенты</h2>
          <table>
            <thead>
              <tr>
                <th>ID</th>
                <th>Пользователь</th>
                <th>Стадия</th>
                <th>Бюджет</th>
                <th>Контакт</th>
                <th>Последнее сообщение</th>
                <th>Время</th>
                <th>Новые</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {% for u in users %}
              <tr>
                <td>{{ u.telegram_id }}</td>
                <td>
                  {{ (u.first_name or '') ~ ' ' ~ (u.last_name or '') }}
                  {% if u.username %}<div class="muted">@{{ u.username }}</div>{% endif %}
                </td>
                <td><span class="tag">{{ u.lead_stage or '—' }}</span></td>
                <td>{{ u.budget or '—' }}</td>
                <td>{{ u.contact_phone or '—' }}</td>
                <td>{{ u.last_message_text or '—' }}</td>
                <td>{{ fmt(u.last_message_at) }}</td>
                <td>{% if u.unread_count %}<span class="pill">{{ u.unread_count }}</span>{% else %}—{% endif %}</td>
                <td><a class="btn" href="{{ url_for('user_detail', telegram_id=u.telegram_id) }}">Открыть</a></td>
              </tr>
              {% endfor %}
            </tbody>
          </table>
        </section>
      </div>
    </body>
    </html>
    """

    USER_TEMPLATE = """
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8">
      <title>Диалог</title>
      <style>
        :root { color-scheme: dark; }
        body { margin:0; font-family:'Segoe UI', system-ui, sans-serif; background:#0f172a; color:#e2e8f0; }
        .container { max-width: 1000px; margin: 0 auto; padding: 28px; }
        a { color:#93c5fd; text-decoration:none; }
        .header { display:flex; align-items:center; justify-content:space-between; margin-bottom:16px; }
        .card { background:#111827; border:1px solid #1f2937; border-radius:16px; padding:16px; margin-bottom:18px; }
        .grid { display:grid; grid-template-columns: repeat(3, 1fr); gap:12px; }
        .label { color:#94a3b8; font-size:12px; text-transform:uppercase; letter-spacing:0.04em; }
        .chat { display:flex; flex-direction:column; gap:12px; }
        .bubble { max-width:75%; padding:12px 14px; border-radius:14px; white-space: pre-wrap; }
        .in { background:#0b1220; border:1px solid #1f2937; align-self:flex-start; }
        .out { background:#1e293b; border:1px solid #334155; align-self:flex-end; }
        .meta { font-size:11px; color:#94a3b8; margin-bottom:6px; }
        form { display:flex; gap:12px; margin-top:16px; }
        textarea { flex:1; min-height:90px; background:#0b1220; border:1px solid #1f2937; color:#e2e8f0; border-radius:10px; padding:10px; resize:vertical; }
        button { background:#6366f1; border:none; color:#fff; padding:10px 16px; border-radius:10px; cursor:pointer; }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <div>
            <div class="label">Диалог</div>
            <h2 style="margin:6px 0 0 0;">{{ user.telegram_id }}</h2>
            {% if user.username %}<div class="meta">@{{ user.username }}</div>{% endif %}
          </div>
          <a href="{{ url_for('index') }}">← Назад к списку</a>
        </div>

        <section class="card grid">
          <div>
            <div class="label">Имя</div>
            <div>{{ (user.first_name or '') ~ ' ' ~ (user.last_name or '') }}</div>
          </div>
          <div>
            <div class="label">Стадия</div>
            <div>{{ user.lead_stage or '—' }}</div>
          </div>
          <div>
            <div class="label">Бюджет</div>
            <div>{{ user.budget or '—' }}</div>
          </div>
          <div>
            <div class="label">Контакт</div>
            <div>{{ user.contact_phone or '—' }}</div>
          </div>
          <div>
            <div class="label">Последнее сообщение</div>
            <div>{{ fmt(user.last_message_at) }}</div>
          </div>
          <div>
            <div class="label">Новые</div>
            <div>{{ user.unread_count or 0 }}</div>
          </div>
        </section>

        <section class="card">
          <div class="chat">
            {% for m in messages %}
              <div class="bubble {{ 'in' if m.direction == 'in' else 'out' }}">
                <div class="meta">{{ 'Клиент' if m.direction == 'in' else 'Вы' }} • {{ fmt(m.created_at) }}</div>
                <div>{{ m.text }}</div>
              </div>
            {% endfor %}
          </div>

          <form method="post" action="{{ url_for('send') }}">
            <input type="hidden" name="telegram_id" value="{{ user.telegram_id }}">
            <textarea name="text" placeholder="Введите ответ..." required></textarea>
            <button type="submit">Отправить</button>
          </form>
        </section>
      </div>
    </body>
    </html>
    """

    ERROR_TEMPLATE = """
    <!doctype html>
    <html lang="ru">
    <head><meta charset="utf-8"><title>Ошибка</title></head>
    <body style="background:#0f172a;color:#e2e8f0;font-family:'Segoe UI',system-ui,sans-serif;">
      <div style="max-width:720px;margin:40px auto;padding:24px;border-radius:16px;background:#111827;border:1px solid #1f2937;">
        <h2>Не удалось отправить сообщение</h2>
        <div style="color:#94a3b8;">{{ error }}</div>
        <div style="margin-top:16px;"><a href="{{ url_for('user_detail', telegram_id=telegram_id) }}" style="color:#93c5fd;">Вернуться</a></div>
      </div>
    </body>
    </html>
    """

    @app.get("/")
    def index():
        users = _get_users_for_web_sync()
        return render_template_string(INDEX_TEMPLATE, users=users, fmt=_format_dt, port=WEBUI_PORT)

    @app.get("/user/<int:telegram_id>")
    def user_detail(telegram_id: int):
        user = _get_user_sync(telegram_id)
        if not user:
            return "User not found", 404
        _update_user_sync(telegram_id, unread_count=0)
        messages = _get_messages_sync(telegram_id, limit=200)
        return render_template_string(USER_TEMPLATE, user=user, messages=messages, fmt=_format_dt)

    @app.post("/send")
    def send():
        telegram_id = int(request.form.get("telegram_id", "0") or 0)
        text = (request.form.get("text") or "").strip()
        if not telegram_id or not text:
            return redirect(request.referrer or url_for("index"))
        ok, err = send_from_webui(telegram_id, text)
        if not ok:
            return render_template_string(ERROR_TEMPLATE, error=err, telegram_id=telegram_id), 500
        return redirect(url_for("user_detail", telegram_id=telegram_id))

    return app


def start_webui() -> None:
    if not WEBUI_ENABLED:
        logger.info("Web UI disabled by WEBUI_ENABLED.")
        return
    if not FLASK_AVAILABLE:
        logger.warning("Flask not installed. Web UI disabled.")
        return
    app = create_web_app()
    if not app:
        return
    thread = threading.Thread(
        target=app.run,
        kwargs={
            "host": WEBUI_HOST,
            "port": WEBUI_PORT,
            "debug": False,
            "use_reloader": False,
        },
        daemon=True,
    )
    thread.start()
    logger.info("Web UI started at http://%s:%s", WEBUI_HOST, WEBUI_PORT)


# =========================
# Бот и диспетчер
# =========================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


# =========================
# Обработчики
# =========================
@dp.message(CommandStart())
@dp.message(Command("menu"))
async def cmd_start(message: Message) -> None:
    await ensure_user(message)
    user = await get_user(message.from_user.id)
    started_at = user["started_at"] if user else None
    await update_user(
        message.from_user.id,
        started_at=started_at or _ts(_utcnow()),
        lead_stage="none",
    )
    await increment_action(message.from_user.id, "main_menu")

    # Сначала отправляем невидимое удаление клавиатуры, чтобы очистить низ экрана
    # (Это сообщение промелькнет и уберет кнопку "Поделиться контактом")
    temp_msg = await message.answer("Загружаю меню...", reply_markup=ReplyKeyboardRemove())
    # Тут же удаляем это временное текстовое сообщение, чтобы не мусорить в чате
    await temp_msg.delete()

    # Теперь отправляем чистое главное меню с Inline-кнопками
    await message.answer(
        TEXT_MAIN_MENU.format(name=message.from_user.first_name),
        reply_markup=main_menu_kb()
    )


@dp.callback_query(F.data == "menu_services")
async def handle_services(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    await update_user(callback.from_user.id, interest_at=_ts(_utcnow()))
    await increment_action(callback.from_user.id, "services_menu")
    # Изменяем текущее сообщение вместо отправки нового
    await callback.message.edit_text(TEXT_SERVICES, reply_markup=services_kb())
    await callback.answer()


@dp.callback_query(F.data == "menu_cases")
async def handle_cases(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    await update_user(callback.from_user.id, interest_at=_ts(_utcnow()))
    await increment_action(callback.from_user.id, "cases")
    await callback.message.edit_text(TEXT_CASES, reply_markup=cases_kb())
    await callback.answer()


@dp.callback_query(F.data == "menu_about")
async def handle_about(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    await increment_action(callback.from_user.id, "about")
    await callback.message.edit_text(
        f'<tg-emoji emoji-id="5215344475039084599"></tg-emoji> Наш Telegram-канал:\n{CHANNEL_URL}',
        reply_markup=about_inline_kb()
    )
    await callback.answer()


@dp.callback_query(F.data.in_({"menu_contact", "lead_start"}))
async def handle_start_lead(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    await update_user(
        callback.from_user.id,
        lead_stage="lead_text",
        funnel_started_at=_ts(_utcnow()),
        lead_text=None,
        budget=None,
        contact_phone=None,
        contact_name=None,
        contact_user_id=None,
        contact_vcard=None,
        nudge_abandoned_sent=0,
    )
    await increment_action(callback.from_user.id, "lead_start")

    # Убираем клавиатуру под текстом
    await callback.message.edit_reply_markup(reply_markup=None)
    # Отправляем новый вопрос и принудительно скрываем системную клавиатуру
    await callback.message.answer(TEXT_LEAD_PROMPT, reply_markup=ReplyKeyboardRemove())
    await callback.answer()


@dp.callback_query(F.data == "srv_dev")
async def handle_dev(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    await update_user(callback.from_user.id, interest_at=_ts(_utcnow()))
    await increment_action(callback.from_user.id, "service_detail")
    await callback.message.edit_text(TEXT_DEV, reply_markup=detail_kb())
    await callback.answer()


@dp.callback_query(F.data.in_({"srv_auto", "srv_automation"}))
async def handle_automation(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    await update_user(callback.from_user.id, interest_at=_ts(_utcnow()))
    await increment_action(callback.from_user.id, "automation_detail")
    await callback.message.edit_text(TEXT_AUTOMATION, reply_markup=detail_kb())
    await callback.answer()


@dp.callback_query(F.data.in_({"back_main", "back_services", "menu_main"}))
async def handle_back(callback: CallbackQuery) -> None:
    await ensure_user(callback)
    if callback.data == "back_services":
        await increment_action(callback.from_user.id, "services_menu")
        await callback.message.edit_text(TEXT_SERVICES, reply_markup=services_kb())
    else:
        await increment_action(callback.from_user.id, "main_menu")
        await callback.message.edit_text(TEXT_MAIN_MENU, reply_markup=main_menu_kb())
    await callback.answer()


@dp.callback_query(F.data.startswith("budget_"))
async def handle_budget_choice(callback: CallbackQuery) -> None:
    await ensure_user(callback)

    # Извлекаем индекс бюджета из callback_data (например, из budget_1 получим 1)
    idx = int(callback.data.split("_")[1])
    selected_budget = BUDGET_OPTIONS[idx]

    await update_user(
        callback.from_user.id,
        lead_stage="contact",
        budget=selected_budget,
    )
    await increment_action(callback.from_user.id, "lead_budget")

    await callback.message.edit_reply_markup(reply_markup=None)
    # Здесь вызываем обычную клавиатуру, так как Inline не умеет запрашивать контакт
    await callback.message.answer(TEXT_CONTACT_PROMPT, reply_markup=contact_kb())
    await callback.answer()


@dp.message(F.contact)
async def handle_contact(message: Message) -> None:
    await ensure_user(message)
    user = await get_user(message.from_user.id)
    if not user:
        return
    contact = message.contact
    contact_phone = contact.phone_number if contact else ""
    contact_name = " ".join([part for part in [contact.first_name, contact.last_name] if part]) if contact else ""

    await log_message(
        message.from_user.id,
        "in",
        f"Контакт: {contact_name} {contact_phone}".strip(),
    )

    await update_user(
        message.from_user.id,
        lead_stage="completed",
        contact_phone=contact_phone,
        contact_name=contact_name,
        contact_user_id=contact.user_id if contact else None,
        contact_vcard=contact.vcard if contact else None,
        nudge_blocked=1,
    )
    await increment_action(message.from_user.id, "lead_contact")

    lead_text = user["lead_text"] or ""
    budget = user["budget"] or ""
    admin_text = build_admin_lead_text(user, lead_text, budget, contact_phone, contact_name)
    await notify_admins(admin_text)

    payload = {
        "timestamp": _ts(_utcnow()),
        "telegram_id": user["telegram_id"],
        "username": user["username"] or "",
        "first_name": user["first_name"] or "",
        "last_name": user["last_name"] or "",
        "lead_text": lead_text,
        "budget": budget,
        "contact_phone": contact_phone,
        "contact_name": contact_name,
        "row": [
            _ts(_utcnow()),
            user["telegram_id"],
            user["username"] or "",
            user["first_name"] or "",
            user["last_name"] or "",
            lead_text,
            budget,
            contact_phone,
            contact_name,
        ],
    }
    await asyncio.to_thread(append_to_google_sheet, payload)

    # Сначала удаляем системную клавиатуру с кнопкой "Поделиться контактом",
    # затем отправляем главное меню с Inline-кнопками
    await message.answer(TEXT_LEAD_DONE, reply_markup=ReplyKeyboardRemove())
    await message.answer(TEXT_MAIN_MENU, reply_markup=main_menu_kb())


@dp.message(F.text)
async def handle_text(message: Message) -> None:
    await ensure_user(message)
    user = await get_user(message.from_user.id)
    if not user:
        return

    text = message.text.strip()
    if text:
        await log_message(message.from_user.id, "in", text)
    lead_stage = user["lead_stage"] or "none"

    # Шаг 1: ожидание описания задачи
    if lead_stage == "lead_text":
        await update_user(
            message.from_user.id,
            lead_stage="budget",
            lead_text=text,
        )
        await increment_action(message.from_user.id, "lead_text")
        await message.answer(TEXT_BUDGET_PROMPT, reply_markup=budget_kb())
        return

    # Шаг 3: ожидание контакта (если пользователь написал текстом)
    if lead_stage == "contact":
        await forward_direct_message(message, user, text)
        return

    # Любое сообщение вне воронки -> админу
    await forward_direct_message(message, user, text)


async def forward_direct_message(message: Message, user: sqlite3.Row, text: str) -> None:
    admin_text = build_admin_message_text(user, text)
    await notify_admins(admin_text)

    now = _utcnow()
    last_ack_str = user["last_ack_sent_at"]
    last_ack = _parse_ts(last_ack_str) if last_ack_str else None

    # Проверяем: если отбивку еще не отправляли ИЛИ прошло больше 24 часов
    if not last_ack or (now - last_ack).total_seconds() > 24 * 3600:
        # Обновляем время отправки отбивки в БД
        await update_user(message.from_user.id, nudge_blocked=1, lead_stage="none", last_ack_sent_at=_ts(now))
        await increment_action(message.from_user.id, "direct_message")

        await message.answer(TEXT_DIRECT_ACK, reply_markup=ReplyKeyboardRemove())
        await message.answer(TEXT_MAIN_MENU, reply_markup=main_menu_kb())
    else:
        # 24 часа не прошло. Просто меняем стадию и глушим дожим, не отправляя сообщение
        await update_user(message.from_user.id, nudge_blocked=1, lead_stage="none")
        await increment_action(message.from_user.id, "direct_message")
# =========================
# Дожим (scheduler)
# =========================
async def nudge_loop() -> None:
    while True:
        await asyncio.sleep(60)
        try:
            users = await get_all_users()
            now = _utcnow()
            for user in users:
                if user["nudge_blocked"]:
                    continue
                if user["lead_stage"] == "completed" or user["contact_phone"]:
                    continue

                # Сценарий 1: брошенная заявка (2-3 часа)
                lead_stage = user["lead_stage"] or "none"
                lead_text = user["lead_text"] or ""
                if lead_stage in {"budget", "contact"} and lead_text and not user["nudge_abandoned_sent"]:
                    last_action = _parse_ts(user["last_action_at"])
                    if last_action and now - last_action >= timedelta(hours=random.uniform(2.0, 3.0)):
                        await safe_send(user["telegram_id"], NUDGE_ABANDONED)
                        await update_user(user["telegram_id"], nudge_abandoned_sent=1)
                        continue

                # Сценарий 2: изучал кейсы/услуги, но ушел (24 часа)
                interest_at = _parse_ts(user["interest_at"])
                if (
                    interest_at
                    and not user["nudge_interest_sent"]
                    and (user["lead_stage"] or "none") == "none"
                    and not user["funnel_started_at"]
                ):
                    if now - interest_at >= timedelta(hours=24):
                        await safe_send(user["telegram_id"], NUDGE_INTEREST)
                        await update_user(user["telegram_id"], nudge_interest_sent=1)
                        continue

                # Сценарий 3: молчун (48 часов)
                started_at = _parse_ts(user["started_at"]) or _parse_ts(user["created_at"])
                if (
                    started_at
                    and not user["nudge_silent_sent"]
                    and not interest_at
                    and (user["lead_stage"] or "none") == "none"
                    and not user["funnel_started_at"]
                ):
                    if now - started_at >= timedelta(hours=48):
                        await safe_send(user["telegram_id"], NUDGE_SILENT)
                        await update_user(user["telegram_id"], nudge_silent_sent=1)
                        continue

                # Сценарий 4: долгосрочный прогрев (раз в месяц после 20-30 дней)
                if started_at and (user["lead_stage"] or "none") == "none" and not user["funnel_started_at"]:
                    if now - started_at >= timedelta(days=20):
                        last_sleep = _parse_ts(user["nudge_sleep_sent_at"])
                        if not last_sleep or now - last_sleep >= timedelta(days=30):
                            await safe_send(user["telegram_id"], NUDGE_SLEEP)
                            await update_user(user["telegram_id"], nudge_sleep_sent_at=_ts(now))
        except Exception as exc:
            logger.warning("Nudge loop error: %s", exc)


async def safe_send(telegram_id: int, text: str) -> None:
    try:
        await bot.send_message(telegram_id, text, reply_markup=main_menu_kb())
    except TelegramForbiddenError:
        await update_user(telegram_id, nudge_blocked=1)
        logger.info("User %s blocked bot. Nudge stopped.", telegram_id)
    except TelegramBadRequest as exc:
        logger.warning("Failed to send nudge to %s: %s", telegram_id, exc)


# =========================
# Entrypoint
# =========================
async def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit("COMPANY_BOT_TOKEN is required.")
    init_db()
    global BOT_LOOP
    BOT_LOOP = asyncio.get_running_loop()
    start_webui()
    asyncio.create_task(nudge_loop())

    # --- Настраиваем кнопку "Меню" в интерфейсе Telegram ---
    await bot.set_my_commands([
        BotCommand(command="menu", description="Главное меню")
    ])
    # -------------------------------------------------------

    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")
