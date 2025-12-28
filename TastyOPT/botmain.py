# botmain.py
import asyncio
import logging
import os
import json
from datetime import datetime
from typing import Any

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.dispatcher.event.bases import SkipHandler

# ============ ЛОГИ ============

logging.basicConfig(level=logging.INFO)

# Загружаем .env
load_dotenv()

# ============ НАСТРОЙКИ ИЗ ENV ============

API_TOKEN = os.getenv("BOT_TOKEN")
ADMINS_RAW = os.getenv("ADMIN_IDS", "")
ARCHIVE_CHAT_ID_RAW = os.getenv("ARCHIVE_CHAT_ID", "").strip()

def parse_admin_ids(raw: str):
    ids = set()
    raw = raw.replace(" ", "")
    for part in raw.split(","):
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            logging.warning(f"Не удалось распарсить ADMIN_ID: {part}")
    return ids

ADMIN_IDS = parse_admin_ids(ADMINS_RAW)

ARCHIVE_CHAT_ID: int | None = None
if ARCHIVE_CHAT_ID_RAW:
    try:
        ARCHIVE_CHAT_ID = int(ARCHIVE_CHAT_ID_RAW)
    except ValueError:
        logging.warning("ARCHIVE_CHAT_ID указан неверно. Должен быть числом (например -100...).")
        ARCHIVE_CHAT_ID = None
else:
    logging.warning("ARCHIVE_CHAT_ID не задан. Умная рассылка/архив работать не будут.")

if not API_TOKEN:
    raise RuntimeError("Не найден токен бота. Укажи BOT_TOKEN в переменных окружения.")

if not ADMIN_IDS:
    logging.warning("ADMIN_IDS пуст — в боте не будет админов. Задай ADMIN_IDS в env.")

# ============ ПУТИ К ФАЙЛАМ "БД" ============

DATA_DIR = "data"
USERS_FILE = os.path.join(DATA_DIR, "users.txt")
STATS_FILE = os.path.join(DATA_DIR, "stats.txt")

BROADCASTS_FILE = os.path.join(DATA_DIR, "broadcasts.json")   # список рассылок (архив)
DELIVERIES_FILE = os.path.join(DATA_DIR, "deliveries.json")   # кто что получил + message_id в личке

# ============ ИНИЦИАЛИЗАЦИЯ БОТА ============

bot = Bot(
    token=API_TOKEN,
    default=DefaultBotProperties(parse_mode="HTML"),
)
dp = Dispatcher()

# message_id приветствия для каждого пользователя (чтобы не удалять)
greeting_messages: dict[int, int] = {}  # user_id -> message_id

# message_id последних ответов бота (для автоудаления)
user_messages: dict[int, set[int]] = {}  # user_id -> set(message_id)

# админы, которые сейчас в режиме "жду сообщение для рассылки"
pending_broadcast_admins: set[int] = set()

# черновики рассылок: admin_id -> {"archive_message_id": int}
broadcast_drafts: dict[int, dict[str, int]] = {}

# ============ JSON HELPERS ============

def _load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        logging.warning(f"JSON повреждён: {path}. Создаю заново.")
        return default

def _save_json(path: str, data: Any) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def ensure_files():
    os.makedirs(DATA_DIR, exist_ok=True)
    for path in (USERS_FILE, STATS_FILE):
        if not os.path.exists(path):
            open(path, "w", encoding="utf-8").close()

    if not os.path.exists(BROADCASTS_FILE):
        _save_json(BROADCASTS_FILE, {"broadcasts": []})

    if not os.path.exists(DELIVERIES_FILE):
        _save_json(DELIVERIES_FILE, {"deliveries": {}})

ensure_files()

def load_broadcasts() -> list[dict[str, Any]]:
    ensure_files()
    data = _load_json(BROADCASTS_FILE, {"broadcasts": []})
    items = data.get("broadcasts", [])
    if not isinstance(items, list):
        return []
    return items

def save_broadcasts(items: list[dict[str, Any]]) -> None:
    _save_json(BROADCASTS_FILE, {"broadcasts": items})

def load_deliveries() -> dict[str, dict[str, int]]:
    """
    deliveries[user_id_str][broadcast_id_str] = chat_message_id_int
    """
    ensure_files()
    data = _load_json(DELIVERIES_FILE, {"deliveries": {}})
    d = data.get("deliveries", {})
    if not isinstance(d, dict):
        return {}
    # нормализуем вложенность
    cleaned: dict[str, dict[str, int]] = {}
    for uid, mp in d.items():
        if not isinstance(mp, dict):
            continue
        cleaned[uid] = {}
        for bid, mid in mp.items():
            try:
                cleaned[uid][str(bid)] = int(mid)
            except Exception:
                continue
    return cleaned

def save_deliveries(deliveries: dict[str, dict[str, int]]) -> None:
    _save_json(DELIVERIES_FILE, {"deliveries": deliveries})

def was_delivered(user_id: int, broadcast_id: str) -> bool:
    deliveries = load_deliveries()
    return broadcast_id in deliveries.get(str(user_id), {})

def mark_delivered(user_id: int, broadcast_id: str, chat_message_id: int) -> None:
    deliveries = load_deliveries()
    uid = str(user_id)
    deliveries.setdefault(uid, {})
    deliveries[uid][broadcast_id] = int(chat_message_id)
    save_deliveries(deliveries)

def unmark_broadcast_everywhere(broadcast_id: str) -> None:
    deliveries = load_deliveries()
    changed = False
    for uid in list(deliveries.keys()):
        if broadcast_id in deliveries[uid]:
            deliveries[uid].pop(broadcast_id, None)
            changed = True
        if not deliveries[uid]:
            deliveries.pop(uid, None)
            changed = True
    if changed:
        save_deliveries(deliveries)

def get_user_ids() -> list[int]:
    user_ids: list[int] = []
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            for idx, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                if idx == 0 and line.lower().startswith("user_id"):
                    continue
                parts = [p.strip() for p in line.split("|")]
                if not parts:
                    continue
                uid_str = parts[0]
                if uid_str.isdigit():
                    user_ids.append(int(uid_str))
    except FileNotFoundError:
        pass
    return user_ids

async def send_missing_broadcasts_to_user(user_id: int) -> None:
    """
    На /start отправляет пользователю все рассылки из архива,
    которых он ещё не получал.
    """
    if ARCHIVE_CHAT_ID is None:
        return

    broadcasts = load_broadcasts()
    if not broadcasts:
        return

    # сортировка по времени создания (если нет — по message_id)
    def _key(b: dict[str, Any]):
        return (b.get("created_at", ""), int(b.get("archive_message_id", 0)))

    broadcasts_sorted = sorted(broadcasts, key=_key)

    for b in broadcasts_sorted:
        archive_mid = b.get("archive_message_id")
        if not isinstance(archive_mid, int):
            continue
        bid = str(archive_mid)

        if was_delivered(user_id, bid):
            continue

        try:
            msg = await bot.forward_message(
                chat_id=user_id,
                from_chat_id=ARCHIVE_CHAT_ID,
                message_id=archive_mid,
            )
            mark_delivered(user_id, bid, msg.message_id)
            # лёгкая пауза (на случай если много архивных сообщений)
            await asyncio.sleep(0.05)
        except Exception:
            # пользователь мог заблокировать бота / нет диалога и т.д.
            break

async def delete_broadcast_everywhere(broadcast_id: str) -> tuple[int, int]:
    """
    Удаляет рассылку у всех пользователей и из архива.
    Возвращает (успешно удалено, ошибок).
    """
    ok = 0
    fail = 0

    if ARCHIVE_CHAT_ID is None:
        return 0, 0

    deliveries = load_deliveries()

    # 1) удалить у пользователей
    ops = 0
    for uid_str, mp in list(deliveries.items()):
        if broadcast_id not in mp:
            continue
        try:
            uid = int(uid_str)
            mid = int(mp[broadcast_id])
            await bot.delete_message(chat_id=uid, message_id=mid)
            ok += 1
        except Exception:
            fail += 1
        ops += 1
        if ops % 25 == 0:
            await asyncio.sleep(1)

    # 2) удалить из архива
    try:
        await bot.delete_message(chat_id=ARCHIVE_CHAT_ID, message_id=int(broadcast_id))
    except Exception:
        # архив мог не удалиться (нет прав/уже удалено)
        pass

    # 3) убрать из deliveries.json
    unmark_broadcast_everywhere(broadcast_id)

    # 4) убрать из broadcasts.json
    broadcasts = load_broadcasts()
    broadcasts = [b for b in broadcasts if str(b.get("archive_message_id")) != broadcast_id]
    save_broadcasts(broadcasts)

    return ok, fail

# ============ ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ============

def save_user(user: types.User):
    """Сохранить пользователя, который нажал /start, в красивом виде."""
    ensure_files()

    STANDARD_HEADER = "user_id | Full_name | @username | first_seen_at"

    need_reset = False
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        if not first_line or not first_line.startswith("user_id |"):
            need_reset = True
    except FileNotFoundError:
        need_reset = True

    if need_reset:
        with open(USERS_FILE, "w", encoding="utf-8") as f:
            f.write(STANDARD_HEADER + "\n")

    existing_ids = set()
    with open(USERS_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.lower().startswith("user_id"):
                continue
            parts = [p.strip() for p in line.split("|")]
            if parts and parts[0].isdigit():
                existing_ids.add(parts[0])

    uid = str(user.id)
    if uid not in existing_ids:
        full_name = user.full_name or ""
        username = f"@{user.username}" if user.username else ""
        first_seen = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with open(USERS_FILE, "a", encoding="utf-8") as f:
            f.write(f"{uid} | {full_name} | {username} | {first_seen}\n")

def log_action(user: types.User, action: str):
    """Логируем любое действие пользователя."""
    ensure_files()
    with open(STATS_FILE, "a", encoding="utf-8") as f:
        f.write(f"{datetime.now().isoformat()};{user.id};{user.username or ''};{action}\n")

async def cleanup_user_messages(chat_id: int, user_id: int):
    """
    Удаляем все прошлые сообщения бота для этого пользователя,
    кроме приветствия.
    """
    msgs = user_messages.get(user_id, set())
    greet_id = greeting_messages.get(user_id)

    for mid in list(msgs):
        if greet_id is not None and mid == greet_id:
            continue
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass

    user_messages[user_id] = set()
    if greet_id is not None:
        user_messages[user_id].add(greet_id)

def remember_bot_message(user_id: int, message_id: int):
    """Запоминаем id сообщения бота, чтобы потом можно было удалить."""
    user_messages.setdefault(user_id, set()).add(message_id)

def get_main_keyboard(is_admin: bool) -> ReplyKeyboardMarkup:
    keyboard: list[list[KeyboardButton]] = []

    keyboard.append([KeyboardButton(text="📦 НАЛИЧИЕ СТОКА")])
    keyboard.append(
        [
            KeyboardButton(text="🔥 Отзывы"),
            KeyboardButton(text="ℹ️ ИНФОРМАЦИЯ ДЛЯ ЗАКАЗА"),
        ]
    )
    keyboard.append(
        [
            KeyboardButton(text="📣 ИНФОРМАЦИОННЫЙ КАНАЛ"),
            KeyboardButton(text="👨‍💻Связь с менеджером"),
        ]
    )

    if is_admin:
        keyboard.append(
            [
                KeyboardButton(text="📨 Рассылка"),
                KeyboardButton(text="📊 Статистика"),
            ]
        )

    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

# Человекочитаемые названия для действий в статистике
ACTION_LABELS = {
    "start": "▶️ Старт бота (/start)",
    "button_stock": "📦 Наличие стока (кнопка)",
    "button_reviews": "🔥 Отзывы (кнопка)",
    "button_info_main": "ℹ️ Инфо для заказа (меню)",
    "button_channel": "📣 Информационный канал (кнопка)",
    "button_manager": "👨‍💻 Связь с менеджером (кнопка)",
    "info_1": "ℹ️ Инфо: формирование заказа",
    "info_2": "ℹ️ Инфо: сбор заказа",
    "info_3": "ℹ️ Инфо: способы оплаты",
    "info_4": "ℹ️ Инфо: самовывоз",
    "info_5": "ℹ️ Инфо: сроки доставки",
    "admin_broadcast_button": "👑 Админ: рассылка (меню)",
    "admin_broadcast_prepare": "👑 Админ: сообщение для рассылки получено",
    "admin_broadcast_start": "👑 Админ: запуск рассылки",
    "admin_broadcast_cancel": "👑 Админ: отмена рассылки",
    "admin_stats_button": "👑 Админ: просмотр статистики",
}

def load_stats_summary():
    total_users = 0
    total_start = 0
    button_counts: dict[str, int] = {}

    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            lines = [l for l in f if l.strip()]
            if lines and lines[0].lower().startswith("user_id"):
                total_users = len(lines) - 1
            else:
                total_users = len(lines)
    except FileNotFoundError:
        total_users = 0

    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                parts = line.split(";")
                if len(parts) < 4:
                    continue
                action = parts[3]
                button_counts[action] = button_counts.get(action, 0) + 1
    except FileNotFoundError:
        pass

    total_start = button_counts.get("start", 0)
    return total_users, total_start, button_counts

# ============ ТЕКСТЫ ДЛЯ ℹ️ ИНФОРМАЦИЯ ДЛЯ ЗАКАЗА ============

INFO_1_TEXT = (
    "<b>Формирование заказа</b> 🧾\n\n"
    "Вы отправляете список нужных вкусов и позиций из актуального наличия Tasty Shop.\n\n"
    "<b>Минимальный заказ:</b> от 20 единиц\n"
    "<b>Максимальный заказ:</b> до 1000 единиц (больше — по согласованию)\n\n"
    "<b>Стоимость</b> зависит от объёма.\n"
    "Постоянным и крупным клиентам предоставляются <b>индивидуальные скидки</b>\n\n"
    "Вы можете свободно комбинировать любые позиции и вкусы — список формируется "
    "полностью под ваши задачи."
)

INFO_2_TEXT = (
    "<b>Сбор заказа</b> 📦\n\n"
    "<b>Порядок оформления заказа в Tasty Shop:</b>\n\n"
    "1️⃣ <b>Составление списка.</b>\n"
    "   Вы выбираете нужные вкусы и товары по нашей таблице и отправляете нам готовый список.\n\n"
    "2️⃣ <b>Сборка заказа.</b>\n"
    "   Мы формируем ваш заказ и отправляем вам <b>видеоотчёт</b> с полностью собранным заказом.\n\n"
    "3️⃣ <b>Оплата.</b>\n"
    "   Вы выбираете удобный способ оплаты и подтверждаете заказ.\n\n"
    "4️⃣ <b>Доставка.</b>\n"
    "   Сообщаете предпочитаемую службу доставки и данные для отправки."
)

INFO_3_TEXT = (
    "<b>Способы оплаты</b> 💳\n\n"
    "Мы принимаем следующие варианты оплаты:\n\n"
    "<b>• USDT TRC-20</b>\n"
    "<b>• Revolut</b>\n"
    "<b>• Monese</b>\n"
    "<b>• Zen</b>\n"
    "<b>• Wise</b>\n"
    "<b>• Monobank</b>\n"
    "<b>• Наличные (на точках самовывоза)</b>\n"
    "<b>• Евро-счёт</b>\n"
    "<b>• PayPal</b>\n\n"
    "📦 По Польше первый заказ можем отправить <b>наложенным платежом InPost</b>."
)

INFO_4_TEXT = (
    "<b>Самовывоз</b> 📍\n\n"
    "Доступны точки самовывоза в Европе:\n\n"
    "<b>• Варшава</b>\n"
    "<b>• Краков</b>\n"
    "<b>• Познань</b>\n"
    "<b>• Кельце</b>\n"
    "<b>• Берлин</b>\n"
    "<b>• Рига</b>\n\n"
    "На точках самовывоза вы можете <b>проверить заказ на месте</b> и оплатить его наличными."
)

INFO_5_TEXT = (
    "<b>Сроки и варианты доставки</b> 🚚\n\n"
    "Мы осуществляем доставку по всей Европе. Доступные курьерские службы:\n\n"
    "• <b>DPD</b> — ~3–5 рабочих дней\n"
    "• <b>GLS</b> — ~3–5 рабочих дней\n"
    "• <b>InPost</b> — ~2 рабочих дня\n\n"
    "Сроки могут незначительно отличаться в зависимости от региона и загрузки служб доставки."
)

def get_info_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🧾 Формирование заказа", callback_data="info_1")],
            [InlineKeyboardButton(text="📦 Сбор заказа", callback_data="info_2")],
            [InlineKeyboardButton(text="💳 Способы оплаты", callback_data="info_3")],
            [InlineKeyboardButton(text="📍 Самовывоз", callback_data="info_4")],
            [InlineKeyboardButton(text="🚚 Сроки доставки", callback_data="info_5")],
        ]
    )

# ============ КОМАНДЫ ============

@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user = message.from_user
    if user is None:
        return

    save_user(user)
    log_action(user, "start")

    try:
        await message.delete()
    except Exception:
        pass

    await cleanup_user_messages(chat_id=message.chat.id, user_id=user.id)

    kb = get_main_keyboard(is_admin=user.id in ADMIN_IDS)
    photo = FSInputFile("assets/tastyshop.jpg")

    caption = (
        "<b>🔥 TASTY SHOP</b> — надёжный поставщик электронных девайсов и жидкостей по всей Европе.\n\n"
        "Выберите нужный раздел на клавиатуре ниже 👇"
    )

    msg = await message.answer_photo(
        photo=photo,
        caption=caption,
        reply_markup=kb,
    )

    greeting_messages[user.id] = msg.message_id
    remember_bot_message(user.id, msg.message_id)

    # ✅ Умная рассылка новым: отправляем всё из архива, чего ещё не получал
    await send_missing_broadcasts_to_user(user.id)

@dp.message(Command("myid"))
async def cmd_myid(message: types.Message):
    user = message.from_user
    if user is None:
        return

    await message.answer(
        f"Твой Telegram ID: <code>{user.id}</code>\n\n"
        "Добавь его в переменную окружения <code>ADMIN_IDS</code> (через запятую, если админов несколько) "
        "и перезапусти бота."
    )

@dp.message(Command("chatid"))
async def cmd_chatid(message: types.Message):
    await message.answer(f"chat_id: <code>{message.chat.id}</code>")

# ============ ОБРАБОТЧИКИ КНОПОК ПОЛЬЗОВАТЕЛЯ ============

@dp.message(F.text == "📦 НАЛИЧИЕ СТОКА")
async def handle_stock(message: types.Message):
    user = message.from_user
    if user is None:
        return

    log_action(user, "button_stock")
    await cleanup_user_messages(message.chat.id, user.id)

    try:
        await message.delete()
    except Exception:
        pass

    text = (
        "📦 <b>Актуальное наличие стока</b>\n\n"
        "Список доступного стока всегда обновляется в таблице по кнопке ниже:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="📊 ОТКРЫТЬ ТАБЛИЦУ",
                    url="https://docs.google.com/spreadsheets/d/1UK8U5I_MNl3xjTxLG0-CJFCGBa41DHjuhK_ep7o7D5k/edit?usp=sharing",
                )
            ]
        ]
    )

    msg = await message.answer(text, reply_markup=kb)
    remember_bot_message(user.id, msg.message_id)

@dp.message(F.text == "👨‍💻Связь с менеджером")
async def handle_manager(message: types.Message):
    user = message.from_user
    if user is None:
        return

    log_action(user, "button_manager")
    await cleanup_user_messages(message.chat.id, user.id)

    try:
        await message.delete()
    except Exception:
        pass

    text = (
        "👨‍💻 <b>Связь с оптовым менеджером</b>\n\n"
        "Нажмите на кнопку ниже, чтобы сразу написать менеджеру в Telegram:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Написать менеджеру", url="https://t.me/tasty2opt")]
        ]
    )

    msg = await message.answer(text, reply_markup=kb)
    remember_bot_message(user.id, msg.message_id)

@dp.message(F.text == "📣 ИНФОРМАЦИОННЫЙ КАНАЛ")
async def handle_channel(message: types.Message):
    user = message.from_user
    if user is None:
        return

    log_action(user, "button_channel")
    await cleanup_user_messages(message.chat.id, user.id)

    try:
        await message.delete()
    except Exception:
        pass

    text = (
        "📣 <b>Информационный канал Tasty Shop</b>\n\n"
        "Все важные объявления, новости и обновления стока публикуются здесь:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="TASTY SHOP🩸", url="https://t.me/+-LRYgeaxmyRhMzVk")]
        ]
    )

    msg = await message.answer(text, reply_markup=kb)
    remember_bot_message(user.id, msg.message_id)

@dp.message(F.text == "🔥 Отзывы")
async def handle_reviews(message: types.Message):
    user = message.from_user
    if user is None:
        return

    log_action(user, "button_reviews")
    await cleanup_user_messages(message.chat.id, user.id)

    try:
        await message.delete()
    except Exception:
        pass

    text = (
        "🔥 <b>Отзывы клиентов</b>\n\n"
        "Посмотреть отзывы о работе Tasty Shop вы можете по кнопке ниже:"
    )
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="ОТКРЫТЬ ОТЗЫВЫ", url="https://t.me/+-LRYgeaxmyRhMzVk")]
        ]
    )

    msg = await message.answer(text, reply_markup=kb)
    remember_bot_message(user.id, msg.message_id)

@dp.message(F.text == "ℹ️ ИНФОРМАЦИЯ ДЛЯ ЗАКАЗА")
async def handle_order_info(message: types.Message):
    user = message.from_user
    if user is None:
        return

    log_action(user, "button_info_main")
    await cleanup_user_messages(message.chat.id, user.id)

    try:
        await message.delete()
    except Exception:
        pass

    text = (
        "ℹ️ <b>Информация для заказа</b>\n\n"
        "Выберите интересующий раздел ниже:"
    )
    kb = get_info_keyboard()

    msg = await message.answer(text, reply_markup=kb)
    remember_bot_message(user.id, msg.message_id)

# ============ CALLBACK ДЛЯ 5 ПУНКТОВ ИНФОРМАЦИИ ============

@dp.callback_query(F.data.startswith("info_"))
async def process_info_callback(callback: types.CallbackQuery):
    user = callback.from_user
    if user is None:
        await callback.answer()
        return

    data = callback.data or ""

    if data == "info_1":
        text = INFO_1_TEXT
        log_action(user, "info_1")
    elif data == "info_2":
        text = INFO_2_TEXT
        log_action(user, "info_2")
    elif data == "info_3":
        text = INFO_3_TEXT
        log_action(user, "info_3")
    elif data == "info_4":
        text = INFO_4_TEXT
        log_action(user, "info_4")
    else:
        text = INFO_5_TEXT
        log_action(user, "info_5")

    try:
        await callback.message.edit_text(text, reply_markup=get_info_keyboard())
    except Exception:
        msg = await callback.message.answer(text, reply_markup=get_info_keyboard())
        remember_bot_message(user.id, msg.message_id)

    await callback.answer()

# ============ АДМИН: РАССЫЛКА (МЕНЮ) ============

def get_broadcast_menu_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ Новая рассылка", callback_data="broadcast_menu_new")],
            [InlineKeyboardButton(text="🗑 Удалить рассылку", callback_data="broadcast_menu_delete")],
        ]
    )

def get_broadcast_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel_mode")]
        ]
    )

@dp.message(F.text.contains("Рассылка"))
async def admin_broadcast_command(message: types.Message):
    user = message.from_user
    if user is None:
        return

    if user.id not in ADMIN_IDS:
        await message.answer(
            "🚫 <b>Эта кнопка доступна только администратору.</b>\n\n"
            f"Твой Telegram ID: <code>{user.id}</code>\n"
            "Добавь его в переменную окружения <code>ADMIN_IDS</code> и перезапусти бота."
        )
        return

    log_action(user, "admin_broadcast_button")

    await cleanup_user_messages(message.chat.id, user.id)
    try:
        await message.delete()
    except Exception:
        pass

    if ARCHIVE_CHAT_ID is None:
        msg = await message.answer(
            "⚠️ <b>ARCHIVE_CHAT_ID не настроен.</b>\n\n"
            "Добавь бота в группу-архив и укажи её ID в .env:\n"
            "<code>ARCHIVE_CHAT_ID=-100...</code>\n\n"
            "Чтобы узнать ID группы — напиши в ней /chatid."
        )
        remember_bot_message(user.id, msg.message_id)
        return

    text = (
        "📨 <b>Умная рассылка</b>\n\n"
        "• Рассылка идёт <b>пересылкой</b> (forward)\n"
        "• Каждая рассылка сохраняется в <b>архив-группе</b>\n"
        "• Новые пользователи на /start получают <b>все прошлые рассылки</b>, которых ещё не получали\n"
        "• Можно <b>удалить рассылку</b> — она удалится у всех пользователей\n"
    )
    msg = await message.answer(text, reply_markup=get_broadcast_menu_kb())
    remember_bot_message(user.id, msg.message_id)

@dp.callback_query(F.data == "broadcast_menu_new")
async def broadcast_menu_new(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    pending_broadcast_admins.add(admin.id)
    broadcast_drafts.pop(admin.id, None)

    text = (
        "➕ <b>Новая рассылка</b>\n\n"
        "Отправь <b>одно сообщение</b> (текст/фото/видео и т.д.), которое нужно разослать.\n\n"
        "Я сначала <b>перешлю его в архив-группу</b>, потом покажу предпросмотр и спрошу подтверждение."
    )
    try:
        await callback.message.edit_text(text, reply_markup=get_broadcast_cancel_kb())
    except Exception:
        await callback.message.answer(text, reply_markup=get_broadcast_cancel_kb())

    await callback.answer()

@dp.callback_query(F.data == "broadcast_cancel_mode")
async def broadcast_cancel_mode(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    pending_broadcast_admins.discard(admin.id)

    # если был черновик — удалим из архива
    draft = broadcast_drafts.pop(admin.id, None)
    if draft and ARCHIVE_CHAT_ID is not None:
        try:
            await bot.delete_message(chat_id=ARCHIVE_CHAT_ID, message_id=draft["archive_message_id"])
        except Exception:
            pass

    try:
        await callback.message.edit_text("❌ Отменено.")
    except Exception:
        pass

    await callback.answer()

# ============ АДМИН: ПОЛУЧЕНИЕ СООБЩЕНИЯ ДЛЯ РАССЫЛКИ ============

@dp.message()
async def admin_broadcast_prepare(message: types.Message):
    user = message.from_user
    if user is None:
        raise SkipHandler

    if user.id not in ADMIN_IDS or user.id not in pending_broadcast_admins:
        raise SkipHandler

    if ARCHIVE_CHAT_ID is None:
        pending_broadcast_admins.discard(user.id)
        await message.answer("⚠️ ARCHIVE_CHAT_ID не настроен, рассылка невозможна.")
        return

    pending_broadcast_admins.discard(user.id)
    log_action(user, "admin_broadcast_prepare")

    # 1) сохраняем в архив пересылкой (важно для premium emoji)
    try:
        archive_msg = await bot.forward_message(
            chat_id=ARCHIVE_CHAT_ID,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
    except Exception as e:
        await message.answer(f"❌ Не удалось переслать в архив-группу: {e}")
        return

    broadcast_drafts[user.id] = {"archive_message_id": archive_msg.message_id}

    # 2) предпросмотр админа: пересылаем из архива ему же
    try:
        await bot.forward_message(
            chat_id=message.chat.id,
            from_chat_id=ARCHIVE_CHAT_ID,
            message_id=archive_msg.message_id,
        )
    except Exception:
        pass

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="🚀 Разослать", callback_data="broadcast_send"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_cancel"),
            ]
        ]
    )

    text = (
        "👀 <b>Предпросмотр</b>\n\n"
        "Это сообщение будет разослано <b>пересылкой</b>.\n"
        "Продолжить?"
    )
    preview_msg = await message.answer(text, reply_markup=kb)
    remember_bot_message(user.id, preview_msg.message_id)

# ============ АДМИН: ОТПРАВИТЬ / ОТМЕНА ============

@dp.callback_query(F.data.in_({"broadcast_send", "broadcast_cancel"}))
async def process_broadcast_action(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    draft = broadcast_drafts.get(admin.id)
    if not draft:
        await callback.answer("Черновик не найден. Создай рассылку заново.", show_alert=True)
        return

    archive_mid = draft["archive_message_id"]
    broadcast_id = str(archive_mid)

    if callback.data == "broadcast_cancel":
        broadcast_drafts.pop(admin.id, None)
        log_action(admin, "admin_broadcast_cancel")

        if ARCHIVE_CHAT_ID is not None:
            try:
                await bot.delete_message(chat_id=ARCHIVE_CHAT_ID, message_id=archive_mid)
            except Exception:
                pass

        try:
            await callback.message.edit_text("❌ Рассылка отменена (и удалена из архива).")
        except Exception:
            pass

        await callback.answer("Отменено.")
        return

    # SEND
    await callback.answer("Запускаю рассылку...")

    log_action(admin, "admin_broadcast_start")

    # добавляем рассылку в список (архив) — чтобы новым юзерам приходила
    broadcasts = load_broadcasts()
    if not any(str(b.get("archive_message_id")) == broadcast_id for b in broadcasts):
        broadcasts.append(
            {
                "archive_message_id": archive_mid,
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "created_by": admin.id,
            }
        )
        save_broadcasts(broadcasts)

    user_ids = get_user_ids()

    success = 0
    failed = 0

    ops = 0
    for uid in user_ids:
        # не шлём повторно тем, кто уже получал
        if was_delivered(uid, broadcast_id):
            continue
        try:
            msg = await bot.forward_message(
                chat_id=uid,
                from_chat_id=ARCHIVE_CHAT_ID,
                message_id=archive_mid,
            )
            mark_delivered(uid, broadcast_id, msg.message_id)
            success += 1
        except Exception:
            failed += 1

        ops += 1
        if ops % 25 == 0:
            await asyncio.sleep(1)

    broadcast_drafts.pop(admin.id, None)

    text = (
        "✅ <b>Рассылка завершена</b>\n\n"
        f"📬 Успешно доставлено: <b>{success}</b>\n"
        f"⚠️ Ошибок: <b>{failed}</b>\n\n"
        f"🗂 ID рассылки (для удаления): <code>{broadcast_id}</code>"
    )

    try:
        msg = await callback.message.edit_text(text)
    except Exception:
        msg = await callback.message.answer(text)

    remember_bot_message(admin.id, msg.message_id)
    log_action(admin, f"admin_broadcast_done_success_{success}_failed_{failed}")

# ============ АДМИН: УДАЛЕНИЕ РАССЫЛКИ ============

@dp.callback_query(F.data == "broadcast_menu_delete")
async def broadcast_menu_delete(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    broadcasts = load_broadcasts()
    if not broadcasts:
        try:
            await callback.message.edit_text("🗑 <b>Удаление рассылки</b>\n\nАрхив пуст.", reply_markup=get_broadcast_menu_kb())
        except Exception:
            await callback.message.answer("Архив пуст.", reply_markup=get_broadcast_menu_kb())
        await callback.answer()
        return

    # показываем последние 10
    broadcasts_sorted = sorted(
        broadcasts,
        key=lambda b: (b.get("created_at", ""), int(b.get("archive_message_id", 0))),
        reverse=True,
    )[:10]

    kb_rows: list[list[InlineKeyboardButton]] = []
    for b in broadcasts_sorted:
        mid = b.get("archive_message_id")
        created = b.get("created_at", "")
        if not isinstance(mid, int):
            continue
        label = f"🗑 ID {mid} | {created}"
        kb_rows.append([InlineKeyboardButton(text=label, callback_data=f"broadcast_delete_pick:{mid}")])

    kb_rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="broadcast_back_to_menu")])

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    text = (
        "🗑 <b>Удаление рассылки</b>\n\n"
        "Выбери рассылку — бот удалит её <b>у всех пользователей</b> и из архива."
    )

    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)

    await callback.answer()

@dp.callback_query(F.data == "broadcast_back_to_menu")
async def broadcast_back_to_menu(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return
    try:
        await callback.message.edit_text("📨 <b>Умная рассылка</b>", reply_markup=get_broadcast_menu_kb())
    except Exception:
        await callback.message.answer("📨 <b>Умная рассылка</b>", reply_markup=get_broadcast_menu_kb())
    await callback.answer()

@dp.callback_query(F.data.startswith("broadcast_delete_pick:"))
async def broadcast_delete_pick(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    _, bid = callback.data.split(":", 1)

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"broadcast_delete_confirm:{bid}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="broadcast_menu_delete"),
            ]
        ]
    )

    text = (
        f"⚠️ <b>Подтверждение удаления</b>\n\n"
        f"Удалить рассылку ID <code>{bid}</code>:\n"
        "• из архива\n"
        "• и <b>у всех пользователей</b> (сообщение исчезнет из их чатов)\n\n"
        "Продолжить?"
    )

    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)

    await callback.answer()

@dp.callback_query(F.data.startswith("broadcast_delete_confirm:"))
async def broadcast_delete_confirm(callback: types.CallbackQuery):
    admin = callback.from_user
    if admin is None or admin.id not in ADMIN_IDS:
        await callback.answer("Недостаточно прав.", show_alert=True)
        return

    _, bid = callback.data.split(":", 1)

    await callback.answer("Удаляю...")

    ok, fail = await delete_broadcast_everywhere(bid)

    text = (
        "🗑 <b>Удаление завершено</b>\n\n"
        f"✅ Удалено у пользователей: <b>{ok}</b>\n"
        f"⚠️ Ошибок при удалении: <b>{fail}</b>\n\n"
        "Если нужно — создай новую рассылку заново."
    )

    try:
        await callback.message.edit_text(text, reply_markup=get_broadcast_menu_kb())
    except Exception:
        await callback.message.answer(text, reply_markup=get_broadcast_menu_kb())

# ============ АДМИН: СТАТИСТИКА + TXT ПОЛЬЗОВАТЕЛЕЙ ============

@dp.message(F.text.contains("Статистика"))
async def admin_stats(message: types.Message):
    user = message.from_user
    if user is None:
        return

    if user.id not in ADMIN_IDS:
        await message.answer(
            "🚫 <b>Раздел «Статистика» доступен только администратору.</b>\n\n"
            f"Твой Telegram ID: <code>{user.id}</code>\n"
            "Добавь его в переменную окружения <code>ADMIN_IDS</code> и перезапусти бота."
        )
        return

    log_action(user, "admin_stats_button")

    await cleanup_user_messages(message.chat.id, user.id)

    try:
        await message.delete()
    except Exception:
        pass

    total_users, total_start, button_counts = load_stats_summary()

    text_lines = [
        "📊 <b>Статистика бота</b>",
        "",
        f"👤 Администратор: <code>{user.id}</code> (@{user.username or 'без_username'})",
        "",
        f"👥 Всего пользователей (нажали /start): <b>{total_users}</b>",
        f"▶️ Всего срабатываний /start: <b>{total_start}</b>",
        "",
        "📌 <b>Нажатия по действиям:</b>",
    ]

    display_counts: dict[str, int] = {}

    for key, val in button_counts.items():
        if key.startswith("admin_broadcast_done_success_"):
            label = "👑 Админ: рассылка завершена"
        else:
            label = ACTION_LABELS.get(key)
            if label is None:
                label = f"🔧 Служебное событие: {key}"

        display_counts[label] = display_counts.get(label, 0) + val

    for label, val in sorted(display_counts.items(), key=lambda x: x[0]):
        text_lines.append(f"• {label}: <b>{val}</b>")

    msg = await message.answer("\n".join(text_lines))
    remember_bot_message(user.id, msg.message_id)

    try:
        if os.path.exists(USERS_FILE) and os.path.getsize(USERS_FILE) > 0:
            doc = FSInputFile(USERS_FILE)
            doc_msg = await message.answer_document(
                document=doc,
                caption="📄 Список всех пользователей (users.txt)",
            )
            remember_bot_message(user.id, doc_msg.message_id)
        else:
            info_msg = await message.answer("Файл <code>users.txt</code> пока пуст.")
            remember_bot_message(user.id, info_msg.message_id)
    except Exception as e:
        logging.error(f"Не удалось отправить users.txt: {e}")
        err_msg = await message.answer("Ошибка при отправке файла <code>users.txt</code>.")
        remember_bot_message(user.id, err_msg.message_id)

# ============ ЗАПУСК БОТА ============

async def main():
    print("Bot started...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
