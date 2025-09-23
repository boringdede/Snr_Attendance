# bot.py — Полный рабочий файл (aiogram v2.25.1)
# pip install aiogram==2.25.1

import csv
import logging
import re
import asyncio
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from pathlib import Path
import os

from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.exceptions import Throttled
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

# ============ НАСТРОЙКИ ============
# НЕ коммить в публичный репозиторий с реальным токеном!
HARDCODED_FALLBACK_TOKEN = "8278332572:AAFT7ijU1Gc_I3KmXsmD7QNXaWSY-OXd39A"  # ← подставь локально (или используй env)
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or HARDCODED_FALLBACK_TOKEN
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set (и HARDCODED_FALLBACK_TOKEN не задан).")

ADMIN_IDS = {1790286972}             # добавь ещё id админов при необходимости
ADMIN_CHAT_IDS = {-1002362042916}    # куда слать уведомления

RADIUS_M_DEFAULT = 200.0
CITY_TZ_HOURS = 5
LATE_GRACE_MIN = 10

# ---- ЛОГИ ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("snr-checkin-bot")

# Часовой пояс
try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    try:
        TZ = ZoneInfo("Asia/Tashkent")
    except ZoneInfoNotFoundError:
        TZ = timezone(timedelta(hours=CITY_TZ_HOURS))
except Exception:
    TZ = timezone(timedelta(hours=CITY_TZ_HOURS))

# ---- ФАЙЛЫ ----
DATA_DIR = Path("."); DATA_DIR.mkdir(parents=True, exist_ok=True)
CHECKS_CSV = DATA_DIR / "checks.csv"
PROFILES_CSV = DATA_DIR / "profiles.csv"

# ====== МЕСТА ======
BASE_PLACES = {
    "SNR School": {"full": "SNR School (офис)", "lat": 41.322921, "lon": 69.277808, "radius_m": 200.0, "free_time": True},
    "559 гос": {"full": "559 государственный садик", "lat": 41.303288, "lon": 69.292031, "radius_m": 200.0, "free_time": False},
    "First kids": {"full": "First kids", "lat": 41.329848, "lon": 69.286872, "radius_m": 200.0, "free_time": False},
    "Domik": {"full": "ДОМИК", "lat": 41.321701, "lon": 69.315380, "radius_m": 200.0, "free_time": False},
    "Small steps": {"full": "Small steps", "lat": 41.294155, "lon": 69.189863, "radius_m": 200.0, "free_time": False},
    "STARKIDS": {"full": "STARKIDS", "lat": 41.298992, "lon": 69.260579, "radius_m": 200.0, "free_time": False},
    "Академия Талантов": {"full": "Академия Талантов", "lat": 41.313393, "lon": 69.294289, "radius_m": 200, "free_time": False},
    "324 гос": {"full": "324 государственный садик", "lat": 41.335171, "lon": 69.335863, "radius_m": 200, "free_time": False},
}
PLACES = {}

# ====== РАСПИСАНИЕ ======
# SCHEDULE[weekday] = list[ {"start","end","place"} ]
SCHEDULE = {
    0: [ {"start": "09:00", "end": "12:30", "place": "559 гос"},
         {"start": "15:45", "end": "16:30", "place": "559 гос"},
         {"start": "10:00", "end": "11:30", "place": "First kids"} ],
    1: [ {"start": "10:30", "end": "11:00", "place": "Domik"},
         {"start": "15:00", "end": "16:00", "place": "324 гос"} ],
    2: [ {"start": "09:00", "end": "12:30", "place": "559 гос"},
         {"start": "15:45", "end": "16:30", "place": "559 гос"},
         {"start": "10:00", "end": "11:00", "place": "Small steps"},
         {"start": "10:30", "end": "16:30", "place": "Академия Талантов"} ],
    3: [ {"start": "10:30", "end": "11:00", "place": "Domik"} ],
    4: [ {"start": "09:30", "end": "12:30", "place": "STARKIDS"},
         {"start": "15:00", "end": "16:00", "place": "324 гос"} ],
    5: [],
    6: [],
}

# ====== БОТ ======
bot = Bot(token=API_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# runtime
STATE = {}              # user_id -> {...}
PROFILES = {}
LATE_SENT_SLOTS = set()

# ====== УТИЛИТЫ ======
def ensure_files():
    if not CHECKS_CSV.exists():
        with CHECKS_CSV.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f, delimiter=";").writerow([
                "telegram_id","teacher_name","phone",
                "action","place_key","place_full",
                "date","time","weekday",
                "slot_start","slot_end",
                "lat","lon","distance_m","in_radius",
                "on_time","notes"
            ])
    if not PROFILES_CSV.exists():
        with PROFILES_CSV.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f, delimiter=";").writerow(["telegram_id","teacher_name","phone"])

def safe_dict_reader(path: Path):
    if not path.exists(): return []
    rows=[]
    with path.open("r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f, delimiter=";")
        for row in r:
            if row: rows.append(row)
    return rows

def load_profiles_cache():
    PROFILES.clear()
    for row in safe_dict_reader(PROFILES_CSV):
        try:
            PROFILES[int(row["telegram_id"])] = {"name": row["teacher_name"], "phone": row["phone"]}
        except Exception:
            continue

def load_places_runtime():
    global PLACES
    PLACES = {k: dict(v) for k, v in BASE_PLACES.items()}

def weekday_ru(dt: datetime) -> str:
    return ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"][dt.weekday()]

def day_short(wd: int) -> str:
    return ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][wd]

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    from math import radians, sin, cos, atan2, sqrt
    p1, p2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlmb = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(p1)*cos(p2)*sin(dlmb/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def pretty_m(m): return f"{int(round(m))} м"
def gmaps(lat, lon): return f"https://maps.google.com/?q={lat},{lon}"

def main_kb(user_id: int | None = None):
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("Показать расписание на сегодня"))
    kb.add(KeyboardButton("Отметиться (выбрать слот)"))
    kb.add(KeyboardButton("Назад в меню"))
    return kb

def ask_contact_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Поделиться контактом", request_contact=True))
    kb.add(KeyboardButton("Назад в меню"))
    return kb

def actions_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Чек-ин (приход)", callback_data="act:in"))
    kb.add(InlineKeyboardButton("Чек-аут (уход)",  callback_data="act:out"))
    return kb

def load_profile(uid: int):
    if uid in PROFILES: return PROFILES[uid]
    for row in safe_dict_reader(PROFILES_CSV):
        if row.get("telegram_id") == str(uid):
            PROFILES[uid] = {"name": row["teacher_name"], "phone": row["phone"]}
            return PROFILES[uid]
    return None

def save_profile(uid: int, name: str, phone: str|None):
    rows = {}
    for row in safe_dict_reader(PROFILES_CSV):
        try:
            rows[int(row["telegram_id"])] = (row["teacher_name"], row.get("phone",""))
        except Exception:
            continue
    if uid in rows: return
    rows[uid] = (name, phone or "")
    with PROFILES_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["telegram_id","teacher_name","phone"])
        for k, (n, p) in rows.items():
            w.writerow([k, n, p])
    PROFILES[uid] = {"name": name, "phone": phone or ""}

async def notify_admins(text: str):
    for chat_id in ADMIN_CHAT_IDS:
        with suppress(Exception):
            await bot.send_message(chat_id, text, disable_web_page_preview=True)

# ====== ПАРСИНГ ДНЕЙ ======
RU_DAYS = {
    "пн":0, "пон":0, "понедельник":0,
    "вт":1, "вторник":1,
    "ср":2, "среда":2,
    "чт":3, "четверг":3,
    "пт":4, "пятница":4,
    "сб":5, "суббота":5,
    "вс":6, "воскресенье":6,
}
def parse_days(text: str):
    txt = text.strip().lower()
    if txt in {"все", "каждый день", "ежедневно", "всю неделю"}:
        return list(range(7))
    parts = re.split(r"[,\s;]+", txt)
    out = []
    for p in parts:
        if not p: continue
        if p.isdigit():
            n = int(p)
            if n in range(7): out.append(n)
            elif n in range(1,8): out.append(n-1)
        else:
            p = p.strip(".")
            if p in RU_DAYS: out.append(RU_DAYS[p])
    return sorted(set(out))

# ====== /start ======
@dp.message_handler(commands=["start","help"])
async def on_start(message: types.Message):
    ensure_files()
    load_profiles_cache()
    load_places_runtime()

    uid = message.from_user.id
    prof = load_profile(uid)
    if not prof:
        STATE[uid] = {"phase": "need_name"}
        await message.answer("👋 Привет! Введите, пожалуйста, <b>Имя и Фамилию</b> (например: <i>Азиз Азимов</i>).")
        return
    STATE[uid] = {"phase": "idle"}
    await message.answer(f"Привет, <b>{prof['name']}</b>! Что делаем?", reply_markup=main_kb(uid))

# ====== вспомогательные ======
@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@dp.message_handler(commands=["my"])
async def cmd_my(message: types.Message):
    uid = message.from_user.id
    prof = load_profile(uid) or {"name": message.from_user.full_name, "phone": ""}
    await message.answer(f"👤 Профиль\nИмя: <b>{prof['name']}</b>\nТелефон: <b>{prof.get('phone','')}</b>")

@dp.message_handler(commands=["schedule","today"])
async def cmd_schedule(message: types.Message):
    now = datetime.now(TZ); wd = now.weekday()
    day = SCHEDULE.get(wd, [])
    lines = [f"Расписание на <b>{weekday_ru(now)}</b>:"]
    lines.append("• 🏢 SNR School — свободное время (в любой момент)")
    if not day:
        lines.append("• (слотов по расписанию нет)")
    else:
        for s in day:
            lines.append(f"• {s['place']}: {s['start']}–{s['end']}")
    await message.answer("\n".join(lines))

# ====== АДМИН-ПАНЕЛЬ ======
@dp.message_handler(commands=["admin"])
async def admin_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ У вас нет доступа к админ-панели.")
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ Добавить школу/садик (пошагово)", callback_data="admin:add_wizard"))
    kb.add(InlineKeyboardButton("➕ Добавить урок к школе", callback_data="admin:add_lesson"))
    kb.add(InlineKeyboardButton("📋 Список школ/садиков", callback_data="admin:list_places"))
    kb.add(InlineKeyboardButton("🗑 Удалить урок (в школе)", callback_data="admin:del_lesson"))
    kb.add(InlineKeyboardButton("🗑 Удалить школу/садик полностью", callback_data="admin:del_place"))
    kb.add(InlineKeyboardButton("📆 Слоты на сегодня", callback_data="admin:list_today"))
    await message.answer("🔧 Панель администратора:", reply_markup=kb)

# ====== ТЕКСТ-РОУТЕР ======
@dp.message_handler(content_types=["text"])
async def text_router(message: types.Message):
    uid = message.from_user.id
    txt = (message.text or "").strip()
    st = STATE.get(uid, {})

    # /admin
    if txt.startswith("/admin"):
        await admin_menu(message); return

    # Визард добавления НОВОЙ ШКОЛЫ/САДИКА
    if st.get("phase") == "add_place_name" and uid in ADMIN_IDS:
        name = txt
        if not name:
            await message.answer("Название пустое. Введите название.")
            return
        if name in PLACES:
            await message.answer("Место с таким названием уже есть. Введите другое.")
            return
        STATE[uid] = {"phase": "add_place_coords", "new_place": {"name": name}}
        await message.answer("Координаты: отправьте <code>широта, долгота</code>\nНапример: <code>41.300000, 69.300000</code>")
        return

    if st.get("phase") == "add_place_coords" and uid in ADMIN_IDS:
        m = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*", txt)
        if not m:
            await message.answer("Координаты не распознаны. Пример: <code>41.300000, 69.300000</code>")
            return
        lat = float(m.group(1)); lon = float(m.group(2))
        st["new_place"]["lat"] = lat; st["new_place"]["lon"] = lon
        STATE[uid] = {"phase": "add_place_times", "new_place": st["new_place"]}
        await message.answer("Время урока: <code>HH:MM-HH:MM</code> (напр. 09:00-09:40). Это время потом применим к выбранным дням.")
        return

    if st.get("phase") == "add_place_times" and uid in ADMIN_IDS:
        m = re.fullmatch(r"\s*(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\s*", txt)
        if not m:
            await message.answer("Время не распознано. Пример: <code>09:00-09:40</code>")
            return
        start_s = f"{m.group(1)}:{m.group(2)}"
        end_s   = f"{m.group(3)}:{m.group(4)}"
        st["new_place"]["start"] = start_s
        st["new_place"]["end"] = end_s
        STATE[uid] = {"phase": "add_place_days", "new_place": st["new_place"]}
        await message.answer(
            "Дни недели (через запятую), можно полные названия: Понедельник, Вторник ...\n"
            "Пример: <code>Понедельник, Среда, Пятница</code>"
        )
        return

    if st.get("phase") == "add_place_days" and uid in ADMIN_IDS:
        days = parse_days(txt)
        if not days:
            await message.answer("Дни не распознаны. Пример: <code>Понедельник, Среда</code>")
            return
        np = st["new_place"]
        name, lat, lon = np["name"], np["lat"], np["lon"]
        start_s, end_s = np["start"], np["end"]

        PLACES[name] = {"full": name, "lat": lat, "lon": lon, "radius_m": RADIUS_M_DEFAULT, "free_time": False}
        for wd in days:
            SCHEDULE.setdefault(wd, []).append({"start": start_s, "end": end_s, "place": name})

        STATE[uid] = {"phase": "idle"}
        days_ru = ", ".join(day_short(d) for d in days)
        await message.answer(f"✅ «{name}» добавлен. Урок {start_s}–{end_s} в дни: {days_ru}")
        return

    # Добавление УРОКА к существующей школе (из админки)
    if st.get("phase") == "add_lesson_choose_school" and uid in ADMIN_IDS:
        school = txt
        if school not in PLACES:
            await message.answer("Школа не найдена. Введите точное название из списка.")
            return
        STATE[uid] = {"phase": "add_lesson_time", "school": school}
        await message.answer(f"Школа: {school}\nВведите время урока <code>HH:MM-HH:MM</code> (напр. 09:00-09:40)")
        return

    if st.get("phase") == "add_lesson_time" and uid in ADMIN_IDS:
        m = re.fullmatch(r"\s*(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\s*", txt)
        if not m:
            await message.answer("Время не распознано. Пример: <code>09:00-09:40</code>")
            return
        start_s = f"{m.group(1)}:{m.group(2)}"; end_s = f"{m.group(3)}:{m.group(4)}"
        st["start"] = start_s; st["end"] = end_s
        STATE[uid] = {"phase": "add_lesson_days", "school": st["school"], "start": start_s, "end": end_s}
        await message.answer("В какие дни добавить? (полные названия через запятую, напр.: Понедельник, Среда)")
        return

    if st.get("phase") == "add_lesson_days" and uid in ADMIN_IDS:
        days = parse_days(txt)
        if not days:
            await message.answer("Дни не распознаны. Пример: <code>Понедельник, Среда</code>")
            return
        school = st["school"]; start_s = st["start"]; end_s = st["end"]
        for wd in days:
            SCHEDULE.setdefault(wd, []).append({"start": start_s, "end": end_s, "place": school})
        STATE[uid] = {"phase": "idle"}
        days_ru = ", ".join(day_short(d) for d in days)
        await message.answer(f"✅ Урок {start_s}–{end_s} добавлен в «{school}» (дни: {days_ru})")
        return

    # Пользовательские команды
    if txt == "Назад в меню":
        STATE[uid] = {"phase": "idle"}
        await message.answer("Главное меню:", reply_markup=main_kb(uid))
        return

    if STATE.get(uid, {}).get("phase") == "need_name" and not load_profile(uid):
        if " " not in txt or len(txt) < 3:
            await message.answer("Введите имя и фамилию одним сообщением (например: <i>Азиз Азимов</i>).")
            return
        save_profile(uid, txt, "")
        STATE[uid] = {"phase": "need_contact"}
        await message.answer(
            f"Спасибо, <b>{txt}</b>! Теперь поделитесь <b>контактом</b> кнопкой ниже.",
            reply_markup=ask_contact_kb()
        )
        return

    if txt in ("Показать расписание на сегодня", "/today", "/schedule"):
        await cmd_schedule(message)
        return

    # === НОВЫЙ ФЛОУ ЧЕК-ИНА: СНАЧАЛА ШКОЛА, ПОТОМ ВРЕМЯ ===
    if txt == "Отметиться (выбрать слот)":
        now = datetime.now(TZ); wd = now.weekday()
        today = SCHEDULE.get(wd, [])

        # список школ на сегодня
        schools_today = sorted({s["place"] for s in today})
        # плюс любые free_time места (например SNR School)
        for name, p in PLACES.items():
            if p.get("free_time") and name not in schools_today:
                schools_today.append(name)
        if not schools_today:
            await message.answer("На сегодня нет уроков. Доступно только свободное время, если включено.")
            return

        STATE[uid] = {"phase": "pick_school", "schools_list": schools_today}
        kb = InlineKeyboardMarkup()
        for i, name in enumerate(schools_today[:50]):
            kb.add(InlineKeyboardButton(name, callback_data=f"cs:school:{i}"))
        await message.answer("Выберите школу/садик:", reply_markup=types.ReplyKeyboardRemove())
        await message.answer("⬇️ Нажмите на нужную школу:", reply_markup=kb)
        return

# ====== контакт ======
@dp.message_handler(content_types=["contact"])
async def on_contact(message: types.Message):
    uid = message.from_user.id
    st = STATE.get(uid, {})
    if load_profile(uid):
        await message.answer("Профиль уже закреплён. Изменение недоступно.", reply_markup=main_kb(uid))
        return
    if st.get("phase") != "need_contact":
        return
    if not message.contact or message.contact.user_id != uid:
        await message.answer("Пожалуйста, отправьте свой контакт через кнопку.")
        return
    prof = load_profile(uid) or {"name": message.from_user.full_name}
    save_profile(uid, prof["name"], message.contact.phone_number)
    STATE[uid] = {"phase": "idle"}
    await message.answer(
        f"Профиль сохранён ✅\nИмя: <b>{prof['name']}</b>\nТелефон: <b>{message.contact.phone_number}</b>",
        reply_markup=main_kb(uid)
    )

# ====== КОЛЛБЭКИ: ЧЕК-ИН ПО ШАГАМ ======
@dp.callback_query_handler(lambda c: c.data.startswith("cs:school:"))
async def choose_school(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid, {})
    if st.get("phase") != "pick_school":
        await callback.answer(); return
    idx = int(callback.data.split(":")[2])
    schools = st.get("schools_list", [])
    if idx < 0 or idx >= len(schools):
        await callback.answer("Не найдено"); return
    school = schools[idx]

    now = datetime.now(TZ); wd = now.weekday()
    day = SCHEDULE.get(wd, [])
    free_time = PLACES.get(school, {}).get("free_time", False)

    if free_time:
        # свободное время — сразу к выбору действия
        slot = {"place": school, "start": "00:00", "end": "23:59", "free_time": True}
        STATE[uid] = {"phase": "pick_action", "slot": slot}
        await callback.message.answer(f"Место: <b>{school}</b> — свободное время\nВыберите действие:", reply_markup=actions_kb())
        await callback.answer(); return

    # соберем слоты ТОЛЬКО этой школы на сегодня
    slots = [s for s in day if s.get("place") == school]
    if not slots:
        await callback.message.answer(f"Сегодня в «{school}» нет слотов.")
        STATE[uid] = {"phase": "idle"}
        await callback.answer(); return

    STATE[uid] = {"phase": "pick_time", "school": school, "slots_for_school": slots}
    kb = InlineKeyboardMarkup()
    for i, s in enumerate(slots[:50]):
        kb.add(InlineKeyboardButton(f"{s['start']}–{s['end']}", callback_data=f"cs:time:{i}"))
    await callback.message.answer(f"Школа: <b>{school}</b>\nВыберите время:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("cs:time:"))
async def choose_time(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid, {})
    if st.get("phase") != "pick_time":
        await callback.answer(); return
    idx = int(callback.data.split(":")[2])
    slots = st.get("slots_for_school", [])
    if idx < 0 or idx >= len(slots):
        await callback.answer("Не найдено"); return
    slot = dict(slots[idx])
    slot["free_time"] = PLACES.get(slot["place"], {}).get("free_time", False)
    STATE[uid] = {"phase": "pick_action", "slot": slot}
    await callback.message.answer(
        f"Место: <b>{slot['place']}</b> — {slot['start']}–{slot['end']}\nВыберите действие:",
        reply_markup=actions_kb()
    )
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("act:"))
async def on_action(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid, {})
    if st.get("phase") != "pick_action" or "slot" not in st:
        await callback.answer("Сначала выберите школу и время", show_alert=True); return
    action = callback.data.split(":")[1]  # in/out
    slot = st["slot"]
    STATE[uid] = {"phase": "await_location", "slot": slot, "action": action}
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Отправить геолокацию", request_location=True))
    kb.add(KeyboardButton("Назад в меню"))
    await callback.message.answer(
        f"{'Чек-ин' if action=='in' else 'Чек-аут'} для <b>{slot['place']}</b>\n"
        f"Отправьте геолокацию кнопкой ниже.",
        reply_markup=kb
    )
    await callback.answer()

# ====== ПРИЁМ ЛОКАЦИИ ======
@dp.message_handler(content_types=["location"])
async def on_location(message: types.Message):
    try:
        ensure_files()
        uid = message.from_user.id
        st = STATE.get(uid, {})
        if st.get("phase") != "await_location":
            await message.answer("Сначала выберите школу и время через «Отметиться (выбрать слот)».", reply_markup=main_kb(uid))
            return

        slot = st["slot"]; action = st["action"]
        prof = load_profile(uid) or {"name": message.from_user.full_name, "phone": ""}

        now = datetime.now(TZ)
        date_s = now.strftime("%Y-%m-%d")
        time_s = now.strftime("%H:%M")
        wd_s = weekday_ru(now)

        place_key = slot["place"]
        place = PLACES.get(place_key, {"full": place_key, "lat": None, "lon": None, "radius_m": RADIUS_M_DEFAULT, "free_time": False})
        lat, lon = message.location.latitude, message.location.longitude

        in_radius = None
        dist_m = None
        if place["lat"] is not None and place["lon"] is not None:
            dist_m = haversine_m(lat, lon, place["lat"], place["lon"])
            in_radius = dist_m <= (place.get("radius_m") or RADIUS_M_DEFAULT)

        # Время
        if place.get("free_time"):
            on_time = bool(in_radius)
            timing_line = "⏰ Свободное посещение (без расписания)"
        else:
            if in_radius:
                sh, sm = map(int, slot["start"].split(":"))
                eh, em = map(int, slot["end"].split(":"))
                start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end_dt   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
                if action == "in":
                    on_time = now <= (start_dt + timedelta(minutes=LATE_GRACE_MIN))
                    timing_line = f"⏰ Должен быть к: {slot['start']} (+{LATE_GRACE_MIN} мин)"
                else:
                    on_time = now >= end_dt
                    timing_line = f"⏰ Конец слота: {slot['end']}"
            else:
                on_time = False
                timing_line = f"⏰ Слот: {slot.get('start','')}-{slot.get('end','')}"

        # Ответ
        lines = [
            f"📍 <b>{prof['name']}</b>",
            f"🏫 {place['full']}",
            f"📅 {wd_s}",
            f"⏱ {time_s} {date_s}",
            f"🔄 Действие: {'Чек-ин' if action=='in' else 'Чек-аут'}",
        ]
        if not place.get("free_time"):
            lines.insert(4, f"🕘 Слот: {slot['start']}–{slot['end']}")

        if in_radius is None:
            lines.append("⚠️ Геолокация места не настроена — проверка радиуса пропущена")
            on_time = False
            status_line = "⚠️ Проверка радиуса недоступна"
        elif in_radius is False:
            status_line = f"🚫 Вне радиуса ({pretty_m(dist_m) if dist_m is not None else 'N/A'})"
        else:
            status_line = ("✅ Прибыл" if place.get("free_time") else ("✅ ВО ВРЕМЯ" if on_time else ("⏳ ОПОЗДАЛ❗️" if action == "in" else "⏳ Рано ушёл")))

        lines.append(status_line)
        lines.append(timing_line)
        text = "\n".join(lines)

        await message.answer(text, reply_markup=main_kb(uid), disable_web_page_preview=True)
        for chat_id in ADMIN_CHAT_IDS:
            with suppress(Exception):
                await bot.send_message(chat_id, text, disable_web_page_preview=True)
                await bot.send_location(chat_id, latitude=lat, longitude=lon)

        with CHECKS_CSV.open("a", encoding="utf-8", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow([
                uid, prof["name"], prof.get("phone",""),
                action,
                place_key, place["full"],
                date_s, time_s, wd_s,
                slot.get("start",""), slot.get("end",""),
                f"{lat:.6f}", f"{lon:.6f}",
                f"{round(dist_m,2) if dist_m is not None else ''}",
                (1 if in_radius else 0) if in_radius is not None else "",
                1 if (in_radius is True and on_time) else 0,
                "" if in_radius is None else ("" if in_radius else "Вне радиуса")
            ])

        STATE[uid] = {"phase": "idle"}

    except Exception as e:
        log.exception("Ошибка on_location")
        await message.answer("⚠️ Произошла ошибка при обработке геолокации. Попробуйте ещё раз.")
        await notify_admins(f"❗️ Ошибка on_location: <code>{type(e).__name__}</code> — {e}")

# ====== АДМИН КОЛЛБЭКИ ======
@dp.callback_query_handler(lambda c: c.data.startswith("admin:"))
async def admin_actions(callback: types.CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return

    parts = callback.data.split(":", 2)
    action = parts[1] if len(parts) > 1 else ""

    if action == "add_wizard":
        STATE[callback.from_user.id] = {"phase": "add_place_name"}
        await callback.message.answer("Введите название школы/садика (одно сообщение).")
        await callback.answer(); return

    if action == "add_lesson":
        names = sorted(PLACES.keys())
        if not names:
            await callback.message.answer("Нет школ в базе — сначала добавьте школу.")
            await callback.answer(); return
        kb = InlineKeyboardMarkup()
        for n in names[:50]:
            kb.add(InlineKeyboardButton(n, callback_data=f"admin:choose_school:{n}"))
        await callback.message.answer("Выберите школу, куда добавить урок:", reply_markup=kb)
        await callback.answer(); return

    if action == "list_places":
        lines = ["📍 Список мест:"]
        for name, p in sorted(PLACES.items()):
            tag = " (свободное время)" if p.get("free_time") else ""
            lines.append(f"- {name}{tag} — ({p.get('lat')}, {p.get('lon')}), r={int(p.get('radius_m',RADIUS_M_DEFAULT))}м")
        await callback.message.answer("\n".join(lines))
        await callback.answer(); return

    if action == "del_place":
        names = [n for n,p in sorted(PLACES.items()) if not p.get("free_time")]
        if not names:
            await callback.message.answer("Нет школ для удаления.")
            await callback.answer(); return
        kb = InlineKeyboardMarkup()
        for n in names[:50]:
            kb.add(InlineKeyboardButton(f"🗑 {n}", callback_data=f"admin:del:{n}"))
        await callback.message.answer("Выберите школу/садик для полного удаления:", reply_markup=kb)
        await callback.answer(); return

    if action == "list_today":
        now = datetime.now(TZ); wd = now.weekday()
        day = SCHEDULE.get(wd, [])
        lines = [f"Слоты на сегодня ({weekday_ru(now)}):"]
        if not day: lines.append("— нет —")
        for s in day:
            lines.append(f"• {s['place']}: {s['start']}–{s['end']}")
        await callback.message.answer("\n".join(lines))
        await callback.answer(); return

    # Выбор школы для ДОБАВЛЕНИЯ урока
    if action == "choose_school" and len(parts) == 3:
        school = parts[2]
        STATE[callback.from_user.id] = {"phase": "add_lesson_choose_school"}
        await callback.message.answer(f"Выбрана школа: <b>{school}</b>\nТеперь отправьте ТЕКСТОМ её название ещё раз для подтверждения и дальше время урока.")
        # (ниже текстовый роутер примет название и продолжит визард)
        await callback.answer(); return

    # УДАЛЕНИЕ КОНКРЕТНОГО УРОКА
    if action == "del_lesson":
        names = sorted({s["place"] for wd, lst in SCHEDULE.items() for s in lst})
        if not names:
            await callback.message.answer("Нет уроков в расписании.")
            await callback.answer(); return
        kb = InlineKeyboardMarkup()
        for i, n in enumerate(names[:50]):
            kb.add(InlineKeyboardButton(n, callback_data=f"adl:school:{i}"))
        # сохраним список для индексации
        STATE[callback.from_user.id] = {"phase": "adl_pick_school", "adl_schools": names}
        await callback.message.answer("Выберите школу, из которой нужно удалить урок:", reply_markup=kb)
        await callback.answer(); return

    if action == "del" and len(parts) == 3:
        name = parts[2]
        if name not in PLACES:
            await callback.message.answer(f"❌ Место «{name}» не найдено.")
            await callback.answer(); return
        if PLACES[name].get("free_time"):
            await callback.message.answer("Нельзя удалить SNR School.")
            await callback.answer(); return
        del PLACES[name]
        for wd in list(SCHEDULE.keys()):
            SCHEDULE[wd] = [s for s in SCHEDULE.get(wd, []) if s.get("place") != name]
        await callback.message.answer(f"✅ «{name}» удалён полностью (включая уроки).")
        await callback.answer("Удалено"); return

    await callback.answer()

# Коллбэки для удаления УРОКА
@dp.callback_query_handler(lambda c: c.data.startswith("adl:school:"))
async def adl_choose_school(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid, {})
    if st.get("phase") != "adl_pick_school":
        await callback.answer(); return
    idx = int(callback.data.split(":")[2])
    schools = st.get("adl_schools", [])
    if idx < 0 or idx >= len(schools):
        await callback.answer("Не найдено"); return
    school = schools[idx]

    # соберём все уроки этой школы по всем дням
    candidates = []
    for wd in range(7):
        for slot in SCHEDULE.get(wd, []):
            if slot.get("place") == school:
                candidates.append({"wd": wd, "start": slot["start"], "end": slot["end"], "place": school})

    if not candidates:
        await callback.message.answer(f"В «{school}» нет уроков.")
        STATE[uid] = {"phase": "idle"}
        await callback.answer(); return

    STATE[uid] = {"phase": "adl_pick_lesson", "adl_candidates": candidates}
    kb = InlineKeyboardMarkup()
    for i, c in enumerate(candidates[:50]):
        kb.add(InlineKeyboardButton(f"{day_short(c['wd'])} {c['start']}-{c['end']}", callback_data=f"adl:pick:{i}"))
    await callback.message.answer(f"Выбрано: {school}\nВыберите урок для удаления:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("adl:pick:"))
async def adl_do_delete(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid, {})
    if st.get("phase") != "adl_pick_lesson":
        await callback.answer(); return
    idx = int(callback.data.split(":")[2])
    cand = st.get("adl_candidates", [])
    if idx < 0 or idx >= len(cand):
        await callback.answer("Не найдено"); return
    item = cand[idx]

    wd = item["wd"]; start = item["start"]; end = item["end"]; place = item["place"]
    before = len(SCHEDULE.get(wd, []))
    SCHEDULE[wd] = [s for s in SCHEDULE.get(wd, []) if not (s["place"] == place and s["start"] == start and s["end"] == end)]
    after = len(SCHEDULE.get(wd, []))

    if after < before:
        await callback.message.answer(f"✅ Удалён урок «{place}» {day_short(wd)} {start}-{end}.")
    else:
        await callback.message.answer("❌ Не удалось найти урок для удаления (возможно, уже удалён).")
    STATE[uid] = {"phase": "idle"}
    await callback.answer("Готово")

# ====== LATE CHECKER ======
async def late_watcher():
    await asyncio.sleep(3)
    while True:
        try:
            ensure_files()
            now = datetime.now(TZ)
            date_s = now.strftime("%Y-%m-%d")
            wd = now.weekday()
            day_slots = SCHEDULE.get(wd, [])
            if not day_slots:
                await asyncio.sleep(60); continue

            rows = safe_dict_reader(CHECKS_CSV)

            for slot in day_slots:
                place = slot["place"]
                if PLACES.get(place, {}).get("free_time"):
                    continue

                sh, sm = map(int, slot["start"].split(":"))
                start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                penalty_time = start_dt + timedelta(minutes=LATE_GRACE_MIN)

                if now < penalty_time:
                    continue

                slot_key = (date_s, wd, place, slot["start"])
                if slot_key in LATE_SENT_SLOTS:
                    continue

                had_any_valid_in = False
                for r in rows:
                    r_place = r.get("place_full") or r.get("place_key")
                    if r_place != place: continue
                    if r.get("date") != date_s: continue
                    if r.get("action") != "in" or r.get("in_radius") != "1": continue
                    try:
                        rt_naive = datetime.strptime(f"{r.get('date')} {r.get('time')}", "%Y-%m-%d %H:%M")
                        rt = rt_naive.replace(tzinfo=TZ)
                    except Exception:
                        continue
                    if (start_dt - timedelta(minutes=30)) <= rt <= penalty_time:
                        had_any_valid_in = True
                        break

                if not had_any_valid_in:
                    msg = (f"⚠️ Нет чек-ина к {slot['start']} в «{place}» "
                           f"({weekday_ru(now)}, {date_s}). Грейс: {LATE_GRACE_MIN} мин.")
                    await notify_admins(msg)
                    LATE_SENT_SLOTS.add(slot_key)

        except Exception as e:
            log.exception("late_watcher error: %s", e)
        finally:
            await asyncio.sleep(60)

# ====== ГЛОБАЛЬНЫЙ ХУК ИСКЛЮЧЕНИЙ ======
@dp.errors_handler()
async def global_errors(update, error):
    if isinstance(error, Throttled):
        return True
    log.exception("Unhandled error: %r", error)
    with suppress(Exception):
        await notify_admins(f"❗️ Unhandled error: <code>{type(error).__name__}</code> — {error}")
    return True

# ====== ЗАПУСК ======
if __name__ == "__main__":
    ensure_files()
    load_profiles_cache()
    load_places_runtime()
    log.info("Starting polling...")
    loop = asyncio.get_event_loop()
    loop.create_task(late_watcher())
    executor.start_polling(dp, skip_updates=True)
