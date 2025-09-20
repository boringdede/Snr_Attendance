# bot.py — aiogram v2.25.1
# Фичи: SNR School (свободное место), добавление садиков по дням, удаление садиков с подтверждением,
# "во время" только в радиусе (чек-ин допускается на LATE_GRACE_MIN минут позже),
# авто-оповещение об отсутствии чек-ина к start+грейс (в админ-чат, без привязки к учителям).

import csv
import logging
import re
import asyncio
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from pathlib import Path

from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.exceptions import Throttled
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton
)

# ==== НАСТРОЙКИ ====
import os
API_TOKEN = os.getenv(8278332572:AAEraxNTF4-01luv6A0mwkqv7zL-zBRKag0)   # ← вот так ДОЛЖНО быть
ADMIN_IDS = {2062714005}
ADMIN_IDS = {1790286972}
ADMIN_CHAT_IDS = {-1002362042916}

RADIUS_M_DEFAULT = 200.0
CITY_TZ_HOURS = 5         # Asia/Tashkent UTC+5
LATE_GRACE_MIN = 10       # грейс к началу слота (минуты) для "во время" и штрафов

# ---- ЛОГИ ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("snr-checkin-bot")

# Часовой пояс (с фолбэком)
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

# ====== МЕСТА (база по умолчанию) + SNR ======
BASE_PLACES = {
    "SNR School": {"full": "SNR School (офис)", "lat": 41.322921, "lon": 69.277808, "radius_m": 200.0, "free_time": True},
    "559 гос": {"full": "559 государственный садик", "lat": 41.303288, "lon": 69.292031, "radius_m": 200.0, "free_time": False},
    "First kids": {"full": "First kids", "lat": 41.329848, "lon": 69.286872, "radius_m": 200.0, "free_time": False},
    "FIRST":      {"full": "First kids", "lat": 41.329848, "lon": 69.286872, "radius_m": 200.0, "free_time": False},
    "Domik": {"full": "ДОМИК", "lat": 41.321701, "lon": 69.315380, "radius_m": 200.0, "free_time": False},
    "Small steps": {"full": "Small steps", "lat": 41.294155, "lon": 69.189863, "radius_m": 200.0, "free_time": False},
    "STARKIDS": {"full": "STARKIDS", "lat": 41.298992, "lon": 69.260579, "radius_m": 200.0, "free_time": False},
    "Академия Талантов": {"full": "Академия Талантов", "lat": 41.313393, "lon": 69.294289, "radius_m": 200, "free_time": False},
    "324 гос": {"full": "324 государственный садик", "lat": 41.335171, "lon": 69.335863, "radius_m": 200, "free_time": False},
}
PLACES = {}  # runtime: имя -> dict

# ====== РАСПИСАНИЕ ======
# Пн=0..Вс=6; SNR School — free_time (всегда доступен, без слотов)
SCHEDULE = {
    0: [ {"start": "09:00", "end": "12:30", "place": "559 гос"},
         {"start": "15:45", "end": "16:30", "place": "559 гос"},
         {"start": "10:00", "end": "11:30", "place": "First kids"} ],
    1: [ {"start": "10:30", "end": "11:00", "place": "ДОМИК"},
         {"start": "15:00", "end": "16:00", "place": "324 гос"} ],
    2: [ {"start": "09:00", "end": "12:30", "place": "559 гос"},
         {"start": "15:45", "end": "16:30", "place": "559 гос"},
         {"start": "10:00", "end": "11:00", "place": "Small steps"},
         {"start": "10:30", "end": "16:30", "place": "Академия Талантов"} ],
    3: [ {"start": "10:30", "end": "11:00", "place": "ДОМИК"} ],
    4: [ {"start": "09:30", "end": "12:30", "place": "STARKIDS"},
         {"start": "15:00", "end": "16:00", "place": "324 гос"} ],
    5: [],
    6: [],
}

# ====== БОТ ======
bot = Bot(token=API_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# runtime
STATE = {}                 # user_id -> {...}
PROFILES = {}              # кэш
LATE_SENT_SLOTS = set()    # {(date, wd, place, start)} — уже уведомлённые слоты

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

def slots_kb(day_slots):
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🏢 SNR School — свободное время", callback_data="slot_snr"))
    for i, s in enumerate(day_slots):
        kb.add(InlineKeyboardButton(f"{s['place']} — {s['start']}–{s['end']}", callback_data=f"slot:{i}"))
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

# ====== ПАРСИНГ ДНЕЙ НЕДЕЛИ ======
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

# ====== вспомогательные команды ======
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
    kb.add(InlineKeyboardButton("➕ Добавить садик (пошагово)", callback_data="admin:add_wizard"))
    kb.add(InlineKeyboardButton("📋 Список садиков", callback_data="admin:list_places"))
    kb.add(InlineKeyboardButton("🗑 Удалить садик", callback_data="admin:del_place"))
    kb.add(InlineKeyboardButton("📆 Слоты на сегодня", callback_data="admin:list_today"))
    await message.answer("🔧 Панель администратора:", reply_markup=kb)

# ====== ТЕКСТ-РОУТЕР ======
@dp.message_handler(content_types=["text"])
async def text_router(message: types.Message):
    uid = message.from_user.id
    txt = (message.text or "").strip()
    st = STATE.get(uid, {})

    # перехват /admin
    if txt.startswith("/admin"):
        await admin_menu(message); return

    # Визард добавления: шаг 1 — имя
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

    # шаг 2 — координаты
    if st.get("phase") == "add_place_coords" and uid in ADMIN_IDS:
        m = re.fullmatch(r"\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*", txt)
        if not m:
            await message.answer("Координаты не распознаны. Пример: <code>41.300000, 69.300000</code>")
            return
        lat = float(m.group(1)); lon = float(m.group(2))
        st["new_place"]["lat"] = lat; st["new_place"]["lon"] = lon
        STATE[uid] = {"phase": "add_place_times", "new_place": st["new_place"]}
        await message.answer(
            "Время слота: <code>HH:MM-HH:MM</code>\nНапример: <code>09:00-12:30</code>"
        )
        return

    # шаг 3 — время
    if st.get("phase") == "add_place_times" and uid in ADMIN_IDS:
        m = re.fullmatch(r"\s*(\d{2}):(\d{2})\s*-\s*(\d{2}):(\d{2})\s*", txt)
        if not m:
            await message.answer("Время не распознано. Пример: <code>09:00-12:30</code>")
            return
        start_s = f"{m.group(1)}:{m.group(2)}"
        end_s   = f"{m.group(3)}:{m.group(4)}"
        st["new_place"]["start"] = start_s
        st["new_place"]["end"] = end_s
        STATE[uid] = {"phase": "add_place_days", "new_place": st["new_place"]}
        await message.answer(
            "Дни недели (через запятую):\n"
            "- числа 1-7 или 0-6 (Пн=1/0 ... Вс=7/6),\n"
            "- или названия: пн, вт, ср, чт, пт, сб, вс,\n"
            "- или «все».\n"
            "Например: <code>пн, ср, пт</code> или <code>1,3,5</code>"
        )
        return

    # шаг 4 — дни
    if st.get("phase") == "add_place_days" and uid in ADMIN_IDS:
        days = parse_days(txt)
        if not days:
            await message.answer("Дни не распознаны. Пример: <code>пн, ср, пт</code> или <code>1,3,5</code>")
            return
        np = st["new_place"]
        name, lat, lon = np["name"], np["lat"], np["lon"]
        start_s, end_s = np["start"], np["end"]

        # 1) добавляем место
        PLACES[name] = {"full": name, "lat": lat, "lon": lon, "radius_m": RADIUS_M_DEFAULT, "free_time": False}
        # 2) добавляем слоты только в выбранные дни
        for wd in days:
            SCHEDULE.setdefault(wd, []).append({"start": start_s, "end": end_s, "place": name})

        STATE[uid] = {"phase": "idle"}
        days_ru = ", ".join(["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][d] for d in days)
        await message.answer(
            f"✅ Садик «{name}» добавлен.\nКоординаты: {lat}, {lon}\nВремя: {start_s}–{end_s}\nДни: {days_ru}"
        )
        return

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

    if txt == "Отметиться (выбрать слот)":
        now = datetime.now(TZ); wd = now.weekday()
        day = SCHEDULE.get(wd, [])
        STATE[uid] = {"phase": "pick_slot"}
        await message.answer("Выберите место/слот:", reply_markup=types.ReplyKeyboardRemove())
        await message.answer("⬇️ Нажмите на нужный:", reply_markup=slots_kb(day))
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

# ====== выбор слота ======
@dp.callback_query_handler(lambda c: c.data.startswith("slot"))
async def on_slot(callback: types.CallbackQuery):
    uid = callback.from_user.id
    now = datetime.now(TZ); wd = now.weekday()
    day = SCHEDULE.get(wd, [])

    if callback.data == "slot_snr":
        slot = {"place": "SNR School", "start": "00:00", "end": "23:59", "free_time": True}
    else:
        try:
            idx = int(callback.data.split(":")[1])
            slot = dict(day[idx])
            slot["free_time"] = PLACES.get(slot["place"], {}).get("free_time", False)
        except Exception:
            await callback.answer("Слот не найден", show_alert=True); return

    STATE[uid] = {"phase": "pick_action", "slot": slot}
    label = "свободное время" if slot.get("free_time") else f"{slot['start']}–{slot['end']}"
    await callback.message.answer(
        f"Место: <b>{slot['place']}</b> — {label}\nВыберите действие:",
        reply_markup=actions_kb()
    )
    await callback.answer()

# ====== выбор действия ======
@dp.callback_query_handler(lambda c: c.data.startswith("act:"))
async def on_action(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid, {})
    if st.get("phase") != "pick_action" or "slot" not in st:
        await callback.answer("Сначала выберите слот", show_alert=True); return
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

# ====== приём локации ======
@dp.message_handler(content_types=["location"])
async def on_location(message: types.Message):
    try:
        ensure_files()
        uid = message.from_user.id
        st = STATE.get(uid, {})
        if st.get("phase") != "await_location":
            await message.answer("Сначала выберите слот: «Отметиться (выбрать слот)».", reply_markup=main_kb(uid))
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
            on_time = bool(in_radius)  # SNR: «во время» == в радиусе
            timing_line = "⏰ Свободное посещение (без расписания)"
        else:
            if in_radius:
                sh, sm = map(int, slot["start"].split(":"))
                eh, em = map(int, slot["end"].split(":"))
                start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                end_dt   = now.replace(hour=eh, minute=em, second=0, microsecond=0)
                if action == "in":
                    # учитываем грейс (10 минут по настройке)
                    on_time = now <= (start_dt + timedelta(minutes=LATE_GRACE_MIN))
                    timing_line = f"⏰ Должен быть к: {slot['start']} (+{LATE_GRACE_MIN} мин)"
                else:
                    on_time = now >= end_dt
                    timing_line = f"⏰ Конец слота: {slot['end']}"
            else:
                on_time = False
                timing_line = f"⏰ Слот: {slot.get('start','')}-{slot.get('end','')}"

        # Текст ответа
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
            status_line = f"🚫 Вы ещё не прибыли (вне радиуса, {pretty_m(dist_m) if dist_m is not None else 'расстояние неизвестно'})"
        else:
            if place.get("free_time"):
                status_line = "✅ Прибыл" if on_time else "⛔ Не засчитано"
            else:
                status_line = "✅ ВО ВРЕМЯ" if on_time else ("⏳ ОПОЗДАЛ❗️" if action == "in" else "⏳ Рано ушёл")

        lines.append(status_line)
        lines.append(timing_line)

        text = "\n".join(lines)

        await message.answer(text, reply_markup=main_kb(uid), disable_web_page_preview=True)
        for chat_id in ADMIN_CHAT_IDS:
            with suppress(Exception):
                await bot.send_message(chat_id, text, disable_web_page_preview=True)
                await bot.send_location(chat_id, latitude=lat, longitude=lon)

        # Запись в CSV
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

    parts = callback.data.split(":", 2)  # важно: максимум 3 части
    # форматы:
    # - "admin:add_wizard"
    # - "admin:list_places"
    # - "admin:del_place"
    # - "admin:del:<Название>"
    action = parts[1] if len(parts) > 1 else ""

    if action == "add_wizard":
        STATE[callback.from_user.id] = {"phase": "add_place_name"}
        await callback.message.answer("Введите название садика (одно сообщение).")
        await callback.answer()
        return

    if action == "list_places":
        lines = ["📍 Список мест:"]
        for name, p in sorted(PLACES.items()):
            tag = " (свободное время)" if p.get("free_time") else ""
            lines.append(f"- {name}{tag} — ({p.get('lat')}, {p.get('lon')}), r={int(p.get('radius_m',RADIUS_M_DEFAULT))}м")
        await callback.message.answer("\n".join(lines))
        await callback.answer()
        return

    if action == "del_place":
        names = [n for n,p in sorted(PLACES.items()) if not p.get("free_time")]
        if not names:
            await callback.message.answer("Нет садиков для удаления.")
            await callback.answer()
            return
        kb = InlineKeyboardMarkup()
        for n in names[:50]:
            kb.add(InlineKeyboardButton(f"🗑 {n}", callback_data=f"admin:del:{n}"))
        await callback.message.answer("Выберите садик для удаления:", reply_markup=kb)
        await callback.answer()
        return

    # обработка конкретного удаления "admin:del:<Название>"
    if action == "del" and len(parts) == 3:
        name = parts[2]
        if name not in PLACES:
            await callback.message.answer(f"❌ Место «{name}» не найдено.")
            await callback.answer()
            return
        if PLACES[name].get("free_time"):
            await callback.message.answer("Нельзя удалить SNR School.")
            await callback.answer()
            return

        # удаляем из базы мест
        del PLACES[name]
        # удаляем из всех дней расписания
        for wd in list(SCHEDULE.keys()):
            SCHEDULE[wd] = [s for s in SCHEDULE.get(wd, []) if s.get("place") != name]

        # явное подтверждение
        await callback.message.answer(f"✅ Садик «{name}» успешно удалён из базы и расписания.")
        await callback.answer("Удалено")
        return

    if action == "list_today":
        now = datetime.now(TZ); wd = now.weekday()
        day = SCHEDULE.get(wd, [])
        lines = [f"Слоты на сегодня ({weekday_ru(now)}):"]
        if not day: lines.append("— нет —")
        for s in day:
            lines.append(f"• {s['place']}: {s['start']}–{s['end']}")
        await callback.message.answer("\n".join(lines))
        await callback.answer()
        return

    await callback.answer()

# ====== LATE CHECKER (штрафы без привязки к учителям) ======
async def late_watcher():
    """
    Ежеминутно проверяем слоты текущего дня:
    если к start + LATE_GRACE_MIN НИКТО не сделал чек-ин в радиусе — шлём 1 сообщение в админ-чат.
    """
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
                    continue  # SNR и подобные не штрафуем

                # время слота
                sh, sm = map(int, slot["start"].split(":"))
                start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                penalty_time = start_dt + timedelta(minutes=LATE_GRACE_MIN)

                # ещё не время проверки
                if now < penalty_time:
                    continue

                slot_key = (date_s, wd, place, slot["start"])
                if slot_key in LATE_SENT_SLOTS:
                    continue  # уже уведомляли про этот слот сегодня

                # Был ли ХОТЬ ОДИН корректный чек-ин?
                had_any_valid_in = False
                for r in rows:
                    r_place = r.get("place_full") or r.get("place_key")
                    if r_place != place:
                        continue
                    if r.get("date") != date_s:
                        continue
                    if r.get("action") != "in" or r.get("in_radius") != "1":
                        continue

                    # время записи CSV -> локальное aware-время (TZ)
                    try:
                        rt_naive = datetime.strptime(f"{r.get('date')} {r.get('time')}", "%Y-%m-%d %H:%M")
                        rt = rt_naive.replace(tzinfo=TZ)
                    except Exception:
                        continue

                    # засчитываем, если чек-ин в интервале [start-30мин ; penalty_time]
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
    try:
        ensure_files()
        load_profiles_cache()
        load_places_runtime()
        log.info("Starting polling...")
        loop = asyncio.get_event_loop()
        loop.create_task(late_watcher())
        executor.start_polling(dp, skip_updates=True)
    except Exception as e:
        import traceback
        print("⚠️ Критическая ошибка при старте:", type(e).__name__, e)
        traceback.print_exc()
        input("\nНажмите Enter, чтобы закрыть окно...")





