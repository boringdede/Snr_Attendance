# bot.py — aiogram 2.25.1, SQLite + CSV
# pip install aiogram==2.25.1

import csv
import logging
import asyncio
import json
from contextlib import suppress
from datetime import datetime, timezone, timedelta
from pathlib import Path
import os
import sqlite3

from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.exceptions import Throttled
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove
)

# ============ НАСТРОЙКИ ============
HARDCODED_FALLBACK_TOKEN = "8278332572:AAHzttbejyLSNZLRbION8v0NtGKlnFwJ2Tg"
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or HARDCODED_FALLBACK_TOKEN
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

ADMIN_IDS = {1790286972, 2062714005}
ADMIN_CHAT_IDS = {-1002362042916}

RADIUS_M_DEFAULT = 200.0
CITY_TZ_HOURS = 5
LATE_GRACE_MIN = 10

# — закреплённая точка для отметки 24/7 —
ALWAYS_PLACE_KEY = "SNR School"
ALWAYS_PLACE_FULL = "SNR School (офис)"
ALWAYS_PLACE_LAT = 41.322921
ALWAYS_PLACE_LON = 69.277808

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
DB_PATH = DATA_DIR / "data.db"
CHECKS_CSV = DATA_DIR / "checks.csv"
PROFILES_CSV = DATA_DIR / "profiles.csv"
PLACES_JSON = DATA_DIR / "places.json"
SCHEDULE_JSON = DATA_DIR / "schedule.json"

# ====== РАНТАЙМ ======
STATE = {}          # простые визарды
LATE_SENT_SLOTS = set()

# ====== БОТ ======
bot = Bot(token=API_TOKEN, parse_mode="HTML")
dp = Dispatcher(bot)

# ====== УТИЛИТЫ ======
def weekday_ru(dt: datetime) -> str:
    return ["Понедельник","Вторник","Среда","Четверг","Пятница","Суббота","Воскресенье"][dt.weekday()]

def day_short(wd: int) -> str:
    return ["Пн","Вт","Ср","Чт","Пт","Сб","Вс"][wd]

def haversine_m(lat1, lon1, lat2, lon2) -> float:
    from math import radians, sin, cos, atan2, sqrt
    R = 6371000.0
    p1, p2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlmb = radians(lon2 - lon1)
    a = sin(dphi/2)**2 + cos(p1)*cos(p2)*sin(dlmb/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))
    return R * c

def ask_contact_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Поделиться контактом", request_contact=True))
    kb.add(KeyboardButton("Назад в меню"))
    return kb

def main_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📅 Расписание на сегодня"))
    kb.add(KeyboardButton("✅ Отметиться"))
    kb.add(KeyboardButton("Назад в меню"))
    return kb

def actions_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Чек-ин", callback_data="act:in"))
    kb.add(InlineKeyboardButton("Чек-аут", callback_data="act:out"))
    return kb

async def notify_admins(text: str):
    for chat_id in ADMIN_CHAT_IDS:
        with suppress(Exception):
            await bot.send_message(chat_id, text, disable_web_page_preview=True)

def pretty_m(m) -> str:
    try:
        return f"{int(round(float(m)))} м"
    except Exception:
        return "-"

# ====== CSV (старая табличка) ======
def ensure_csv_files():
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

def write_check_to_csv(row_dict: dict):
    with CHECKS_CSV.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow([
            row_dict.get("telegram_id"),
            row_dict.get("teacher_name"),
            row_dict.get("phone",""),
            row_dict.get("action"),
            row_dict.get("place_key"),
            row_dict.get("place_full"),
            row_dict.get("date"),
            row_dict.get("time"),
            row_dict.get("weekday"),
            row_dict.get("slot_start"),
            row_dict.get("slot_end"),
            row_dict.get("lat"),
            row_dict.get("lon"),
            row_dict.get("distance_m"),
            1 if row_dict.get("in_radius") else 0,
            1 if row_dict.get("on_time") else 0,
            row_dict.get("notes","")
        ])

def write_profile_to_csv(uid:int, name:str, phone:str):
    rows = {}
    if PROFILES_CSV.exists():
        with PROFILES_CSV.open("r", encoding="utf-8", newline="") as f:
            r = csv.DictReader(f, delimiter=";")
            for row in r:
                try:
                    rows[int(row["telegram_id"])] = (row.get("teacher_name",""), row.get("phone",""))
                except Exception:
                    continue
    rows[uid] = (name, phone or "")
    with PROFILES_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["telegram_id","teacher_name","phone"])
        for k,(n,p) in rows.items():
            w.writerow([k,n,p])

# ====== БАЗА ДАННЫХ ======
def db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS profiles (
        telegram_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        phone TEXT DEFAULT ''
    )""")

    cur.execute(f"""
    CREATE TABLE IF NOT EXISTS places (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        key TEXT UNIQUE NOT NULL,
        full TEXT NOT NULL,
        lat REAL,
        lon REAL,
        radius_m REAL DEFAULT {RADIUS_M_DEFAULT}
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        weekday INTEGER NOT NULL,
        start TEXT NOT NULL,
        end TEXT NOT NULL,
        place_key TEXT NOT NULL,
        FOREIGN KEY(place_key) REFERENCES places(key)
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS checks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_id INTEGER NOT NULL,
        teacher_name TEXT NOT NULL,
        phone TEXT,
        action TEXT NOT NULL,
        place_key TEXT NOT NULL,
        place_full TEXT NOT NULL,
        date TEXT NOT NULL,
        time TEXT NOT NULL,
        weekday TEXT NOT NULL,
        slot_start TEXT NOT NULL,
        slot_end TEXT NOT NULL,
        lat REAL,
        lon REAL,
        distance_m REAL,
        in_radius INTEGER,
        on_time INTEGER,
        notes TEXT
    )""")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS stopped (
        telegram_id INTEGER PRIMARY KEY
    )""")

    conn.commit()
    conn.close()

def import_legacy():
    conn = db(); cur = conn.cursor()
    # places
    cur.execute("SELECT COUNT(*) AS c FROM places")
    if cur.fetchone()["c"] == 0 and PLACES_JSON.exists():
        try:
            places = json.loads(PLACES_JSON.read_text(encoding="utf-8"))
            for k, v in places.items():
                cur.execute("INSERT OR IGNORE INTO places(key, full, lat, lon, radius_m) VALUES(?,?,?,?,?)",
                            (k, v.get("full", k), v.get("lat"), v.get("lon"), v.get("radius_m", RADIUS_M_DEFAULT)))
        except Exception:
            pass
    # schedule
    cur.execute("SELECT COUNT(*) AS c FROM schedule")
    if cur.fetchone()["c"] == 0 and SCHEDULE_JSON.exists():
        try:
            sched = json.loads(SCHEDULE_JSON.read_text(encoding="utf-8"))
            for wd_str, slots in sched.items():
                wd = int(wd_str)
                for s in slots:
                    cur.execute("INSERT INTO schedule(weekday,start,end,place_key) VALUES(?,?,?,?)",
                                (wd, s["start"], s["end"], s["place"]))
        except Exception:
            pass
    # profiles
    if PROFILES_CSV.exists():
        try:
            with PROFILES_CSV.open("r", encoding="utf-8", newline="") as f:
                r = csv.DictReader(f, delimiter=";")
                for row in r:
                    try:
                        tid = int(row["telegram_id"])
                        cur.execute("INSERT OR IGNORE INTO profiles(telegram_id,name,phone) VALUES(?,?,?)",
                                    (tid, row.get("teacher_name",""), row.get("phone","")))
                    except Exception:
                        continue
        except Exception:
            pass
    conn.commit(); conn.close()

def ensure_always_place():
    """Гарантируем, что SNR School есть в БД и корректен."""
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT key FROM places WHERE key=?", (ALWAYS_PLACE_KEY,))
    row = cur.fetchone()
    if not row:
        cur.execute("INSERT INTO places(key,full,lat,lon,radius_m) VALUES(?,?,?,?,?)",
                    (ALWAYS_PLACE_KEY, ALWAYS_PLACE_FULL, ALWAYS_PLACE_LAT, ALWAYS_PLACE_LON, RADIUS_M_DEFAULT))
    conn.commit(); conn.close()

def get_profile(uid:int):
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT name, phone FROM profiles WHERE telegram_id=?", (uid,))
    row = cur.fetchone(); conn.close()
    if row: return {"name": row["name"], "phone": row["phone"]}
    return None

def save_profile(uid:int, name:str, phone:str):
    conn = db(); cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO profiles(telegram_id,name,phone) VALUES(?,?,?)", (uid,name,phone or ""))
    conn.commit(); conn.close()
    write_profile_to_csv(uid, name, phone or "")

def list_places():
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT key, full, lat, lon, radius_m FROM places ORDER BY key COLLATE NOCASE")
    rows = cur.fetchall(); conn.close()
    return rows

def add_place(key:str, full:str, lat:float, lon:float, radius:float):
    conn = db(); cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO places(key,full,lat,lon,radius_m) VALUES(?,?,?,?,?)",
        (key, full, lat, lon, radius))
    conn.commit(); conn.close()

def delete_place(key:str):
    conn = db(); cur = conn.cursor()
    cur.execute("DELETE FROM schedule WHERE place_key=?", (key,))
    cur.execute("DELETE FROM places WHERE key=?", (key,))
    conn.commit(); conn.close()

def today_slots():
    wd = datetime.now(TZ).weekday()
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT start,end,place_key FROM schedule WHERE weekday=? ORDER BY start", (wd,))
    rows = cur.fetchall(); conn.close()
    return rows

def list_schedule(wd:int):
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT start,end,place_key FROM schedule WHERE weekday=? ORDER BY start", (wd,))
    rows = cur.fetchall(); conn.close()
    return rows

def add_lesson(wd:int, place_key:str, start:str, end:str):
    conn = db(); cur = conn.cursor()
    cur.execute("INSERT INTO schedule(weekday,start,end,place_key) VALUES(?,?,?,?)",
                (wd, start, end, place_key))
    conn.commit(); conn.close()

def delete_lesson(wd:int, place_key:str, start:str):
    conn = db(); cur = conn.cursor()
    cur.execute("DELETE FROM schedule WHERE weekday=? AND place_key=? AND start=?", (wd, place_key, start))
    conn.commit(); conn.close()

def is_stopped(uid:int)->bool:
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT 1 FROM stopped WHERE telegram_id=?", (uid,))
    res = cur.fetchone()
    conn.close()
    return bool(res)

def set_stopped(uid:int, stop:bool):
    conn = db(); cur = conn.cursor()
    if stop:
        cur.execute("INSERT OR IGNORE INTO stopped(telegram_id) VALUES(?)", (uid,))
    else:
        cur.execute("DELETE FROM stopped WHERE telegram_id=?", (uid,))
    conn.commit(); conn.close()

# ====== СТАРТ / СТОП ======
@dp.message_handler(commands=["stop"])
async def cmd_stop(message: types.Message):
    uid = message.from_user.id
    set_stopped(uid, True)
    await message.answer("🛑 Бот остановлен. Для возобновления отправьте /start", reply_markup=ReplyKeyboardRemove())

@dp.message_handler(lambda m: is_stopped(m.from_user.id) and not (m.text or "").startswith("/start"), content_types=types.ContentTypes.ANY)
async def guard_stopped_messages(message: types.Message):
    return

@dp.callback_query_handler(lambda c: is_stopped(c.from_user.id))
async def guard_stopped_callbacks(callback: types.CallbackQuery):
    with suppress(Exception):
        await callback.answer()
    return

# ====== /start ======
@dp.message_handler(commands=["start","help"])
async def on_start(message: types.Message):
    ensure_csv_files()
    init_db()
    import_legacy()
    ensure_always_place()  # <- гарантируем, что SNR присутствует

    uid = message.from_user.id
    set_stopped(uid, False)

    prof = get_profile(uid)
    if not prof:
        STATE[uid] = {"phase": "need_name"}
        await message.answer("👋 Привет! Введите, пожалуйста, <b>Имя и Фамилию</b> (например: <i>Азиз Азимов</i>).",
                             reply_markup=ReplyKeyboardRemove())
        return
    STATE[uid] = {"phase": "idle"}
    await message.answer(f"Привет, <b>{prof['name']}</b>! Что делаем?", reply_markup=main_kb())

# ====== ПРОФИЛЬ ======
@dp.message_handler(commands=["id"])
async def cmd_id(message: types.Message):
    await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

@dp.message_handler(commands=["my"])
async def cmd_my(message: types.Message):
    uid = message.from_user.id
    prof = get_profile(uid) or {"name": message.from_user.full_name, "phone": ""}
    await message.answer(f"👤 Профиль\nИмя: <b>{prof['name']}</b>\nТелефон: <b>{prof.get('phone','')}</b>")

@dp.message_handler(commands=["schedule","today"])
async def cmd_schedule(message: types.Message):
    now = datetime.now(TZ); wd = now.weekday()
    rows = list_schedule(wd)
    lines = [f"Расписание на <b>{weekday_ru(now)}</b>:"]
    if not rows:
        lines.append("• (слотов нет)")
    else:
        for r in rows:
            lines.append(f"• {r['place_key']}: {r['start']}–{r['end']}")
    await message.answer("\n".join(lines))

# ====== АДМИН ======
def admin_menu_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("➕ Добавить школу", callback_data="admin:add_place"))
    kb.add(InlineKeyboardButton("📋 Список школ", callback_data="admin:list_places"))
    kb.add(InlineKeyboardButton("🗑 Удалить школу", callback_data="admin:del_place"))
    kb.add(InlineKeyboardButton("➕ Добавить урок", callback_data="admin:add_lesson"))
    kb.add(InlineKeyboardButton("🗑 Удалить урок", callback_data="admin:del_lesson"))
    return kb

@dp.message_handler(commands=["admin"])
async def admin_menu(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет доступа."); return
    await message.answer("🔧 Панель администратора:", reply_markup=admin_menu_kb())

# ====== ВИЗАРДЫ ДОБАВЛЕНИЯ ШКОЛ/УРОКОВ ======
@dp.callback_query_handler(lambda c: c.data == "admin:add_place")
async def cb_add_place(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True); return
    STATE[uid] = {"phase":"add_place_name"}
    await callback.message.answer("Введите <b>название школы</b> (ключ):")
    await callback.answer()

@dp.message_handler(lambda m: STATE.get(m.from_user.id,{}).get("phase")=="add_place_name")
async def add_place_name(message: types.Message):
    uid = message.from_user.id
    key = (message.text or "").strip()
    if not key:
        await message.answer("Пусто. Введите название школы:"); return
    STATE[uid] = {"phase":"add_place_lat", "key":key, "full":key}
    await message.answer("Введите <b>широту</b> (например 41.322921):")

@dp.message_handler(lambda m: STATE.get(m.from_user.id,{}).get("phase")=="add_place_lat")
async def add_place_lat(message: types.Message):
    uid = message.from_user.id
    try:
        lat = float((message.text or "").replace(",", "."))
    except Exception:
        await message.answer("Неверный формат. Введите широту числом:"); return
    st = STATE[uid]; st["lat"]=lat; st["phase"]="add_place_lon"
    await message.answer("Введите <b>долготу</b> (например 69.277808):")

@dp.message_handler(lambda m: STATE.get(m.from_user.id,{}).get("phase")=="add_place_lon")
async def add_place_lon(message: types.Message):
    uid = message.from_user.id
    try:
        lon = float((message.text or "").replace(",", "."))
    except Exception:
        await message.answer("Неверный формат. Введите долготу числом:"); return
    st = STATE[uid]; st["lon"]=lon; st["phase"]="add_place_radius"
    await message.answer(f"Введите <b>радиус</b> в метрах (по умолчанию {int(RADIUS_M_DEFAULT)}):")

@dp.message_handler(lambda m: STATE.get(m.from_user.id,{}).get("phase")=="add_place_radius")
async def add_place_radius(message: types.Message):
    uid = message.from_user.id
    txt = (message.text or "").strip()
    radius = RADIUS_M_DEFAULT
    if txt:
        try: radius = float(txt.replace(",", "."))
        except Exception:
            await message.answer("Неверный формат. Введите радиус числом:"); return
    st = STATE[uid]
    add_place(st["key"], st["full"], st["lat"], st["lon"], radius)
    STATE[uid] = {"phase":"idle"}
    await message.answer(f"✅ Школа <b>{st['key']}</b> добавлена.\n(lat={st['lat']}, lon={st['lon']}, r={int(radius)} м)")

@dp.callback_query_handler(lambda c: c.data == "admin:list_places")
async def cb_list_places(callback: types.CallbackQuery):
    rows = list_places()
    if not rows:
        await callback.message.answer("Список школ пуст."); await callback.answer(); return
    lines = ["<b>Школы:</b>"]
    for r in rows:
        lines.append(f"• {r['key']} — r={int(r['radius_m'])} м; lat={r['lat']}, lon={r['lon']}")
    await callback.message.answer("\n".join(lines))
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "admin:del_place")
async def cb_del_place(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True); return
    rows = list_places()
    if not rows:
        await callback.message.answer("Нет школ для удаления.")
        await callback.answer(); return
    kb = InlineKeyboardMarkup()
    for r in rows[:50]:
        kb.add(InlineKeyboardButton(f"🗑 {r['key']}", callback_data=f"admin:delp:{r['key']}"))
    await callback.message.answer("Выберите школу для удаления:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("admin:delp:"))
async def cb_del_place_pick(callback: types.CallbackQuery):
    key = callback.data.split(":",2)[2]
    delete_place(key)
    await callback.message.answer(f"🗑 Удалено: {key} (и связанные уроки).")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "admin:add_lesson")
async def cb_add_lesson(callback: types.CallbackQuery):
    uid = callback.from_user.id
    if uid not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True); return
    kb = InlineKeyboardMarkup()
    for i, name in enumerate(["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]):
        kb.add(InlineKeyboardButton(name, callback_data=f"al:wd:{i}"))
    STATE[uid] = {"phase":"add_lesson_wd"}
    await callback.message.answer("Выберите день недели:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("al:wd:"))
async def al_pick_wd(callback: types.CallbackQuery):
    uid = callback.from_user.id
    wd = int(callback.data.split(":")[2])
    rows = list_places()
    if not rows:
        await callback.message.answer("Нет школ. Сначала добавьте школу.")
        await callback.answer(); return
    kb = InlineKeyboardMarkup()
    for r in rows[:50]:
        kb.add(InlineKeyboardButton(r["key"], callback_data=f"al:place:{wd}:{r['key']}"))
    await callback.message.answer("Выберите школу:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("al:place:"))
async def al_pick_place(callback: types.CallbackQuery):
    uid = callback.from_user.id
    _,_,wd_s,key = callback.data.split(":",3)
    STATE[uid] = {"phase":"al_start", "wd":int(wd_s), "place":key}
    await callback.message.answer("Введите время <b>начала</b> в формате HH:MM (например 09:00):")
    await callback.answer()

@dp.message_handler(lambda m: STATE.get(m.from_user.id,{}).get("phase")=="al_start")
async def al_get_start(message: types.Message):
    uid = message.from_user.id
    t = (message.text or "").strip()
    if not t or len(t)!=5 or t[2]!=":":
        await message.answer("Неверный формат. Введите время начала HH:MM:"); return
    st = STATE[uid]; st["start"]=t; st["phase"]="al_end"
    await message.answer("Введите время <b>окончания</b> в формате HH:MM (например 12:30):")

@dp.message_handler(lambda m: STATE.get(m.from_user.id,{}).get("phase")=="al_end")
async def al_get_end(message: types.Message):
    uid = message.from_user.id
    t = (message.text or "").strip()
    if not t or len(t)!=5 or t[2]!=":":
        await message.answer("Неверный формат. Введите время окончания HH:MM:"); return
    st = STATE[uid]
    add_lesson(st["wd"], st["place"], st["start"], t)
    STATE[uid] = {"phase":"idle"}
    await message.answer(f"✅ Урок добавлен: {day_short(st['wd'])} • {st['place']} • {st['start']}-{t}")

@dp.callback_query_handler(lambda c: c.data == "admin:del_lesson")
async def cb_del_lesson(callback: types.CallbackQuery):
    uid = callback.from_user.id
    kb = InlineKeyboardMarkup()
    for i, name in enumerate(["Пн","Вт","Ср","Чт","Пт","Сб","Вс"]):
        kb.add(InlineKeyboardButton(name, callback_data=f"dl:wd:{i}"))
    await callback.message.answer("Выберите день недели:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("dl:wd:"))
async def dl_pick_wd(callback: types.CallbackQuery):
    wd = int(callback.data.split(":")[2])
    rows = list_schedule(wd)
    if not rows:
        await callback.message.answer("На этот день уроков нет."); await callback.answer(); return
    kb = InlineKeyboardMarkup()
    for r in rows[:50]:
        kb.add(InlineKeyboardButton(f"{r['start']} {r['place_key']}", callback_data=f"dl:pick:{wd}:{r['place_key']}:{r['start']}"))
    await callback.message.answer("Выберите урок для удаления:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("dl:pick:"))
async def dl_do(callback: types.CallbackQuery):
    _,_,wd_s,place_key,start = callback.data.split(":",4)
    delete_lesson(int(wd_s), place_key, start)
    await callback.message.answer(f"🗑 Урок удалён: {day_short(int(wd_s))} • {place_key} • {start}")
    await callback.answer()

# ====== ТЕКСТ РОУТЕР ======
@dp.message_handler(content_types=["text"])
async def text_router(message: types.Message):
    uid = message.from_user.id
    txt = (message.text or "").strip()
    st = STATE.get(uid, {})

    if st.get("phase") == "need_name" and not get_profile(uid):
        if " " not in txt or len(txt) < 3:
            await message.answer("Введите имя и фамилию (например: Азиз Азимов).")
            return
        save_profile(uid, txt, "")
        STATE[uid] = {"phase":"need_contact", "name": txt}
        await message.answer(f"Спасибо, <b>{txt}</b>! Теперь поделитесь контактом:", reply_markup=ask_contact_kb())
        return

    if txt in ("📅 Расписание на сегодня","/today","/schedule"):
        await cmd_schedule(message); return

    if txt == "✅ Отметиться":
        rows = today_slots()
        schools_today = sorted({r["place_key"] for r in rows})
        # Всегда добавляем закреплённую точку (24/7)
        ensure_always_place()
        if ALWAYS_PLACE_KEY not in schools_today:
            schools_today.append(ALWAYS_PLACE_KEY)
        if not schools_today:
            await message.answer("Сегодня нет уроков."); return
        kb = InlineKeyboardMarkup()
        for i, name in enumerate(schools_today[:50]):
            kb.add(InlineKeyboardButton(name, callback_data=f"cs:school:{i}"))
        STATE[uid] = {"phase":"pick_school","schools":schools_today}
        await message.answer("⬇️ Выберите школу:", reply_markup=kb)
        return

# ====== CONTACT ======
@dp.message_handler(content_types=["contact"])
async def on_contact(message: types.Message):
    uid = message.from_user.id
    st = STATE.get(uid, {})
    if get_profile(uid):
        await message.answer("Профиль уже есть.", reply_markup=main_kb())
        return
    if st.get("phase") != "need_contact": return
    if not message.contact or message.contact.user_id != uid:
        await message.answer("Отправьте контакт через кнопку."); return
    name = st.get("name") or message.from_user.full_name
    save_profile(uid, name, message.contact.phone_number)
    STATE[uid] = {"phase":"idle"}
    await message.answer(f"✅ Профиль сохранён\nИмя: {name}\nТел: {message.contact.phone_number}",
                         reply_markup=main_kb())

# ====== CALLBACKS: выбор школы/времени/действия ======
@dp.callback_query_handler(lambda c: c.data.startswith("cs:school:"))
async def choose_school(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid,{})
    idx = int(callback.data.split(":")[2])
    schools = st.get("schools",[])
    if idx<0 or idx>=len(schools):
        with suppress(Exception): await callback.answer()
        return
    school = schools[idx]
    wd = datetime.now(TZ).weekday()
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT start,end FROM schedule WHERE weekday=? AND place_key=? ORDER BY start", (wd, school))
    slots = cur.fetchall(); conn.close()

    # если это SNR School и слотов нет — подставляем 00:00–23:59 (24/7)
    if (not slots) and school == ALWAYS_PLACE_KEY:
        slots = [{"start": "00:00", "end": "23:59"}]  # обычные dict

    kb = InlineKeyboardMarkup()
    for i, s in enumerate(slots[:50]):
        kb.add(InlineKeyboardButton(f"{s['start']}-{s['end']}", callback_data=f"cs:time:{i}"))
    STATE[uid] = {"phase":"pick_time","slots":[dict(s) | {"place":school} for s in slots]}
    await callback.message.answer(f"Школа: {school}\nВыберите время:", reply_markup=kb)
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("cs:time:"))
async def choose_time(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid,{})
    idx = int(callback.data.split(":")[2])
    slots = st.get("slots",[])
    if idx<0 or idx>=len(slots):
        with suppress(Exception): await callback.answer()
        return
    slot = slots[idx]
    STATE[uid] = {"phase":"pick_action","slot":slot}
    # вернули панель действий
    await callback.message.answer(f"{slot['place']} {slot['start']}-{slot['end']}\nВыберите действие:", reply_markup=actions_kb())
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("act:"))
async def on_action(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid,{})
    slot = st.get("slot")
    action = callback.data.split(":")[1]
    STATE[uid] = {"phase":"await_location","slot":slot,"action":action}
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Отправить геолокацию", request_location=True))
    await callback.message.answer(f"{'Чек-ин' if action=='in' else 'Чек-аут'} для {slot['place']}. Отправьте геолокацию:", reply_markup=kb)
    await callback.answer()

# ====== LOCATION (обычная, без Live; запрет пересылки) ======
@dp.message_handler(content_types=["location"])
async def on_location(message: types.Message):
    uid = message.from_user.id
    st = STATE.get(uid,{})
    if st.get("phase")!="await_location": return

    # 1) Запрет пересылки геолокации
    if message.forward_date or message.forward_from or message.forward_from_chat or message.forward_sender_name:
        await message.answer("❌ Нельзя пересылать геолокацию. Отправьте свою геопозицию через кнопку «Отправить геолокацию».")
        return

    slot=st["slot"]; action=st["action"]
    prof=get_profile(uid) or {"name":message.from_user.full_name, "phone":""}

    # берём координаты школы
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT full, lat, lon, radius_m FROM places WHERE key=?", (slot["place"],))
    pl = cur.fetchone()
    if not pl:
        await message.answer("❗ Школа не найдена в базе. Обратитесь к администратору.", reply_markup=main_kb()); return

    lat,lon=message.location.latitude,message.location.longitude

    # вычисляем радиус/дистанцию
    can_check_radius = (pl["lat"] is not None and pl["lon"] is not None)
    dist=None; in_radius=None
    if can_check_radius:
        dist=haversine_m(lat,lon,pl["lat"],pl["lon"])
        in_radius=dist<= (pl["radius_m"] or RADIUS_M_DEFAULT)

    now=datetime.now(TZ)
    wd_name = weekday_ru(now)
    act_text = "Чек-ин" if action=="in" else "Чек-аут"

    # -- НОВАЯ ПАНЕЛЬКА (как просили) --
    lines = [
        f"📍 <b>{prof['name']}</b>",
        f"🏫 {pl['full']}",
        f"📅 {wd_name}",
        f"⏱️ {now.strftime('%H:%M %Y-%m-%d')}",
        f"🕘 Слот: {slot['start']}–{slot['end']}",
        f"🔄 Действие: <b>{act_text}</b>",
    ]

    # «во время» для ЧЕК-ИН: до (start + LATE_GRACE_MIN)
    on_time = None
    try:
        sh, sm = map(int, str(slot['start']).split(":"))
        start_dt = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
        grace_dt = start_dt + timedelta(minutes=LATE_GRACE_MIN)
        if action == "in":
            on_time = now <= grace_dt
    except Exception:
        on_time = None

    if action == "in" and on_time is True:
        lines.append(f"✅ ВО ВРЕМЯ")
        lines.append(f"⏰ Должен быть к: {slot['start']} (+{LATE_GRACE_MIN} мин)")
    else:
        if not can_check_radius:
            lines.append("⚠️ Геолокация места не настроена — проверка радиуса пропущена")
            lines.append("⚠️ Проверка радиуса недоступна")
        else:
            if in_radius is True:
                lines.append(f"✅ В радиусе ({pretty_m(dist)})")
            elif in_radius is False:
                lines.append(f"🚫 Вне радиуса ({pretty_m(dist)})")
        # второй рядок со слотом — через дефис
        lines.append(f"⏰ Слот: {slot['start']}-{slot['end']}")

    panel_text = "\n".join(lines)
    await message.answer(panel_text, reply_markup=main_kb())

    # пишем в БД
    on_time_int = None
    if action == "in" and on_time is not None:
        on_time_int = 1 if on_time else 0
    elif in_radius is not None:
        on_time_int = 1 if in_radius else 0

    cur.execute("""INSERT INTO checks(
        telegram_id,teacher_name,phone,action,place_key,place_full,date,time,weekday,
        slot_start,slot_end,lat,lon,distance_m,in_radius,on_time,notes
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        uid, prof["name"], prof.get("phone",""), action, slot["place"], pl["full"],
        now.strftime("%Y-%m-%d"), now.strftime("%H:%M"), wd_name,
        slot["start"], slot["end"], float(f"{lat:.6f}"), float(f"{lon:.6f}"),
        float(round(dist,2)) if dist is not None else None,
        1 if in_radius else 0 if in_radius is not None else None,
        on_time_int,
        ""
    ))
    conn.commit(); conn.close()

    # синхронно пишем в CSV
    write_check_to_csv({
        "telegram_id": uid,
        "teacher_name": prof["name"],
        "phone": prof.get("phone",""),
        "action": action,
        "place_key": slot["place"],
        "place_full": pl["full"],
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M"),
        "weekday": wd_name,
        "slot_start": slot["start"],
        "slot_end": slot["end"],
        "lat": f"{lat:.6f}",
        "lon": f"{lon:.6f}",
        "distance_m": float(round(dist,2)) if dist is not None else "",
        "in_radius": in_radius if in_radius is not None else 0,
        "on_time": (1 if on_time else 0) if on_time is not None else (1 if in_radius else 0 if in_radius is not None else 0),
        "notes": ""
    })

    STATE[uid]={"phase":"idle"}

# ====== LATE WATCHER ======
async def late_watcher():
    await asyncio.sleep(3)
    while True:
        try:
            now=datetime.now(TZ); wd=now.weekday(); date_s=now.strftime("%Y-%m-%d")
            conn = db(); cur = conn.cursor()
            cur.execute("SELECT start, place_key FROM schedule WHERE weekday=?", (wd,))
            slots = cur.fetchall()
            cur.execute("SELECT place_key FROM checks WHERE date=? AND action='in'", (date_s,))
            ins = {r["place_key"] for r in cur.fetchall()}
            conn.close()

            for slot in slots:
                sh,sm=map(int,slot["start"].split(":"))
                start_dt=now.replace(hour=sh,minute=sm,second=0,microsecond=0)
                if now>start_dt+timedelta(minutes=LATE_GRACE_MIN):
                    slot_key=(date_s,wd,slot["place_key"],slot["start"])
                    if slot_key in LATE_SENT_SLOTS: continue
                    if slot["place_key"] not in ins:
                        await notify_admins(f"⚠️ Нет чек-ина {slot['place_key']} {slot['start']}")
                        LATE_SENT_SLOTS.add(slot_key)
        except Exception:
            log.exception("late_watcher")
        await asyncio.sleep(60)

# ====== ERRORS ======
@dp.errors_handler()
async def global_errors(update, error):
    if isinstance(error, Throttled): return True
    log.exception("Unhandled: %r", error); return True

# ====== MAIN ======
if __name__=="__main__":
    ensure_csv_files()
    init_db()
    import_legacy()
    ensure_always_place()
    log.info("Starting polling…")
    loop=asyncio.get_event_loop()
    loop.create_task(late_watcher())
    executor.start_polling(dp, skip_updates=True)
