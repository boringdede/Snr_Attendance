# bot.py — aiogram 2.25.1, SQLite, анти-пересыл, бэкап/восстановление, напоминания, персональные правила SNR
# pip install aiogram==2.25.1

import os
import csv
import json
import asyncio
import logging
import sqlite3
from contextlib import suppress
from pathlib import Path
from datetime import datetime, timezone, timedelta

from aiogram import Bot, Dispatcher, executor, types
from aiogram.utils.exceptions import Throttled
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardRemove, InputFile
)

# ============ НАСТРОЙКИ ============
HARDCODED_FALLBACK_TOKEN = "8278332572:AAHAYzg0_GvRmmWhUgbndOYlSwB790OfNHE"
API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or HARDCODED_FALLBACK_TOKEN
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set.")

# Админы и админ-группы
ADMIN_IDS = {1790286972, 2062714005}
ADMIN_CHAT_IDS = {-1002362042916}

# Гео/время
RADIUS_M_DEFAULT = 200.0
CITY_TZ_HOURS = 5
LATE_GRACE_MIN = 10                         # глобальная «поздно» для /late_watcher пингов (может не использоваться для оценки чек-ина)
REMINDERS_ON = os.getenv("REMINDERS", "0") == "1"  # напоминания за 10 минут

# Закреплённое место (SNR — доступно всегда)
ALWAYS_PLACE_KEY = "SNR School"
ALWAYS_PLACE_FULL = "SNR School (офис)"
ALWAYS_PLACE_LAT = 41.322921
ALWAYS_PLACE_LON = 69.277808

# Персональные правила SNR (ТОЛЬКО для SNR School)
# user_id: разрешённое опоздание (минуты)
SNR_SPECIAL_GRACE = {
    5280510534: 15,   # Ситора Муслимова
    1677978086: 15,   # Камола Нарзиева
    1033120831: 10,   # Сарварбек Эшмуродов
    1790286972: 10,   # Аббосхон Азларов
}
# Пользователь, который должен быть в SNR к 09:00 (+10 мин максимум)
SNR_MUST_BE_9_ID = 7819786422
SNR_MUST_BE_9_GRACE = 10

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

# ====== РАНТАЙМ ======
STATE = {}          # визарды/состояния
LATE_SENT_SLOTS = set()
REMINDER_SENT = set()  # (date, place_key, start) чтобы 10-минутные не слали многократно

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

def is_forwarded(msg: types.Message) -> bool:
    return any([
        getattr(msg, "forward_date", None),
        getattr(msg, "forward_from", None),
        getattr(msg, "forward_from_chat", None),
        getattr(msg, "forward_sender_name", None),
        getattr(msg, "forward_from_message_id", None),
        getattr(msg, "forward_signature", None),
        hasattr(msg, "forward_origin") and getattr(msg, "forward_origin") is not None,
    ])

# ====== CSV ======
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

# ====== БАЗА ДАННЫХ (SQLite) ======
def db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = db(); cur = conn.cursor()
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
    cur.execute("""CREATE TABLE IF NOT EXISTS stopped (telegram_id INTEGER PRIMARY KEY)""")
    # индексы
    cur.execute("CREATE INDEX IF NOT EXISTS idx_schedule_wd_place_start ON schedule(weekday, place_key, start)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_checks_date_place_action ON checks(date, place_key, action)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_checks_date_tid ON checks(date, telegram_id)")
    conn.commit(); conn.close()

def import_legacy_if_empty():
    """Оставлено на случай, если где-то есть JSON-сид — сейчас не используем."""
    return

def ensure_always_place():
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

# ====== BACKUP / RESTORE ======
def dump_data_for_backup() -> dict:
    conn = db(); cur = conn.cursor()
    cur.execute("SELECT key, full, lat, lon, radius_m FROM places ORDER BY key")
    places = [dict(r) for r in cur.fetchall()]
    cur.execute("SELECT weekday, start, end, place_key FROM schedule ORDER BY weekday, start")
    schedule = [dict(r) for r in cur.fetchall()]
    conn.close()
    return {"version": 1, "places": places, "schedule": schedule}

def load_data_from_backup(payload: dict):
    assert isinstance(payload, dict) and "places" in payload and "schedule" in payload
    conn = db(); cur = conn.cursor()
    cur.execute("DELETE FROM schedule")
    cur.execute("DELETE FROM places")
    for p in payload["places"]:
        cur.execute(
            "INSERT INTO places(key,full,lat,lon,radius_m) VALUES(?,?,?,?,?)",
            (p["key"], p["full"], p.get("lat"), p.get("lon"), p.get("radius_m") or RADIUS_M_DEFAULT)
        )
    for s in payload["schedule"]:
        cur.execute(
            "INSERT INTO schedule(weekday,start,end,place_key) VALUES(?,?,?,?)",
            (int(s["weekday"]), s["start"], s["end"], s["place_key"])
        )
    conn.commit(); conn.close()
    ensure_always_place()

@dp.message_handler(commands=["backup"])
async def cmd_backup(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет доступа."); return
    data = dump_data_for_backup()
    path = DATA_DIR / "backup.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    await bot.send_document(message.chat.id, InputFile(str(path)),
                            caption=f"backup {datetime.now(TZ).strftime('%Y-%m-%d %H:%M')}")

@dp.message_handler(commands=["restore"])
async def cmd_restore(message: types.Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет доступа."); return
    STATE[message.from_user.id] = {"phase": "await_restore_file"}
    await message.answer("Пришлите файл <code>backup.json</code> с подписью <b>restore</b>.")

@dp.message_handler(content_types=["document"])
async def on_doc_restore(message: types.Message):
    uid = message.from_user.id
    if STATE.get(uid, {}).get("phase") != "await_restore_file":
        return
    if message.from_user.id not in ADMIN_IDS:
        await message.answer("⛔ Нет доступа."); return
    if (message.caption or "").strip().lower() != "restore":
        await message.answer("Нужна подпись (caption) <b>restore</b> у файла."); return
    try:
        file = await bot.get_file(message.document.file_id)
        tmp = DATA_DIR / "restore_tmp.json"
        await bot.download_file(file.file_path, destination=tmp)
        payload = json.loads(tmp.read_text(encoding="utf-8"))
        load_data_from_backup(payload)
        STATE[uid] = {"phase":"idle"}
        await message.answer("✅ Восстановлено: школы/сады и расписание.")
    except Exception as e:
        log.exception("restore failed")
        await message.answer(f"❌ Ошибка восстановления: {e}")

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
    import_legacy_if_empty()
    ensure_always_place()
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

# ====== «РАСПИСАНИЕ НА СЕГОДНЯ» С ПОДСПИСКОМ ПО ШКОЛЕ ======
@dp.message_handler(commands=["schedule","today"])
async def cmd_schedule(message: types.Message):
    now = datetime.now(TZ); wd = now.weekday()
    rows = list_schedule(wd)
    schools_today = sorted({r["place_key"] for r in rows})
    ensure_always_place()
    if ALWAYS_PLACE_KEY not in schools_today:
        schools_today.append(ALWAYS_PLACE_KEY)
    if not schools_today:
        await message.answer(f"Расписание на <b>{weekday_ru(now)}</b>:\n• (слотов нет)")
        return
    kb = InlineKeyboardMarkup()
    for i, name in enumerate(schools_today[:50]):
        kb.add(InlineKeyboardButton(name, callback_data=f"tday:school:{i}"))
    STATE[message.from_user.id] = {"phase": "pick_school_today", "schools": schools_today}
    await message.answer(f"Расписание на <b>{weekday_ru(now)}</b>:\nВыберите школу, чтобы увидеть слоты.", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith("tday:school:"))
async def cb_today_school(callback: types.CallbackQuery):
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
    if not slots and school == ALWAYS_PLACE_KEY:
        await callback.message.answer(f"🏫 {school}\n• Доступно для отметки: 00:00–23:59 (круглосуточно)")
    elif not slots:
        await callback.message.answer(f"🏫 {school}\n• (слотов нет сегодня)")
    else:
        lines = [f"🏫 {school} — слоты на сегодня:"]
        for s in slots:
            lines.append(f"• {s['start']}–{s['end']}")
        await callback.message.answer("\n".join(lines))
    await callback.answer()

# ====== АДМИН МЕНЮ (как было) ======
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

# ====== ВИЗАРДЫ ДОБАВЛЕНИЯ/УДАЛЕНИЯ ======
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

    # Если это SNR School и слотов нет — показываем 00:00–23:59
    if (not slots) and school == ALWAYS_PLACE_KEY:
        slots = [{"start": "00:00", "end": "23:59"}]

    kb = InlineKeyboardMarkup()
    for i, s in enumerate(slots[:50]):
        s_start = s['start'] if isinstance(s, sqlite3.Row) else s['start']
        s_end   = s['end']   if isinstance(s, sqlite3.Row) else s['end']
        kb.add(InlineKeyboardButton(f"{s_start}-{s_end}", callback_data=f"cs:time:{i}"))
    # Храним слоты в STATE
    STATE[uid] = {"phase":"pick_time","slots":[{"start": (s['start'] if isinstance(s, sqlite3.Row) else s['start']),
                                                "end":   (s['end']   if isinstance(s, sqlite3.Row) else s['end']),
                                                "place": school} for s in slots]}
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
    await callback.message.answer(f"{slot['place']} {slot['start']}-{slot['end']}\nВыберите действие:", reply_markup=actions_kb())
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data.startswith("act:"))
async def on_action(callback: types.CallbackQuery):
    uid = callback.from_user.id
    st = STATE.get(uid,{})
    action = callback.data.split(":")[1]
    slot = st.get("slot")
    if not slot:
        await callback.message.answer("Пожалуйста, заново выберите школу и время (слот не найден).",
                                      reply_markup=main_kb())
        with suppress(Exception): await callback.answer()
        return

    STATE[uid] = {"phase":"await_location","slot":slot,"action":action}
    kb = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    kb.add(KeyboardButton("Отправить геолокацию", request_location=True))
    await callback.message.answer(f"{'Чек-ин' if action=='in' else 'Чек-аут'} для {slot['place']}. Отправьте геолокацию:", reply_markup=kb)
    await callback.answer()

# ====== ОТПРАВКА ОТЧЁТА В АДМИН-ЧАТ ======
async def report_check_to_admins(*, teacher_name: str, place_full: str, weekday_str: str,
                                 now_str: str, slot_start: str, slot_end: str, action: str,
                                 in_radius, dist, on_time_flag, lat: float, lon: float,
                                 is_snr: bool, show_time_status: bool):
    act_text = "Чек-ин" if action == "in" else "Чек-аут"
    status = []

    # Показ статуса «во время/поздно» скрываем для SNR по умолчанию
    if show_time_status:
        if on_time_flag is True:
            status.append("✅ ВО ВРЕМЯ")
        elif on_time_flag is False:
            status.append("🛑 ПОЗДНО")

    # Радиус
    if in_radius is True:
        status.append(f"✅ В радиусе ({pretty_m(dist)})")
    elif in_radius is False:
        status.append(f"🚫 Вне радиуса ({pretty_m(dist)})")

    text = (
        f"📍 <b>{teacher_name}</b>\n"
        f"🏫 {place_full}\n"
        f"📅 {weekday_str}\n"
        f"⏱️ {now_str}\n"
        f"🕘 Слот: {slot_start}–{slot_end}\n"
        f"🔄 Действие: <b>{act_text}</b>\n"
        + ("\n".join(status) if status else "")
        + f"\n📍 <a href='https://maps.google.com/?q={lat:.6f},{lon:.6f}'>Открыть на карте</a>"
    )

    for chat_id in ADMIN_CHAT_IDS:
        with suppress(Exception):
            await bot.send_message(chat_id, text, disable_web_page_preview=False)
            await bot.send_location(chat_id, latitude=lat, longitude=lon, disable_notification=True)

# ====== LOCATION (ГЛАВНЫЙ: жёсткий радиус, жёсткое опоздание) ======
@dp.message_handler(content_types=["location"])
async def on_location(message: types.Message):
    uid = message.from_user.id
    st = STATE.get(uid,{})
    if st.get("phase")!="await_location": return

    # Запрет пересылки
    if is_forwarded(message):
        await message.answer("❌ Нельзя пересылать геолокацию. Отправьте свою геопозицию кнопкой «Отправить геолокацию».")
        return

    slot = st.get("slot"); action = st.get("action")
    if not slot or not action:
        await message.answer("Слот не найден. Нажмите «✅ Отметиться» и выберите школу/время заново.",
                             reply_markup=main_kb())
        STATE[uid] = {"phase":"idle"}
        return

    prof = get_profile(uid) or {"name":message.from_user.full_name, "phone":""}

    conn = db(); cur = conn.cursor()
    cur.execute("SELECT full, lat, lon, radius_m FROM places WHERE key=?", (slot["place"],))
    pl = cur.fetchone()
    if not pl:
        await message.answer("❗ Школа не найдена в базе. Обратитесь к администратору.", reply_markup=main_kb());
        STATE[uid] = {"phase":"idle"}
        return

    lat = message.location.latitude
    lon = message.location.longitude

    # Проверка радиуса ОБЯЗАТЕЛЬНА для приёма чек-ина
    can_check_radius = (pl["lat"] is not None and pl["lon"] is not None)
    dist=None; in_radius=None
    if can_check_radius:
        dist=haversine_m(lat,lon,pl["lat"],pl["lon"])
        in_radius=dist<= (pl["radius_m"] or RADIUS_M_DEFAULT)
    if (not can_check_radius) or (in_radius is not True):
        # Не принимаем чек-ин
        await message.answer("Вы не в радиусе 🛑\nПодойдите ближе к месту и попробуйте снова.", reply_markup=main_kb())
        STATE[uid]={"phase":"idle"}
        return

    now=datetime.now(TZ)
    wd_name = weekday_ru(now)
    act_text = "Чек-ин" if action=="in" else "Чек-аут"

    # === Оценка «во время/поздно» ===
    s_h, s_m = map(int, str(slot['start']).split(":"))
    start_dt = now.replace(hour=s_h, minute=s_m, second=0, microsecond=0)

    is_snr = (slot["place"] == ALWAYS_PLACE_KEY)

    # Глобальные правила:
    # 1) Для НЕ SNR: допуск = 0 минут (строго). Любая минута позже — поздно.
    # 2) Для SNR: по умолчанию не показываем статус времени. НО:
    #    - особые пользователи SNR имеют персональную «grace»
    #    - спец-пользователь должен быть к 09:00 (+10)
    show_time_status = True
    grace_min = 0

    if not is_snr:
        grace_min = 0
    else:
        # SNR — по умолчанию статусы не показываем
        show_time_status = False
        # Персональные правила SNR
        if uid in SNR_SPECIAL_GRACE:
            grace_min = SNR_SPECIAL_GRACE[uid]
            show_time_status = True
        elif uid == SNR_MUST_BE_9_ID:
            # обязателен к 09:00 (+10)
            must_h, must_m = 9, 0
            start_dt = now.replace(hour=must_h, minute=must_m, second=0, microsecond=0)
            grace_min = SNR_MUST_BE_9_GRACE
            show_time_status = True
        else:
            grace_min = 0  # не важно — статус всё равно скрыт

    on_time_flag = None
    if action == "in":
        on_time_flag = (now <= start_dt + timedelta(minutes=grace_min))

    # === Формирование панели пользователю ===
    lines = [
        f"📍 <b>{prof['name']}</b>",
        f"🏫 {pl['full']}",
        f"📅 {wd_name}",
        f"⏱️ {now.strftime('%H:%M %Y-%m-%d')}",
        f"🕘 Слот: {slot['start']}–{slot['end']}",
        f"🔄 Действие: <b>{act_text}</b>",
    ]

    # Для SNR по умолчанию не выводим «во время/поздно», только время прибытия и радиус
    if show_time_status:
        if action == "in":
            if on_time_flag:
                lines.append("✅ ВО ВРЕМЯ")
            else:
                lines.append("🛑 ПОЗДНО")
    # Радиус уже проверен (иначе бы вышли), но покажем факт
    lines.append(f"✅ В радиусе ({pretty_m(dist)})")

    panel_text = "\n".join(lines)
    await message.answer(panel_text, reply_markup=main_kb())

    # === Отчёт в админ-чат (оставить) ===
    try:
        await report_check_to_admins(
            teacher_name=prof['name'],
            place_full=pl['full'],
            weekday_str=wd_name,
            now_str=now.strftime('%H:%M %Y-%m-%d'),
            slot_start=slot['start'],
            slot_end=slot['end'],
            action=action,
            in_radius=True,
            dist=dist,
            on_time_flag=on_time_flag,
            lat=lat,
            lon=lon,
            is_snr=is_snr,
            show_time_status=show_time_status
        )
    except Exception:
        log.exception("report_check_to_admins failed")

    # === Запись в БД/CSV ===
    on_time_int = None
    if action == "in" and on_time_flag is not None:
        on_time_int = 1 if on_time_flag else 0

    conn = db(); cur = conn.cursor()
    cur.execute("""INSERT INTO checks(
        telegram_id,teacher_name,phone,action,place_key,place_full,date,time,weekday,
        slot_start,slot_end,lat,lon,distance_m,in_radius,on_time,notes
    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
        uid, prof["name"], prof.get("phone",""), action, slot["place"], pl["full"],
        now.strftime("%Y-%m-%d"), now.strftime("%H:%M"), wd_name,
        slot["start"], slot["end"], float(f"{lat:.6f}"), float(f"{lon:.6f}"),
        float(round(dist,2)) if dist is not None else None,
        1,  # in_radius — гарантированно True на этом этапе
        on_time_int,
        ""
    ))
    conn.commit(); conn.close()

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
        "in_radius": True,
        "on_time": (1 if on_time_flag else 0) if on_time_flag is not None else 0,
        "notes": ""
    })

    STATE[uid]={"phase":"idle"}

# ====== НАПОМИНАНИЯ И КОНТРОЛЬ ОПОЗДАНИЙ (фоновая задача) ======
async def late_watcher():
    await asyncio.sleep(3)
    while True:
        try:
            now = datetime.now(TZ)
            wd = now.weekday()
            date_s = now.strftime("%Y-%m-%d")

            conn = db(); cur = conn.cursor()
            cur.execute("SELECT start,end,place_key FROM schedule WHERE weekday=?", (wd,))
            slots = cur.fetchall()
            cur.execute("SELECT telegram_id FROM profiles")
            teachers = [r["telegram_id"] for r in cur.fetchall()]
            cur.execute("SELECT telegram_id, place_key FROM checks WHERE date=? AND action='in'", (date_s,))
            ins = {(r["telegram_id"], r["place_key"]) for r in cur.fetchall()}
            conn.close()

            for slot in slots:
                sh,sm=map(int,slot["start"].split(":"))
                start_dt=now.replace(hour=sh,minute=sm,second=0,microsecond=0)

                # Напоминание за 10 минут
                if REMINDERS_ON:
                    rem_key = (date_s, slot["place_key"], slot["start"])
                    if start_dt - timedelta(minutes=10) <= now < start_dt and rem_key not in REMINDER_SENT:
                        for tid in teachers:
                            with suppress(Exception):
                                await bot.send_message(
                                    tid,
                                    f"⏰ Через 10 минут у вас урок в <b>{slot['place_key']}</b> ({slot['start']}–{slot['end']}). Не забудьте отметиться!"
                                )
                        REMINDER_SENT.add(rem_key)

                # Старое уведомление для админов — если совсем нет чек-ина по месту
                late_dt = start_dt + timedelta(minutes=LATE_GRACE_MIN)
                if now>late_dt:
                    slot_key=(date_s,wd,slot["place_key"],slot["start"])
                    if slot_key in LATE_SENT_SLOTS:
                        continue
                    if not any(pk == slot["place_key"] for (_tid, pk) in ins):
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
    import_legacy_if_empty()
    ensure_always_place()
    log.info("Starting polling…")
    loop=asyncio.get_event_loop()
    loop.create_task(late_watcher())
    executor.start_polling(dp, skip_updates=True)
