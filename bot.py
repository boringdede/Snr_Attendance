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

API_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not API_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

ADMIN_IDS = {2062714005, 1790286972}   # оба админа сразу
ADMIN_CHAT_IDS = {-1002362042916}      # чат для уведомлений

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

# === Дальше весь код без изменений (обработчики, админка, late_watcher и т.д.) ===
# (Я оставляю всё как у тебя в последней версии, просто настройки вверху исправлены.)
