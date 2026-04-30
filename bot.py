"""
Telegram-бот з погодою, логуванням, аналітикою та графіками.

Цей бот демонструє бібліотеку pandas для аналізу даних.
Pandas — потужна бібліотека Python для роботи з табличними даними.
Основна структура pandas — DataFrame (таблиця з рядками та колонками).

Потрібні бібліотеки:
    pip install python-telegram-bot requests pandas matplotlib python-dotenv

Перед запуском:
    1. Отримай токен у @BotFather
    2. Отримай API-ключ на https://openweathermap.org/api (безкоштовно)
    3. Створи файл .env з ключами TELEGRAM_TOKEN та WEATHER_API_KEY
"""

# ── Стандартні бібліотеки Python (вбудовані, не потребують встановлення) ──
import csv          # робота з CSV-файлами (читання/запис таблиць)
import io           # робота з потоками байтів (для відправки графіків)
import logging      # логування подій (для дебагу)
import os           # доступ до змінних середовища (os.getenv)
from datetime import datetime  # робота з датами та часом
from pathlib import Path       # зручна робота з шляхами до файлів

# ── Сторонні бібліотеки (встановлюємо через pip) ──
from dotenv import load_dotenv  # завантаження змінних з .env файлу

import matplotlib
matplotlib.use("Agg")  # без GUI — для серверів (рендеримо графіки у пам'яті)
import matplotlib.pyplot as plt  # побудова графіків

import pandas as pd  # pandas — бібліотека для аналізу табличних даних (DataFrame)

import psycopg2  # драйвер PostgreSQL (той самий, що у sql_basics.py)

import requests  # HTTP-запити до зовнішніх API

from telegram import Update  # об'єкт оновлення від Telegram
from telegram.ext import (
    ApplicationBuilder,  # будівник додатку
    CommandHandler,      # обробник команд (/start, /help, ...)
    MessageHandler,      # обробник текстових повідомлень
    filters,             # фільтри повідомлень (TEXT, COMMAND, ...)
    ContextTypes,        # типи контексту для хендлерів
)

# ──────────────────────────────────────────────
# Конфігурація — завантажуємо токени з .env файлу
# ──────────────────────────────────────────────
# load_dotenv() шукає файл .env у поточній директорії
# і завантажує змінні з нього в os.environ.
# Це безпечніше, ніж писати токени прямо в коді!
load_dotenv()

# os.getenv("KEY") — бере значення змінної середовища за ключем.
# Другий аргумент "" — значення за замовчуванням, якщо змінної немає.
# .strip() — видаляє зайві пробіли та переноси рядків.
# .rstrip(";") — видаляє крапку з комою в кінці (типова помилка в .env файлах).
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip().rstrip(";")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "").strip()

# Перевіряємо, що токени не порожні — інакше бот не зможе працювати
if not TELEGRAM_TOKEN or not WEATHER_API_KEY:
    raise ValueError(
        "Не знайдено TELEGRAM_TOKEN або WEATHER_API_KEY!\n"
        "Переконайся, що файл .env існує і містить обидва ключі."
    )

# Шлях до CSV-файлу, де зберігатимуться логи запитів
LOG_FILE = Path("bot_log.csv")

# ── Налаштування PostgreSQL ──
# Ті самі змінні, що й у sql_basics.py. Бот пише запити паралельно:
# і в CSV (для pandas-аналітики), і в таблицю weather_log (для SQL-аналітики).
# Якщо БД недоступна — бот продовжує працювати, пишучи тільки в CSV.
DB_NAME = os.getenv("DB_NAME", "weather_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

# Налаштування логування (для дебагу в консолі)
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════
# 1. ЛОГЕР — зберігає кожен запит у CSV
# ══════════════════════════════════════════════
# CSV (Comma-Separated Values) — текстовий формат для зберігання таблиць.
# Кожен рядок — один запис, значення розділені комами.
# Pandas чудово працює з CSV: pd.read_csv() зчитує CSV у DataFrame.
# Тому ми зберігаємо логи саме у CSV — потім легко аналізувати через pandas.

def init_log():
    """Створює CSV-файл із заголовками, якщо його ще немає."""
    if not LOG_FILE.exists():
        with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            # Це заголовки колонок — вони стануть назвами колонок у DataFrame
            writer.writerow(["timestamp", "user_id", "username", "city", "temp", "feels_like", "description"])


def log_request(user_id: int, username: str, city: str, temp: float, feels_like: float, description: str):
    """Додає один рядок у лог — кожен виклик = один новий рядок у CSV."""
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.now().isoformat(),  # поточний час у форматі ISO 8601
            user_id,
            username,
            city,
            temp,
            feels_like,
            description,
        ])


# ══════════════════════════════════════════════
# 1b. БАЗА ДАНИХ — паралельно з CSV пишемо в PostgreSQL
# ══════════════════════════════════════════════
# Чому паралельно? CSV — простий формат для pandas (урок №1),
# а PostgreSQL — справжня база даних для SQL-запитів (урок №2 — sql_basics.py).
# Пишемо одні й ті самі дані в обидва місця, щоб на одному боті можна було
# вчити і pandas, і SQL.
#
# Схема таблиці weather_log точно така сама, як у sql_basics.py:
# timestamp, user_id, username, city, temp, feels_like, description.

# Одне глобальне з'єднання з БД — відкриваємо раз, перевикористовуємо для всіх запитів.
# Відкривати нове з'єднання на кожне повідомлення — повільно й марнотратно.
_db_conn = None


def get_db_conn():
    """Повертає активне з'єднання з PostgreSQL, перепідключаючись за потреби."""
    global _db_conn
    # conn.closed == 0 означає "живе". Якщо з'єднання ще не створене
    # або було закрите (наприклад, після втрати мережі) — створюємо нове.
    if _db_conn is None or _db_conn.closed:
        _db_conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        # autocommit = True: кожен INSERT фіксується одразу, без conn.commit().
        _db_conn.autocommit = True
    return _db_conn


def init_db():
    """Створює таблицю weather_log, якщо її ще немає.

    На відміну від sql_basics.py (який робить DROP TABLE для чистого старту уроку),
    бот НЕ видаляє існуючі дані — тільки додає нові записи.
    CREATE TABLE IF NOT EXISTS — створити тільки якщо таблиці ще не існує.
    """
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather_log (
                    id          SERIAL PRIMARY KEY,
                    timestamp   TIMESTAMP NOT NULL,
                    user_id     INTEGER NOT NULL,
                    username    VARCHAR(100) NOT NULL,
                    city        VARCHAR(100) NOT NULL,
                    temp        REAL NOT NULL,
                    feels_like  REAL NOT NULL,
                    description TEXT NOT NULL
                )
            """)
        logger.info("✅ PostgreSQL підключено, таблиця weather_log готова.")
    except Exception as e:
        # Бот не повинен падати, якщо БД недоступна — просто працюватимемо без неї.
        logger.warning(f"⚠️  PostgreSQL недоступний: {e}. Бот писатиме тільки у CSV.")


def log_to_db(user_id: int, username: str, city: str, temp: float, feels_like: float, description: str):
    """Додає один рядок у таблицю weather_log (PostgreSQL).

    Використовуємо параметризований запит (%s) — так само, як у sql_basics.py.
    Це захищає від SQL-ін'єкцій (наприклад, якщо хтось введе місто з лапками
    або спецсимволами — psycopg2 автоматично їх екранує).
    """
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO weather_log
                    (timestamp, user_id, username, city, temp, feels_like, description)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (datetime.now(), user_id, username, city, temp, feels_like, description),
            )
    except Exception as e:
        # Якщо БД лягла посеред дня — не зриваємо відповідь користувачу,
        # лог просто осяде тільки в CSV.
        logger.warning(f"⚠️  Не вдалося записати в PostgreSQL: {e}")


def get_top_cities_sql(limit: int = 5):
    """Повертає топ-N міст за кількістю запитів із PostgreSQL.

    Демонстрація уроку №3 (sql_groupby.py): GROUP BY + ORDER BY + LIMIT.
    SQL-аналог pandas-функції з /stats:
        df["city"].value_counts().head(N)

    Повертає список кортежів [(city, count), ...] або None, якщо БД недоступна.
    Хендлер /top_cities на основі None покаже користувачу зрозумілу помилку.
    """
    try:
        conn = get_db_conn()
        with conn.cursor() as cur:
            # Параметризований запит (%s) — limit підставляється безпечно.
            # GROUP BY city — розбиваємо рядки в купки за містом.
            # COUNT(*) AS query_count — рахуємо кожну купку, даємо ім'я колонці.
            # ORDER BY query_count DESC — найбільші купки зверху.
            # LIMIT %s — лишаємо тільки N перших.
            cur.execute(
                """
                SELECT city, COUNT(*) AS query_count
                FROM weather_log
                GROUP BY city
                ORDER BY query_count DESC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()
    except Exception as e:
        logger.warning(f"⚠️  PostgreSQL запит /top_cities впав: {e}")
        return None


# ══════════════════════════════════════════════
# 2. WEATHER API — отримуємо погоду з OpenWeatherMap
# ══════════════════════════════════════════════

def get_weather(city: str) -> dict | None:
    """
    Запитує погоду у OpenWeatherMap.
    Повертає dict з даними або None, якщо місто не знайдено.
    """
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,                  # назва міста для пошуку
        "appid": WEATHER_API_KEY,   # наш API-ключ (з .env)
        "units": "metric",          # температура в °C (не в Кельвінах)
        "lang": "uk",               # опис погоди українською мовою
    }

    try:
        # Робимо GET-запит до API з таймаутом 10 секунд
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()  # кидає виняток, якщо статус != 200
        data = resp.json()       # парсимо JSON-відповідь у dict

        return {
            "city": data["name"],
            "temp": data["main"]["temp"],
            "feels_like": data["main"]["feels_like"],
            "description": data["weather"][0]["description"],
            "humidity": data["main"]["humidity"],
            "wind": data["wind"]["speed"],
        }
    except requests.exceptions.HTTPError:
        return None  # місто не знайдено (404) або інша HTTP-помилка
    except Exception as e:
        logger.error(f"Weather API error: {e}")
        return None


# ══════════════════════════════════════════════
# 3. ХЕНДЛЕРИ БОТА — обробники команд
#    Тут ми використовуємо pandas для аналізу даних!
# ══════════════════════════════════════════════

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start — привітання."""
    await update.message.reply_text(
        "Привіт! Я погодний бот з аналітикою.\n\n"
        "Просто напиши назву міста — і я покажу погоду.\n\n"
        "Команди:\n"
        "/stats — статистика запитів (pandas з CSV)\n"
        "/plot — графік температур\n"
        "/plot Київ — графік для одного міста\n"
        "/top_cities — топ-5 міст (SQL з PostgreSQL)\n"
        "/help — допомога"
    )


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help."""
    await update.message.reply_text(
        "Напиши назву міста — отримаєш погоду.\n\n"
        "Кожен запит зберігається, тому:\n"
        "/stats — зведена статистика через pandas (читає CSV)\n"
        "/plot — намалює графік температур\n"
        "/plot Київ — графік тільки для Києва\n"
        "/top_cities — те саме, що /stats, але через SQL GROUP BY на PostgreSQL"
    )


async def weather(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє текстове повідомлення як назву міста."""
    city = update.message.text.strip()

    if not city:
        return

    # Запитуємо погоду через API
    data = get_weather(city)

    if data is None:
        await update.message.reply_text(f"Не знайшов місто «{city}». Спробуй ще раз.")
        return

    # Логуємо запит паралельно у ДВА місця:
    #   1) CSV — для уроку pandas (/stats, /plot читають звідти)
    #   2) PostgreSQL — для уроку SQL (таблицю weather_log аналізуємо у sql_basics.py)
    # Обидва записи незалежні: якщо один із них впаде, другий все одно виконається.
    user = update.effective_user
    log_request(
        user_id=user.id,
        username=user.username or user.first_name or "unknown",
        city=data["city"],
        temp=data["temp"],
        feels_like=data["feels_like"],
        description=data["description"],
    )
    log_to_db(
        user_id=user.id,
        username=user.username or user.first_name or "unknown",
        city=data["city"],
        temp=data["temp"],
        feels_like=data["feels_like"],
        description=data["description"],
    )

    # Відповідаємо користувачу
    await update.message.reply_text(
        f"🌍 {data['city']}\n"
        f"🌡 Температура: {data['temp']}°C (відчувається як {data['feels_like']}°C)\n"
        f"☁ {data['description'].capitalize()}\n"
        f"💧 Вологість: {data['humidity']}%\n"
        f"💨 Вітер: {data['wind']} м/с"
    )


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /stats — зведена статистика.

    ╔══════════════════════════════════════════════════════════════╗
    ║  Тут починається головна демонстрація pandas!              ║
    ║  Кожна операція прокоментована покроково.                   ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    if not LOG_FILE.exists():
        await update.message.reply_text("Поки немає даних. Запитай погоду для будь-якого міста!")
        return

    # ── PANDAS КРОК 1: Завантажуємо CSV у DataFrame ──────────────────
    # pd.read_csv() — функція pandas, яка зчитує CSV-файл
    # і створює об'єкт DataFrame (таблицю).
    # DataFrame — це основна структура даних у pandas.
    # Уяви його як таблицю Excel: є рядки (rows) та колонки (columns).
    # Наші колонки: timestamp, user_id, username, city, temp, feels_like, description
    df = pd.read_csv(LOG_FILE)

    # ── PANDAS КРОК 2: Перевіряємо, чи DataFrame порожній ────────────
    # df.empty — це властивість (property), яка повертає True,
    # якщо у DataFrame немає жодного рядка даних.
    # Це корисно, щоб не робити обчислення на порожній таблиці.
    if df.empty:
        await update.message.reply_text("Поки немає даних. Запитай погоду для будь-якого міста!")
        return

    # ── PANDAS КРОК 3: Рахуємо кількість рядків ─────────────────────
    # len(df) — повертає кількість рядків у DataFrame.
    # Кожен рядок — це один запит погоди від користувача.
    total = len(df)

    # ── PANDAS КРОК 4: Рахуємо унікальні значення ───────────────────
    # df["city"] — вибираємо одну колонку "city" з DataFrame.
    # Результат — це Series (одновимірний масив даних pandas).
    # .nunique() — метод Series, який рахує кількість УНІКАЛЬНИХ значень.
    # Наприклад: запити Київ, Львів, Київ, Одеса → nunique() поверне 3
    cities_count = df["city"].nunique()

    # Те саме для user_id — рахуємо скільки різних користувачів було
    users_count = df["user_id"].nunique()

    # ── PANDAS КРОК 5: Підрахунок частоти значень ────────────────────
    # .value_counts() — рахує, скільки разів зустрічається кожне значення.
    # Результат автоматично відсортований від найбільшого до найменшого.
    # Наприклад: Київ — 15 разів, Львів — 8 разів, Одеса — 3 рази...
    # .head(5) — бере тільки перші 5 рядків (топ-5 найпопулярніших міст).
    popular = df["city"].value_counts().head(5)

    # ── PANDAS КРОК 6: Групування та агрегація (groupby) ─────────────
    # .groupby("city") — розбиває DataFrame на групи за містом.
    # Уяви, що ти розкладаєш картки по купках: одна купка для Києва,
    # одна для Львова, одна для Одеси...
    # ["temp"] — з кожної групи беремо тільки колонку "temp" (температура).
    # .mean() — рахуємо середнє значення температури в кожній групі.
    # .round(1) — округлюємо до 1 знаку після коми (наприклад, 15.3).
    # Результат: Series, де індекс — назва міста, значення — середня температура.
    avg_temps = df.groupby("city")["temp"].mean().round(1)

    # Збираємо текст відповіді
    text = (
        f"📊 Статистика бота\n"
        f"{'─' * 25}\n"
        f"Всього запитів: {total}\n"
        f"Унікальних міст: {cities_count}\n"
        f"Унікальних користувачів: {users_count}\n\n"
        f"🏆 Топ-5 міст:\n"
    )

    # .items() — ітерація по Series: повертає пари (значення_індексу, значення).
    # popular — це Series, де індекс = назва міста, значення = кількість запитів.
    for city, count in popular.items():
        # avg_temps.get(city, 0) — безпечно отримуємо середню температуру;
        # якщо міста немає — повертаємо 0 (працює як dict.get()).
        avg = avg_temps.get(city, 0)
        text += f"  • {city}: {count} запитів (середня {avg}°C)\n"

    # ── PANDAS КРОК 7: Агрегатні функції для числових колонок ────────
    # df['temp'] — вибираємо колонку температури (Series).
    # .min() — мінімальне значення у всій колонці.
    # .max() — максимальне значення у всій колонці.
    # .mean() — середнє арифметичне всіх значень.
    # Ці функції працюють по ВСІХ рядках одразу — це і є сила pandas!
    # Замість циклу for по тисячах рядків — один виклик .mean().
    text += (
        f"\n🌡 Температура по всіх запитах:\n"
        f"  Мін: {df['temp'].min()}°C\n"
        f"  Макс: {df['temp'].max()}°C\n"
        f"  Середня: {df['temp'].mean():.1f}°C"
    )

    await update.message.reply_text(text)


async def top_cities(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /top_cities — топ-5 міст із PostgreSQL через SQL-агрегацію.

    ╔══════════════════════════════════════════════════════════════╗
    ║  Це урок №3 (sql_groupby.py) у дії!                          ║
    ║  На відміну від /stats (pandas, читає CSV), ця команда       ║
    ║  читає з PostgreSQL через GROUP BY + ORDER BY + LIMIT.       ║
    ║                                                              ║
    ║  SQL-запит:                                                  ║
    ║      SELECT city, COUNT(*) AS query_count                    ║
    ║      FROM weather_log                                        ║
    ║      GROUP BY city                                           ║
    ║      ORDER BY query_count DESC                               ║
    ║      LIMIT 5                                                 ║
    ║                                                              ║
    ║  pandas-аналог (з /stats):                                   ║
    ║      df["city"].value_counts().head(5)                       ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    rows = get_top_cities_sql(limit=5)

    # rows == None — БД недоступна (не запущено / погані креденшіали).
    # У цьому випадку бот не падає, просто пропонує альтернативу.
    if rows is None:
        await update.message.reply_text(
            "❌ База даних недоступна. Спробуй пізніше або скористайся /stats."
        )
        return

    # rows == [] — БД працює, але таблиця порожня.
    if not rows:
        await update.message.reply_text(
            "📊 Поки немає запитів у базі. Запитай погоду для якогось міста!"
        )
        return

    # rows — це список кортежів [(city, count), ...], відсортований від більшого.
    text = "🏆 Топ-5 міст за запитами (SQL з PostgreSQL):\n\n"
    for i, (city, count) in enumerate(rows, 1):
        text += f"{i}. {city} — {count} запитів\n"
    text += "\n💡 SQL: SELECT city, COUNT(*) ... GROUP BY city ORDER BY ... LIMIT 5"

    await update.message.reply_text(text)


async def plot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Команда /plot — графік температур.

    ╔══════════════════════════════════════════════════════════════╗
    ║  Продовжуємо працювати з pandas — тепер для візуалізації!   ║
    ║  Pandas підготовлює дані, matplotlib малює графік.          ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    if not LOG_FILE.exists():
        await update.message.reply_text("Поки немає даних для графіка.")
        return

    # ── PANDAS КРОК 1: Зчитуємо CSV у DataFrame (вже знайоме!) ──────
    df = pd.read_csv(LOG_FILE)

    if len(df) < 2:
        await update.message.reply_text("Замало даних для графіка. Зроби більше запитів!")
        return

    # ── PANDAS КРОК 2: Конвертація типів — pd.to_datetime() ─────────
    # У CSV все зберігається як текст (рядки).
    # pd.to_datetime() перетворює текстову колонку на тип datetime.
    # Це потрібно, щоб matplotlib правильно розташував дати на осі X графіка.
    # Без цього "2024-03-15 14:30:00" — це просто текст, а не дата.
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Якщо вказано місто: /plot Київ
    if context.args:
        city = " ".join(context.args)

        # ── PANDAS КРОК 3: Булеве (логічне) індексування ─────────────
        # df["city"].str.lower() — бере колонку "city" і через .str accessor
        # застосовує метод .lower() до КОЖНОГО рядка (переводить у нижній регістр).
        # == city.lower() — порівнює кожне значення з шуканим містом.
        # Результат: Series з True/False для кожного рядка.
        # df[...] — фільтруємо DataFrame: залишаємо тільки рядки, де True.
        # Це як SQL: SELECT * FROM df WHERE LOWER(city) = LOWER('Київ')
        city_df = df[df["city"].str.lower() == city.lower()]

        if city_df.empty:
            await update.message.reply_text(f"Немає даних для «{city}».")
            return

        # ── PANDAS КРОК 4: Доступ за позицією — .iloc[] ─────────────
        # .iloc[0] — бере елемент за ЦІЛОЧИСЕЛЬНИМ індексом (integer location).
        # iloc[0] — перший елемент, iloc[1] — другий, iloc[-1] — останній.
        # Тут беремо назву міста з першого рядка відфільтрованих даних,
        # щоб використати оригінальний регістр (наприклад, "Київ", а не "київ").
        title = f"Температура: {city_df['city'].iloc[0]}"

        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(city_df["timestamp"], city_df["temp"], marker="o", linewidth=2, color="#2196F3")
        ax.fill_between(city_df["timestamp"], city_df["temp"], alpha=0.1, color="#2196F3")
    else:
        title = "Температура: всі міста"
        fig, ax = plt.subplots(figsize=(10, 5))

        colors = ["#2196F3", "#FF5722", "#4CAF50", "#FFC107", "#9C27B0", "#00BCD4"]

        # ── PANDAS КРОК 5: Ітерація по групах groupby ────────────────
        # df.groupby("city") — знову групуємо за містом (вже бачили у stats).
        # Але тут ми ІТЕРУЄМО по групах за допомогою enumerate().
        # Кожна ітерація дає: city_name (назва міста) та group (DataFrame з даними міста).
        # enumerate() додає лічильник i — використовуємо його для вибору кольору.
        for i, (city_name, group) in enumerate(df.groupby("city")):
            color = colors[i % len(colors)]
            ax.plot(
                group["timestamp"],  # вісь X — дати з групи
                group["temp"],       # вісь Y — температура з групи
                marker="o",
                linewidth=2,
                label=city_name,
                color=color,
            )
        ax.legend(fontsize=10)

    # Налаштовуємо зовнішній вигляд графіка (matplotlib)
    ax.set_title(title, fontsize=14, fontweight="bold")
    ax.set_xlabel("Час", fontsize=11)
    ax.set_ylabel("Температура, °C", fontsize=11)
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()   # автоматичне форматування дат на осі X
    fig.tight_layout()    # прибираємо зайві відступи

    # Зберігаємо графік у буфер пам'яті (не у файл) і відправляємо
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    buf.seek(0)      # повертаємо курсор на початок буфера
    plt.close(fig)   # закриваємо фігуру, щоб звільнити пам'ять

    await update.message.reply_photo(photo=buf, caption=title)


# ══════════════════════════════════════════════
# 4. ЗАПУСК БОТА
# ══════════════════════════════════════════════

def main():
    # Ініціалізуємо CSV-файл для логування (створюємо заголовки, якщо файлу ще немає)
    init_log()

    # Ініціалізуємо базу даних — створюємо таблицю weather_log, якщо її ще немає.
    # Якщо PostgreSQL недоступний, функція просто попередить у лог і бот запуститься без БД.
    init_db()

    # Створюємо додаток з нашим Telegram-токеном (завантаженим з .env)
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    # Реєструємо команди
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("plot", plot))
    app.add_handler(CommandHandler("top_cities", top_cities))

    # Текстові повідомлення (не команди) — обробляємо як назву міста
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, weather))

    logger.info("Бот запущений!")
    app.run_polling()  # запускаємо бот у режимі polling (опитування Telegram серверів)


if __name__ == "__main__":
    main()
