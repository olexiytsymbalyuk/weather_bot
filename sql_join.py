"""
SQL JOIN — З'єднання таблиць на прикладі погодних даних
========================================================
Четвертий урок SQL для курсу програмування.
Продовжує sql_basics.py та sql_groupby.py: тепер у нас не одна таблиця,
а три (weather_log, users, cities) — і ми вчимося з'єднувати їх через JOIN.

Перевага цього уроку: JOIN у SQL — це майже дослівний переклад
pd.merge(...) у pandas. Той самий принцип, інша мова.
"""

# ╔══════════════════════════════════════════════════════════════════╗
# ║                         ІМПОРТИ                                ║
# ╚══════════════════════════════════════════════════════════════════╝

import os                  # Для роботи зі змінними середовища
from datetime import datetime, date  # Для роботи з датами та часом

import pandas as pd        # Знадобиться в КРОЦІ 7 — pd.merge() як pandas-аналог JOIN
import psycopg2            # Бібліотека для підключення до PostgreSQL
from dotenv import load_dotenv  # Завантаження змінних з .env файлу

# ╔══════════════════════════════════════════════════════════════════╗
# ║                      НАЛАШТУВАННЯ                              ║
# ╚══════════════════════════════════════════════════════════════════╝

load_dotenv()

DB_NAME = os.getenv("DB_NAME", "weather_db")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                  ПІДКЛЮЧЕННЯ ДО БАЗИ ДАНИХ                     ║
# ╚══════════════════════════════════════════════════════════════════╝

def connect_db():
    """Створює підключення до PostgreSQL та повертає об'єкт з'єднання."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    # autocommit = True — кожен SQL виконується одразу.
    conn.autocommit = True
    print("✅ Підключення до бази даних встановлено!\n")
    return conn


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ДОПОМІЖНА ФУНКЦІЯ: Красивий вивід результатів запиту          ║
# ╚══════════════════════════════════════════════════════════════════╝

def print_results(cursor, title="Результат"):
    """Виводить результати SQL-запиту у вигляді таблиці."""
    rows = cursor.fetchall()
    if not rows:
        print(f"  {title}: (порожній результат)\n")
        return

    col_names = [desc[0] for desc in cursor.description]

    print(f"  {title}:")
    print(f"  {' | '.join(col_names)}")
    print(f"  {'─' * 60}")
    for row in rows:
        # str(val) перетворює None у "None" — у JOIN це часто й хочемо побачити.
        print(f"  {' | '.join(str(val) for val in row)}")
    print()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ПІДГОТОВКА ДАНИХ — створюємо ТРИ таблиці та наповнюємо їх    ║
# ╚══════════════════════════════════════════════════════════════════╝

def prepare_data(cur):
    """
    Створює таблиці weather_log, users та cities і вставляє тестові дані.

    Ідея:
      • weather_log — точно та сама схема, що в sql_basics.py і sql_groupby.py.
                      Зберігаємо username та city як рядки (денормалізовано).
      • users       — окрема довідкова таблиця ПРО ЮЗЕРІВ.
                      Її НЕ було в попередніх уроках. Поле home_city
                      дозволяє нам розповісти юзерові щось більше
                      за самі лише запити в weather_log.
      • cities      — окрема довідкова таблиця ПРО МІСТА.
                      Країна, регіон, координати — те, чого немає у weather_log.

    Зв'язки (які й даватимуть нам поле для JOIN):
        weather_log.user_id  ↔  users.user_id
        weather_log.city     ↔  cities.name
    """
    print("═" * 60)
    print("ПІДГОТОВКА: створюємо ТРИ таблиці та наповнюємо їх даними")
    print("═" * 60)

    # ── weather_log: ідентичний sql_groupby.py ──────────────────────────
    # DROP + CREATE — кожен запуск починає з чистого аркуша. У реальному
    # проєкті так НЕ роблять (видалить дані!), але для уроку — зручно.
    cur.execute("DROP TABLE IF EXISTS weather_log")
    cur.execute("""
        CREATE TABLE weather_log (
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

    # 16 записів — той самий набір, що в sql_groupby.py, для безперервності.
    weather_data = [
        (datetime(2026, 4, 8,  9, 15), 1001, "олена_к",  "Київ",    12.5, 10.8, "хмарно"),
        (datetime(2026, 4, 8,  9, 30), 1002, "максим_п", "Львів",   10.2,  8.5, "невеликий дощ"),
        (datetime(2026, 4, 8, 10,  0), 1001, "олена_к",  "Одеса",   15.7, 14.9, "ясне небо"),
        (datetime(2026, 4, 8, 10, 45), 1003, "ірина_с",  "Київ",    13.1, 11.2, "хмарно"),
        (datetime(2026, 4, 8, 11,  0), 1002, "максим_п", "Харків",  11.8,  9.6, "невеликий дощ"),
        (datetime(2026, 4, 8, 12, 30), 1004, "андрій_м", "Вінниця", 14.0, 12.3, "мінлива хмарність"),
        (datetime(2026, 4, 8, 13,  0), 1001, "олена_к",  "Київ",    14.5, 13.0, "мінлива хмарність"),
        (datetime(2026, 4, 8, 14,  0), 1005, "даша_л",   "Одеса",   16.2, 15.5, "ясне небо"),
        (datetime(2026, 4, 8, 14, 30), 1003, "ірина_с",  "Львів",   11.0,  9.2, "хмарно"),
        (datetime(2026, 4, 8, 15,  0), 1004, "андрій_м", "Київ",    14.8, 13.5, "мінлива хмарність"),
        (datetime(2026, 4, 8, 16,  0), 1002, "максим_п", "Дніпро",  13.5, 11.8, "хмарно"),
        (datetime(2026, 4, 8, 17,  0), 1005, "даша_л",   "Харків",  11.2,  9.0, "невеликий дощ"),
        (datetime(2026, 4, 8, 17, 30), 1001, "олена_к",  "Київ",    13.8, 12.4, "хмарно"),
        (datetime(2026, 4, 8, 18,  0), 1003, "ірина_с",  "Дніпро",  14.2, 12.7, "мінлива хмарність"),
        (datetime(2026, 4, 8, 18, 30), 1004, "андрій_м", "Одеса",   15.0, 14.0, "ясне небо"),
        (datetime(2026, 4, 8, 19,  0), 1002, "максим_п", "Київ",    12.0, 10.5, "хмарно"),
    ]
    for row in weather_data:
        cur.execute(
            """INSERT INTO weather_log
               (timestamp, user_id, username, city, temp, feels_like, description)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            row,
        )

    # ── users: ДОВІДНИК ЮЗЕРІВ ──────────────────────────────────────────
    # Зверни увагу на спеціальні випадки в даних:
    #   • ірина_с має home_city = NULL  → демо обробки NULL у JOIN.
    #   • оля_к — є в users, але немає жодного запиту у weather_log
    #             → LEFT JOIN покаже її; INNER JOIN — ні.
    cur.execute("DROP TABLE IF EXISTS users")
    cur.execute("""
        CREATE TABLE users (
            user_id     INTEGER PRIMARY KEY,
            username    VARCHAR(100) NOT NULL,
            first_seen  DATE NOT NULL,
            language    VARCHAR(10) NOT NULL,
            home_city   VARCHAR(100)
        )
    """)
    users_data = [
        (1001, "олена_к",  date(2026, 3, 15), "uk", "Київ"),
        (1002, "максим_п", date(2026, 3, 20), "uk", "Львів"),
        (1003, "ірина_с",  date(2026, 3, 22), "uk", None),       # home_city = NULL
        (1004, "андрій_м", date(2026, 3, 25), "uk", "Вінниця"),
        (1005, "даша_л",   date(2026, 4,  1), "en", "Одеса"),
        (1006, "оля_к",    date(2026, 4,  5), "uk", "Київ"),     # без запитів
    ]
    for row in users_data:
        cur.execute(
            "INSERT INTO users (user_id, username, first_seen, language, home_city) "
            "VALUES (%s, %s, %s, %s, %s)",
            row,
        )

    # ── cities: ДОВІДНИК МІСТ ───────────────────────────────────────────
    # Полтава та Чернівці у нас є в довіднику, але їх НЕ запитували.
    # Це знадобиться для RIGHT JOIN / FULL OUTER JOIN — побачимо "сирітські" міста.
    cur.execute("DROP TABLE IF EXISTS cities")
    cur.execute("""
        CREATE TABLE cities (
            name        VARCHAR(100) PRIMARY KEY,
            country     VARCHAR(50) NOT NULL,
            region      VARCHAR(100),
            population  INTEGER,
            latitude    REAL,
            longitude   REAL
        )
    """)
    cities_data = [
        ("Київ",     "Україна", "Київська",         2884000, 50.45, 30.52),
        ("Львів",    "Україна", "Львівська",         717000, 49.84, 24.03),
        ("Одеса",    "Україна", "Одеська",          1011000, 46.48, 30.73),
        ("Харків",   "Україна", "Харківська",       1421000, 49.99, 36.23),
        ("Вінниця",  "Україна", "Вінницька",         370000, 49.23, 28.47),
        ("Дніпро",   "Україна", "Дніпропетровська",  980000, 48.46, 35.04),
        ("Полтава",  "Україна", "Полтавська",        280000, 49.59, 34.55),  # без запитів
        ("Чернівці", "Україна", "Чернівецька",       264000, 48.29, 25.94),  # без запитів
    ]
    for row in cities_data:
        cur.execute(
            "INSERT INTO cities (name, country, region, population, latitude, longitude) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            row,
        )

    print(f"✅ weather_log: {len(weather_data)} записів")
    print(f"✅ users:       {len(users_data)} рядків (1 без запитів, 1 без home_city)")
    print(f"✅ cities:      {len(cities_data)} рядків (2 без запитів)\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 1: INNER JOIN — основа основ                       ║
# ══════════════════════════════════════════════════════════════════

def step1_inner_join(cur):
    """
    INNER JOIN з'єднує дві таблиці й залишає ТІЛЬКИ ті рядки, де є збіг.

    Синтаксис:
        SELECT ...
        FROM   ліва_таблиця
        INNER JOIN права_таблиця
            ON ліва.колонка = права.колонка;

    Слово INNER можна не писати — просто JOIN означає те саме.
    "ON ..." — це УМОВА З'ЄДНАННЯ: рядки беруться разом, коли вона True.

    Порівняй з pandas: pd.merge(left, right, on="key", how="inner")
                        або просто pd.merge(left, right, on="key") — inner за замовчуванням.
    """
    print("═" * 60)
    print("SQL КРОК 1: INNER JOIN — тільки збіги")
    print("═" * 60)

    # ── 1.1: Простий INNER JOIN weather_log ↔ users ─────────────────
    # Для кожного запиту погоди підтягуємо мову юзера та його home_city.
    # У weather_log є user_id; у users є user_id — це й буде наш "ключ" JOIN-у.
    cur.execute("""
        SELECT w.timestamp, w.username, w.city, w.temp, u.language, u.home_city
        FROM weather_log AS w
        INNER JOIN users AS u
            ON w.user_id = u.user_id
        ORDER BY w.timestamp
        LIMIT 5
    """)
    print_results(cur, "1.1 weather_log JOIN users (перші 5 запитів з мовою юзера)")

    # 💡 Зверни увагу на AS w / AS u — це псевдоніми (aliases) таблиць.
    # Без них треба було б писати weather_log.timestamp щоразу. З ними — w.timestamp.

    # ── 1.2: INNER JOIN weather_log ↔ cities ────────────────────────
    # Збагачуємо запит координатами та населенням міста.
    cur.execute("""
        SELECT w.username, w.city, w.temp, c.country, c.region, c.population
        FROM weather_log AS w
        JOIN cities AS c
            ON w.city = c.name
        ORDER BY w.timestamp
        LIMIT 5
    """)
    print_results(cur, "1.2 weather_log JOIN cities (з регіоном та населенням)")

    # ── 1.3: Колізія імен колонок ───────────────────────────────────
    # У users є колонка username, у weather_log — теж username.
    # Якщо написати просто "SELECT username", SQL не знає, з якої таблиці брати.
    # Розв'язання: завжди уточнюємо джерело через alias (w.username, u.username).
    cur.execute("""
        SELECT w.username AS wl_name, u.username AS u_name, u.first_seen
        FROM weather_log AS w
        JOIN users AS u
            ON w.user_id = u.user_id
        ORDER BY u.first_seen
        LIMIT 4
    """)
    print_results(cur, "1.3 Колізія username — розв'язано через alias-и")

    # ── 1.4: Скільки запитів зробив юзер ірина_с? ───────────────────
    # JOIN + WHERE: спочатку з'єднуємо, потім фільтруємо.
    # Порівняй з pandas:
    #   merged = pd.merge(wl, users, on="user_id")
    #   merged[merged["username_y"] == "ірина_с"]
    cur.execute("""
        SELECT u.username, COUNT(*) AS requests
        FROM weather_log AS w
        JOIN users AS u
            ON w.user_id = u.user_id
        WHERE u.username = 'ірина_с'
        GROUP BY u.username
    """)
    print_results(cur, "1.4 JOIN + WHERE + GROUP BY — скільки запитів у ірини_с")

    print("  💡 Запам'ятай: INNER JOIN = перетин. Немає пари → рядок зникає.")
    print("                  Усі юзери weather_log потрапили (бо є в users).")
    print("                  Юзер 'оля_к' не з'явився — у неї немає жодного запиту.\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 2: LEFT JOIN — "усі ліві, навіть без пари"          ║
# ══════════════════════════════════════════════════════════════════

def step2_left_join(cur):
    """
    LEFT JOIN (LEFT OUTER JOIN) — беремо ВСІ рядки з лівої таблиці,
    а з правої підтягуємо те, що збіглось. Якщо збігу немає — NULL.

    Це найкорисніший варіант, коли треба запитання типу:
      "Покажи мені всіх юзерів, навіть тих, хто нічого не зробив."

    Порівняй з pandas: pd.merge(left, right, on="key", how="left")
    """
    print("═" * 60)
    print("SQL КРОК 2: LEFT JOIN — усі ліві рядки, навіть без пари справа")
    print("═" * 60)

    # ── 2.1: Усі юзери + їхні запити (якщо є) ───────────────────────
    # Зверни увагу: оля_к (user_id=1006) у нас в users є, але запитів нема.
    # У INNER JOIN її не побачимо. У LEFT JOIN — побачимо, з NULL у колонках weather_log.
    cur.execute("""
        SELECT u.user_id, u.username, w.city, w.temp
        FROM users AS u
        LEFT JOIN weather_log AS w
            ON u.user_id = w.user_id
        ORDER BY u.user_id, w.timestamp
    """)
    print_results(cur, "2.1 users LEFT JOIN weather_log (оля_к → NULL у city/temp)")

    # ── 2.2: Знайти юзерів, які НЕ зробили жодного запиту ───────────
    # Класичний трюк: LEFT JOIN + WHERE правий_ключ IS NULL.
    # Якщо для юзера правий ключ = NULL, значить пари не знайшлось → запитів немає.
    # Порівняй з pandas:
    #   m = pd.merge(users, wl, on="user_id", how="left", indicator=True)
    #   m[m["_merge"] == "left_only"]
    cur.execute("""
        SELECT u.user_id, u.username, u.first_seen
        FROM users AS u
        LEFT JOIN weather_log AS w
            ON u.user_id = w.user_id
        WHERE w.user_id IS NULL
    """)
    print_results(cur, "2.2 LEFT JOIN + IS NULL → юзери без запитів")

    # ── 2.3: Кількість запитів по кожному юзеру (включно з нулями) ──
    # COUNT(колонка) ігнорує NULL — тому юзери без запитів отримають 0,
    # а не зникнуть. (А COUNT(*) НАВПАКИ — рахував би 1 для NULL-рядка.)
    cur.execute("""
        SELECT u.username, COUNT(w.id) AS requests
        FROM users AS u
        LEFT JOIN weather_log AS w
            ON u.user_id = w.user_id
        GROUP BY u.user_id, u.username
        ORDER BY requests DESC
    """)
    print_results(cur, "2.3 LEFT JOIN + GROUP BY — запитів у кожного (з нулями!)")

    # ── 2.4: Міста, які ніхто НЕ запитував ──────────────────────────
    # Та сама ідея, але повертаємо її від cities (Полтава, Чернівці).
    cur.execute("""
        SELECT c.name, c.region, c.population
        FROM cities AS c
        LEFT JOIN weather_log AS w
            ON c.name = w.city
        WHERE w.id IS NULL
    """)
    print_results(cur, "2.4 Міста з довідника, які жодного разу не запитували")

    print("  💡 LEFT JOIN — твій інструмент для запитів 'усі X, навіть без Y'.")
    print("                  Перевернути LEFT на RIGHT — це просто переставити таблиці.\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 3: RIGHT JOIN та FULL OUTER JOIN                    ║
# ══════════════════════════════════════════════════════════════════

def step3_right_full_outer(cur):
    """
    RIGHT JOIN — дзеркало LEFT JOIN: беремо всі рядки з ПРАВОЇ таблиці,
    зліва підтягуємо те, що збіглось. У реальному коді його використовують
    дуже рідко — простіше переставити таблиці й написати LEFT.

    FULL OUTER JOIN — беремо ВСЕ з обох сторін.
    Те, чому немає пари, лишається з NULL відповідно зліва або справа.

    Венн-діаграма (хто куди потрапляє):
        ┌─ LEFT JOIN ───────────────────┐
        │  ●●●●●● ●●●  (усі ліві)       │
        │       ●●● (тільки збіги справа)│
        └────────────────────────────────┘
        ┌─ RIGHT JOIN ──────────────────┐
        │       ●●● (тільки збіги зліва)│
        │  ●●●●●● ●●●  (усі праві)      │
        └────────────────────────────────┘
        ┌─ FULL OUTER JOIN ─────────────┐
        │  ●●●●●● ●●● ●●●●● (усі звідусіль)│
        └────────────────────────────────┘
    """
    print("═" * 60)
    print("SQL КРОК 3: RIGHT JOIN та FULL OUTER JOIN")
    print("═" * 60)

    # ── 3.1: RIGHT JOIN — те саме, що 2.1, але "з іншого боку" ──────
    # Тут weather_log ліворуч, users праворуч. RIGHT JOIN бере все з users.
    # Результат ідентичний 2.1 — просто інша форма запису.
    cur.execute("""
        SELECT u.user_id, u.username, w.city, w.temp
        FROM weather_log AS w
        RIGHT JOIN users AS u
            ON w.user_id = u.user_id
        ORDER BY u.user_id, w.timestamp
    """)
    print_results(cur, "3.1 weather_log RIGHT JOIN users (= users LEFT JOIN weather_log)")

    # 💡 На практиці RIGHT JOIN зустрічається рідко: майже завжди можна
    #    переставити таблиці й написати LEFT JOIN — читається легше.

    # ── 3.2: FULL OUTER JOIN — усі рядки з обох сторін ──────────────
    # Беремо ВСІ міста з cities і ВСІ міста з weather_log, з'єднуємо за назвою.
    # Сирітські міста (Полтава, Чернівці) дадуть NULL у weather_log-колонках.
    # А якби у weather_log було місто, якого нема в cities — у c.country був би NULL.
    # У нашому наборі такого нема: усі міста weather_log є в cities.
    cur.execute("""
        SELECT c.name, c.region, COUNT(w.id) AS requests
        FROM cities AS c
        FULL OUTER JOIN weather_log AS w
            ON c.name = w.city
        GROUP BY c.name, c.region
        ORDER BY requests DESC, c.name
    """)
    print_results(cur, "3.2 cities FULL OUTER JOIN weather_log — усі міста з кількістю")

    # ── 3.3: FULL OUTER JOIN, де NULL з'являється з ОБОХ боків ──────
    # Уявімо синтетичну ситуацію: у weather_log є місто без запису в cities.
    # Тут такого нема (бо ми все підготували), але FULL OUTER чесно покаже:
    #   • Полтава / Чернівці   → c.name є, w.id IS NULL (місто-сирітка)
    #   • (нічого зліва)        → c.name IS NULL, w.id є (запит без довідника)
    cur.execute("""
        SELECT c.name AS city_in_dir, w.city AS city_in_log
        FROM cities AS c
        FULL OUTER JOIN weather_log AS w
            ON c.name = w.city
        WHERE c.name IS NULL OR w.city IS NULL
        ORDER BY c.name NULLS LAST
        LIMIT 10
    """)
    print_results(cur, "3.3 FULL OUTER + IS NULL → рядки 'без пари' з будь-якого боку")

    print("  💡 RIGHT JOIN — рідкісний у реальному коді (легко замінити на LEFT).")
    print("                  FULL OUTER — для синхронізації двох списків:")
    print("                  'що є тут, чого немає там, що співпадає'.\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 4: JOIN трьох таблиць (+ GROUP BY)                  ║
# ══════════════════════════════════════════════════════════════════

def step4_three_tables(cur):
    """
    JOIN не обмежується двома таблицями. Можна ланцюжком приєднувати скільки треба.
    Логіка проста: спочатку з'єднали A з B, далі результат з'єднуємо з C.

    Тут поєднаємо все, що вивчили: JOIN ↔ JOIN ↔ JOIN + GROUP BY (з уроку 3).
    """
    print("═" * 60)
    print("SQL КРОК 4: JOIN трьох таблиць + GROUP BY")
    print("═" * 60)

    # ── 4.1: Кожен запит з мовою юзера ТА регіоном міста ────────────
    # Три таблиці, два JOIN-и.
    cur.execute("""
        SELECT w.timestamp, u.username, u.language, w.city, c.region, w.temp
        FROM weather_log AS w
        JOIN users  AS u ON w.user_id = u.user_id
        JOIN cities AS c ON w.city    = c.name
        ORDER BY w.timestamp
        LIMIT 6
    """)
    print_results(cur, "4.1 weather_log + users + cities — повна картина запиту")

    # ── 4.2: Топ-3 регіони за кількістю запитів ─────────────────────
    # Тут JOIN дає нам колонку region, яку ми б не мали тільки з weather_log.
    # GROUP BY region — рахуємо запити в розрізі регіонів.
    # Порівняй з pandas:
    #   m = pd.merge(wl, cities, left_on="city", right_on="name")
    #   m.groupby("region").size().sort_values(ascending=False).head(3)
    cur.execute("""
        SELECT c.region, COUNT(*) AS requests
        FROM weather_log AS w
        JOIN cities AS c ON w.city = c.name
        GROUP BY c.region
        ORDER BY requests DESC
        LIMIT 3
    """)
    print_results(cur, "4.2 Топ-3 регіони за кількістю запитів (через JOIN)")

    # ── 4.3: Активність юзерів у розрізі мови ───────────────────────
    # GROUP BY мовою → бачимо, що 'en'-юзерів менше, але вони активні.
    cur.execute("""
        SELECT u.language, COUNT(*) AS requests, COUNT(DISTINCT u.user_id) AS users
        FROM weather_log AS w
        JOIN users AS u ON w.user_id = u.user_id
        GROUP BY u.language
        ORDER BY requests DESC
    """)
    print_results(cur, "4.3 Активність по мовах (LEFT-частина рахує, JOIN підтягує мову)")

    # ── 4.4: Запити НЕ зі свого home_city ───────────────────────────
    # Це класичний "цікавий" запит, який можна зробити лише через JOIN:
    # знайти випадки, коли юзер дивиться погоду НЕ у місті, де живе.
    # Звертай увагу на u.home_city IS DISTINCT FROM w.city —
    # це версія "!=", яка коректно працює з NULL (звичайне != з NULL дає NULL).
    cur.execute("""
        SELECT u.username, u.home_city, w.city, w.temp
        FROM weather_log AS w
        JOIN users AS u ON w.user_id = u.user_id
        WHERE u.home_city IS DISTINCT FROM w.city
        ORDER BY u.username, w.timestamp
    """)
    print_results(cur, "4.4 Запити НЕ зі свого home_city (IS DISTINCT FROM — NULL-safe)")

    print("  💡 Три таблиці, два JOIN-и, один GROUP BY — і ми відповідаємо")
    print("     на питання, на які з одної таблиці ніяк не відповісти.\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 5: Self JOIN — таблиця сама з собою                 ║
# ══════════════════════════════════════════════════════════════════

def step5_self_join(cur):
    """
    Self JOIN — JOIN таблиці САМОЇ З СОБОЮ.
    Звучить дивно, але це звичайний JOIN: просто з обох боків — той самий weather_log.
    Щоб SQL не плутався, обов'язково даємо різні псевдоніми (a, b).

    Корисно, щоб знаходити ПАРИ рядків усередині однієї таблиці:
      "знайди двох юзерів, які запитували те саме місто",
      "знайди різні запити того ж юзера" тощо.
    """
    print("═" * 60)
    print("SQL КРОК 5: Self JOIN — таблиця сама з собою")
    print("═" * 60)

    # ── 5.1: Пари різних юзерів, які дивилися ОДНЕ Й ТЕ САМЕ місто ──
    # a.user_id < b.user_id — щоб одна пара не з'явилась двічі (А-Б і Б-А)
    # і ми не з'єднали юзера сам з собою.
    cur.execute("""
        SELECT a.username AS user_a, b.username AS user_b, a.city
        FROM weather_log AS a
        JOIN weather_log AS b
            ON a.city = b.city
           AND a.user_id < b.user_id
        GROUP BY a.username, b.username, a.city
        ORDER BY a.city, user_a, user_b
    """)
    print_results(cur, "5.1 Пари різних юзерів, які дивилися ОДНЕ і те саме місто")

    # ── 5.2: Той самий юзер, два РІЗНІ міста за один день ───────────
    # Self JOIN по user_id, з умовою a.city < b.city — щоб пари були унікальні.
    cur.execute("""
        SELECT a.username, a.city AS city_a, b.city AS city_b
        FROM weather_log AS a
        JOIN weather_log AS b
            ON a.user_id = b.user_id
           AND a.city < b.city
        GROUP BY a.username, a.city, b.city
        ORDER BY a.username, city_a, city_b
    """)
    print_results(cur, "5.2 Той самий юзер дивився пару різних міст")

    print("  💡 Self JOIN — твій інструмент для запитів про ПАРИ в одній таблиці.")
    print("                  Завжди: різні alias-и + умова, що відкидає дзеркальні пари.\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 6: Pandas-міст — pd.merge() як аналог JOIN          ║
# ══════════════════════════════════════════════════════════════════

def step6_pandas_bridge(conn):
    """
    Те ж саме мислення, інша мова. У pandas з'єднання таблиць — це pd.merge().

    Параметр how= керує типом JOIN-у:
        how="inner"  → INNER JOIN
        how="left"   → LEFT JOIN
        how="right"  → RIGHT JOIN
        how="outer"  → FULL OUTER JOIN

    Параметри on= / left_on= / right_on= — це аналог ON у SQL.
    """
    print("═" * 60)
    print("SQL КРОК 6: Pandas-міст — pd.read_sql + pd.merge")
    print("═" * 60)

    # ── 6.1: Завантажуємо всі три таблиці у pandas одним викликом ───
    # pd.read_sql_query(sql, conn) виконує SQL і повертає DataFrame.
    wl = pd.read_sql_query("SELECT * FROM weather_log", conn)
    users = pd.read_sql_query("SELECT * FROM users", conn)
    cities = pd.read_sql_query("SELECT * FROM cities", conn)

    print(f"  weather_log → DataFrame {wl.shape}")
    print(f"  users       → DataFrame {users.shape}")
    print(f"  cities      → DataFrame {cities.shape}\n")

    # ── 6.2: INNER JOIN у pandas ────────────────────────────────────
    # SQL:   weather_log JOIN users USING (user_id)
    # У pandas обидві таблиці мають username — pandas автоматично перейменує
    # колізії в username_x (з лівої) та username_y (з правої).
    m_inner = pd.merge(wl, users, on="user_id", how="inner")
    print(f"  pd.merge(wl, users, on='user_id', how='inner') → {m_inner.shape}")
    print(f"    Колонки: {list(m_inner.columns)}\n")

    # ── 6.3: LEFT JOIN у pandas — knock-out тих, кого нема в weather_log ──
    # SQL:   users LEFT JOIN weather_log USING (user_id) WHERE w.user_id IS NULL
    # У pandas використовуємо indicator=True — додасть колонку _merge зі
    # значеннями 'both', 'left_only', 'right_only'.
    m_left = pd.merge(users, wl, on="user_id", how="left", indicator=True)
    only_users = m_left[m_left["_merge"] == "left_only"][["user_id", "username_x", "first_seen"]]
    print(f"  pd.merge(users, wl, how='left', indicator=True)")
    print(f"    Юзери без запитів (left_only):")
    if only_users.empty:
        print("      (порожньо)")
    else:
        for _, row in only_users.iterrows():
            print(f"      {row['user_id']} | {row['username_x']} | {row['first_seen']}")
    print()

    # ── 6.4: Три таблиці у pandas — два merge поспіль ───────────────
    # SQL: weather_log JOIN users JOIN cities (= крок 4.1)
    full = pd.merge(wl, users, on="user_id", how="inner")
    full = pd.merge(full, cities, left_on="city", right_on="name", how="inner")

    # GROUP BY region COUNT(*) — той самий запит, що в 4.2.
    top_regions = full.groupby("region").size().sort_values(ascending=False).head(3)
    print(f"  pandas: топ-3 регіони за запитами (= SQL 4.2):")
    for region, n in top_regions.items():
        print(f"      {region:25s} | {n}")
    print()

    print("  💡 Підсумок: SQL JOIN та pd.merge — це одна й та сама ідея.")
    print("     Вибір залежить від контексту: дані вже в БД → SQL швидший,")
    print("     дані вже в DataFrame → pd.merge зручніший.\n")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                       ГОЛОВНА ФУНКЦІЯ                          ║
# ╚══════════════════════════════════════════════════════════════════╝

def main():
    """Запускає всі кроки уроку послідовно."""
    print("\n🔗 SQL JOIN — З'єднання таблиць")
    print("=" * 60)
    print()

    conn = connect_db()
    cur = conn.cursor()

    try:
        prepare_data(cur)            # створюємо 3 таблиці та наповнюємо
        step1_inner_join(cur)        # КРОК 1: INNER JOIN
        step2_left_join(cur)         # КРОК 2: LEFT JOIN
        step3_right_full_outer(cur)  # КРОК 3: RIGHT та FULL OUTER JOIN
        step4_three_tables(cur)      # КРОК 4: JOIN трьох таблиць + GROUP BY
        step5_self_join(cur)         # КРОК 5: Self JOIN
        step6_pandas_bridge(conn)    # КРОК 6: pd.merge() — pandas-аналог JOIN

        print("=" * 60)
        print("🎉 Усі кроки виконано! Тепер ти вмієш з'єднувати таблиці.")
        print("👉 Подивись команду /me у bot.py — це JOIN трьох таблиць у боті.")
        print("=" * 60)

    finally:
        # Завжди закриваємо курсор та з'єднання — навіть якщо була помилка.
        cur.close()
        conn.close()
        print("\n🔒 З'єднання з базою даних закрито.")


# Стандартний Python-патерн: код виконується тільки якщо файл запущено напряму.
if __name__ == "__main__":
    main()
