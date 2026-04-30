"""
SQL GROUP BY — Агрегації та групування на прикладі погодних даних
==================================================================
Третій урок SQL для курсу програмування.
Продовжує sql_basics.py: тепер вчимося зводити багато рядків
в одну цифру (агрегатні функції) та рахувати окремо по кожній групі (GROUP BY).

Перевага цього уроку: GROUP BY у SQL — це майже дослівний переклад
df.groupby(...) у pandas, з яким ви вже знайомі з уроку №1 (bot.py / stats).
"""

# ╔══════════════════════════════════════════════════════════════════╗
# ║                         ІМПОРТИ                                ║
# ╚══════════════════════════════════════════════════════════════════╝

import os                  # Для роботи зі змінними середовища
from datetime import datetime  # Для роботи з датами

import psycopg2            # Бібліотека для підключення до PostgreSQL
from dotenv import load_dotenv  # Завантаження змінних з .env файлу

# ╔══════════════════════════════════════════════════════════════════╗
# ║                      НАЛАШТУВАННЯ                              ║
# ╚══════════════════════════════════════════════════════════════════╝

# Завантажуємо змінні середовища з файлу .env (так само, як у sql_basics.py).
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
    # autocommit = True — кожен SQL виконується одразу, як у sql_basics.py.
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

    # cursor.description[i][0] — назва i-ї колонки результату.
    col_names = [desc[0] for desc in cursor.description]

    print(f"  {title}:")
    print(f"  {' | '.join(col_names)}")
    print(f"  {'─' * 60}")
    for row in rows:
        print(f"  {' | '.join(str(val) for val in row)}")
    print()


# ╔══════════════════════════════════════════════════════════════════╗
# ║  ПІДГОТОВКА ДАНИХ — створюємо таблицю та наповнюємо її         ║
# ╚══════════════════════════════════════════════════════════════════╝

def prepare_data(cur):
    """
    Створює таблицю weather_log і вставляє тестові дані.

    Перші 12 записів — ті ж самі, що у sql_basics.py (для безперервності уроку).
    Ще 4 додаткові записи додаємо, щоб приклади з HAVING давали ширші результати.
    """
    print("═" * 60)
    print("ПІДГОТОВКА: створюємо таблицю та наповнюємо її даними")
    print("═" * 60)

    # DROP + CREATE — кожен запуск починає з чистого аркуша.
    # ⚠️  В реальному проєкті так НЕ роблять — це видалить усі дані!
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

    # 16 тестових записів. Перші 12 — як у sql_basics.py.
    # Ще 4 — щоб у GROUP BY було більше різноманіття: Київ отримує ще 1 запит
    # (стає 5 разів), Дніпро та Вінниця — другий запит (щоб HAVING COUNT(*) >= 2
    # відсіяв їх по-різному), і ще одна Одеса.
    sample_data = [
        # ── оригінальні 12 з sql_basics.py ──────────────────────────
        (datetime(2026, 4, 8, 9, 15), 1001, "олена_к", "Київ", 12.5, 10.8, "хмарно"),
        (datetime(2026, 4, 8, 9, 30), 1002, "максим_п", "Львів", 10.2, 8.5, "невеликий дощ"),
        (datetime(2026, 4, 8, 10, 0), 1001, "олена_к", "Одеса", 15.7, 14.9, "ясне небо"),
        (datetime(2026, 4, 8, 10, 45), 1003, "ірина_с", "Київ", 13.1, 11.2, "хмарно"),
        (datetime(2026, 4, 8, 11, 0), 1002, "максим_п", "Харків", 11.8, 9.6, "невеликий дощ"),
        (datetime(2026, 4, 8, 12, 30), 1004, "андрій_м", "Вінниця", 14.0, 12.3, "мінлива хмарність"),
        (datetime(2026, 4, 8, 13, 0), 1001, "олена_к", "Київ", 14.5, 13.0, "мінлива хмарність"),
        (datetime(2026, 4, 8, 14, 0), 1005, "даша_л", "Одеса", 16.2, 15.5, "ясне небо"),
        (datetime(2026, 4, 8, 14, 30), 1003, "ірина_с", "Львів", 11.0, 9.2, "хмарно"),
        (datetime(2026, 4, 8, 15, 0), 1004, "андрій_м", "Київ", 14.8, 13.5, "мінлива хмарність"),
        (datetime(2026, 4, 8, 16, 0), 1002, "максим_п", "Дніпро", 13.5, 11.8, "хмарно"),
        (datetime(2026, 4, 8, 17, 0), 1005, "даша_л", "Харків", 11.2, 9.0, "невеликий дощ"),
        # ── 4 додаткові записи для багатшого GROUP BY/HAVING ─────────
        (datetime(2026, 4, 8, 17, 30), 1001, "олена_к", "Київ", 13.8, 12.4, "хмарно"),
        (datetime(2026, 4, 8, 18, 0), 1003, "ірина_с", "Дніпро", 14.2, 12.7, "мінлива хмарність"),
        (datetime(2026, 4, 8, 18, 30), 1004, "андрій_м", "Одеса", 15.0, 14.0, "ясне небо"),
        (datetime(2026, 4, 8, 19, 0), 1002, "максим_п", "Київ", 12.0, 10.5, "хмарно"),
    ]

    insert_query = """
        INSERT INTO weather_log (timestamp, user_id, username, city, temp, feels_like, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    for row in sample_data:
        cur.execute(insert_query, row)

    print(f"✅ Створено таблицю та додано {len(sample_data)} записів.\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 1: Агрегатні функції — N рядків → 1 цифра          ║
# ══════════════════════════════════════════════════════════════════

def aggregates_basics(cur):
    """
    Агрегатні функції згортають багато рядків в одне число.

    Основні агрегати в SQL:
      COUNT(*)     — скільки рядків (ігнорує умови, рахує все).
      COUNT(колонка) — скільки рядків, де колонка не NULL.
      COUNT(DISTINCT колонка) — скільки УНІКАЛЬНИХ значень.
      AVG(числова) — середнє арифметичне.
      MIN(числова) — мінімум.
      MAX(числова) — максимум.
      SUM(числова) — сума.

    Усі ці функції є і в pandas — назви майже однакові (len, mean, min, max, sum).
    """
    print("═" * 60)
    print("SQL КРОК 1: Агрегатні функції (без GROUP BY)")
    print("═" * 60)

    # ── 1.1: COUNT(*) — скільки всього записів ───────────────────
    # Порівняй з pandas: len(df)
    cur.execute("SELECT COUNT(*) FROM weather_log")
    print_results(cur, "COUNT(*) — загальна кількість записів")

    # ── 1.2: COUNT(DISTINCT ...) — скільки унікальних значень ────
    # DISTINCT всередині COUNT — рахує лише різні значення.
    # Порівняй з pandas: df["city"].nunique()
    cur.execute("SELECT COUNT(DISTINCT city) FROM weather_log")
    print_results(cur, "COUNT(DISTINCT city) — унікальних міст")

    # Те саме для користувачів.
    # Порівняй з pandas: df["user_id"].nunique()
    cur.execute("SELECT COUNT(DISTINCT user_id) FROM weather_log")
    print_results(cur, "COUNT(DISTINCT user_id) — унікальних користувачів")

    # ── 1.3: AVG — середнє значення ──────────────────────────────
    # Порівняй з pandas: df["temp"].mean()
    cur.execute("SELECT AVG(temp) FROM weather_log")
    print_results(cur, "AVG(temp) — середня температура (без округлення)")

    # ── 1.4: MIN та MAX в одному запиті ──────────────────────────
    # Можна викликати кілька агрегатів одразу — отримаємо один рядок з N колонок.
    # Порівняй з pandas: df["temp"].min(), df["temp"].max()
    cur.execute("SELECT MIN(temp), MAX(temp) FROM weather_log")
    print_results(cur, "MIN(temp), MAX(temp) — найхолодніше та найтепліше")

    # ── 1.5: SUM — сума значень ──────────────────────────────────
    # Для температур сума не дуже корисна (дивний показник),
    # але SUM незамінний для грошей, продажів, кліків тощо.
    # Порівняй з pandas: df["temp"].sum()
    cur.execute("SELECT SUM(temp) FROM weather_log")
    print_results(cur, "SUM(temp) — сума всіх температур (для прикладу)")

    # ── 1.6: ROUND + AS — округлення та псевдонім колонки ────────
    # ROUND(число, N) — округлює до N знаків після коми.
    # ::numeric — це приведення типу REAL до NUMERIC, бо PostgreSQL хоче
    #   саме NUMERIC у ROUND з другим аргументом. Не лякайтесь синтаксису.
    # AS avg_temp — псевдонім (alias) для колонки результату.
    #   Без AS колонка б називалась "round" — некрасиво в виводі.
    cur.execute("""
        SELECT ROUND(AVG(temp)::numeric, 1) AS avg_temp
        FROM weather_log
    """)
    print_results(cur, "Округлене середнє з псевдонімом AS avg_temp")

    # 💡 Запам'ятай: одна агрегатна функція = один результат на ВСЮ таблицю.
    #    Щоб отримати окремий результат для кожного міста — потрібен GROUP BY.


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 2: GROUP BY — розбиваємо таблицю на групи           ║
# ══════════════════════════════════════════════════════════════════

def group_by_basics(cur):
    """
    GROUP BY — розбиває таблицю на групи за значенням однієї (або кількох) колонки,
    а потім обчислює агрегатну функцію ОКРЕМО для кожної групи.

    Уяви, що ти розкладаєш картки запитів по купках за містом:
      купка "Київ", купка "Львів", купка "Одеса"...
    Тепер можна порахувати кожну купку окремо.

    Порівняй з pandas: df.groupby("city")["temp"].mean()
    Це майже дослівний переклад того, що ми робили в bot.py / stats!
    """
    print("═" * 60)
    print("SQL КРОК 2: GROUP BY — групуємо за значенням колонки")
    print("═" * 60)

    # ── 2.1: Кількість запитів по кожному місту ───────────────────
    # Порівняй з pandas: df.groupby("city").size()  або  df["city"].value_counts()
    cur.execute("""
        SELECT city, COUNT(*)
        FROM weather_log
        GROUP BY city
    """)
    print_results(cur, "GROUP BY city — кількість запитів по кожному місту")

    # ── 2.2: Середня температура по містам ────────────────────────
    # Порівняй з pandas: df.groupby("city")["temp"].mean()
    cur.execute("""
        SELECT city, AVG(temp)
        FROM weather_log
        GROUP BY city
    """)
    print_results(cur, "GROUP BY city — середня температура по містам")

    # ── 2.3: Активність користувачів ──────────────────────────────
    # Порівняй з pandas: df.groupby("username").size()
    cur.execute("""
        SELECT username, COUNT(*)
        FROM weather_log
        GROUP BY username
    """)
    print_results(cur, "GROUP BY username — скільки запитів зробив кожен")

    # ── 2.4: Як часто яка погода ──────────────────────────────────
    # Порівняй з pandas: df["description"].value_counts()
    cur.execute("""
        SELECT description, COUNT(*)
        FROM weather_log
        GROUP BY description
    """)
    print_results(cur, "GROUP BY description — частота кожної погоди")

    # ⚠️  КЛАСИЧНА ПОМИЛКА (НЕ ВИКОНУЄМО — закоментовано).
    #
    # Якщо колонка не агрегована і не в GROUP BY — PostgreSQL відмовляється:
    #
    #   SELECT city, temp, COUNT(*) FROM weather_log GROUP BY city;
    #
    # Помилка: column "weather_log.temp" must appear in the GROUP BY clause
    #          or be used in an aggregate function.
    #
    # Логіка проста: для одного міста є БАГАТО температур. Яку показати?
    # SQL не вгадує — він вимагає, щоб ти або агрегував (AVG, MIN, MAX),
    # або поставив колонку в GROUP BY.
    print("  💡 Правило GROUP BY:")
    print("     Кожна колонка в SELECT має бути або в GROUP BY,")
    print("     або всередині агрегатної функції (COUNT/AVG/MIN/MAX/SUM).\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 3: GROUP BY + ORDER BY + LIMIT — рейтинги (топи)    ║
# ══════════════════════════════════════════════════════════════════

def group_by_with_order(cur):
    """
    Тепер вміємо рахувати по групах — давай знайдемо лідерів.

    Порядок виконання SQL у голові:
      1) FROM        — звідки беремо рядки
      2) WHERE       — фільтруємо рядки
      3) GROUP BY    — розкладаємо в купки
      4) SELECT      — обираємо що показати (агрегати + колонки з GROUP BY)
      5) ORDER BY    — сортуємо результат
      6) LIMIT       — обрізаємо до N перших

    ORDER BY можна сортувати за псевдонімом (AS) — це дуже зручно.
    """
    print("═" * 60)
    print("SQL КРОК 3: GROUP BY + ORDER BY + LIMIT — рейтинги")
    print("═" * 60)

    # ── 3.1: Топ-3 міст за кількістю запитів ──────────────────────
    # AS cnt — даємо ім'я агрегатній колонці, щоб потім сортувати по ньому.
    # Порівняй з pandas: df["city"].value_counts().head(3)
    cur.execute("""
        SELECT city, COUNT(*) AS cnt
        FROM weather_log
        GROUP BY city
        ORDER BY cnt DESC
        LIMIT 3
    """)
    print_results(cur, "Топ-3 міст за кількістю запитів")

    # ── 3.2: Найтепліші міста за середньою температурою ───────────
    # Порівняй з pandas:
    #   df.groupby("city")["temp"].mean().sort_values(ascending=False)
    cur.execute("""
        SELECT city, ROUND(AVG(temp)::numeric, 1) AS avg_temp
        FROM weather_log
        GROUP BY city
        ORDER BY avg_temp DESC
    """)
    print_results(cur, "Міста за середньою температурою (від найтеплішого)")

    # ── 3.3: Топ-5 активних користувачів ──────────────────────────
    # Порівняй з pandas:
    #   df.groupby("username").size().sort_values(ascending=False).head(5)
    cur.execute("""
        SELECT username, COUNT(*) AS requests
        FROM weather_log
        GROUP BY username
        ORDER BY requests DESC
        LIMIT 5
    """)
    print_results(cur, "Топ-5 найактивніших користувачів")

    # 💡 Саме цей патерн (GROUP BY + ORDER BY + LIMIT) ми використаємо
    #    в боті для команди /top_cities. Поглянь на функцію get_top_cities_sql()
    #    у bot.py — вона робить рівно те саме, що запит 3.1.


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 4: HAVING — фільтр ПО ГРУПАХ (vs WHERE)             ║
# ══════════════════════════════════════════════════════════════════

def group_by_having(cur):
    """
    HAVING — це WHERE для груп.

      WHERE  — фільтрує окремі РЯДКИ ДО групування.
      HAVING — фільтрує цілі ГРУПИ ПІСЛЯ агрегації.

    ┌──────────────┬────────────┬────────────────────────┐
    │ Етап         │ Інструмент │ Що фільтрує            │
    ├──────────────┼────────────┼────────────────────────┤
    │ До GROUP BY  │ WHERE      │ окремі рядки           │
    │ Після GROUP  │ HAVING     │ цілі групи (агрегати)  │
    └──────────────┴────────────┴────────────────────────┘

    ⚠️  Поширена помилка: написати WHERE COUNT(*) > 1 — це НЕ працює.
    WHERE виконується ДО агрегації, тому ще не знає, скільки в групі рядків.
    Для умов на агрегати — завжди HAVING.
    """
    print("═" * 60)
    print("SQL КРОК 4: HAVING — фільтр по групах")
    print("═" * 60)

    # ── 4.1: Тільки міста, які запитували 2+ разів ────────────────
    # Порівняй з pandas:
    #   counts = df.groupby("city").size()
    #   counts[counts >= 2]
    cur.execute("""
        SELECT city, COUNT(*) AS cnt
        FROM weather_log
        GROUP BY city
        HAVING COUNT(*) >= 2
        ORDER BY cnt DESC
    """)
    print_results(cur, "HAVING COUNT(*) >= 2 — тільки популярні міста")

    # ── 4.2: Міста з середньою температурою > 13°C ────────────────
    # Порівняй з pandas:
    #   avg = df.groupby("city")["temp"].mean()
    #   avg[avg > 13]
    cur.execute("""
        SELECT city, ROUND(AVG(temp)::numeric, 1) AS avg_temp
        FROM weather_log
        GROUP BY city
        HAVING AVG(temp) > 13
        ORDER BY avg_temp DESC
    """)
    print_results(cur, "HAVING AVG(temp) > 13 — теплі міста за середнім")

    # ── 4.3: WHERE + HAVING в одному запиті ───────────────────────
    # Спочатку WHERE temp > 10 — викидає холодні рядки (ще ДО групування).
    # Потім GROUP BY city — групує те, що лишилось.
    # Потім HAVING COUNT(*) >= 2 — лишає тільки міста з ≥2 "теплими" запитами.
    cur.execute("""
        SELECT city, COUNT(*) AS cnt, ROUND(AVG(temp)::numeric, 1) AS avg_temp
        FROM weather_log
        WHERE temp > 10                  -- крок 1: фільтр РЯДКІВ
        GROUP BY city                    -- крок 2: групування
        HAVING COUNT(*) >= 2             -- крок 3: фільтр ГРУП
        ORDER BY cnt DESC
    """)
    print_results(cur, "WHERE temp > 10 + HAVING COUNT(*) >= 2")

    print("  💡 Правило двох фільтрів:")
    print("     WHERE  — для умов на колонки рядків (city, temp, username...).")
    print("     HAVING — для умов на агрегати (COUNT, AVG, SUM, MIN, MAX).\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 5: GROUP BY кількома колонками + підсумок           ║
# ══════════════════════════════════════════════════════════════════

def group_by_multi(cur):
    """
    GROUP BY можна робити одразу за кількома колонками.
    Тоді групи — це УНІКАЛЬНІ КОМБІНАЦІЇ значень цих колонок.

    Наприклад, GROUP BY username, city → одна група = (один користувач, одне місто).
    Це корисно, щоб подивитись "хто скільки разів запитував кожне місто".

    Порівняй з pandas: df.groupby(["username", "city"]).size()
    """
    print("═" * 60)
    print("SQL КРОК 5: GROUP BY кількома колонками")
    print("═" * 60)

    # ── 5.1: Скільки разів кожен користувач запитував кожне місто ─
    # Порівняй з pandas: df.groupby(["username", "city"]).size()
    cur.execute("""
        SELECT username, city, COUNT(*) AS cnt
        FROM weather_log
        GROUP BY username, city
        ORDER BY username, city
    """)
    print_results(cur, "GROUP BY username, city — комбіноване групування")

    # ── 5.2: Те саме + фільтр по групах ────────────────────────────
    # Тільки ті пари (користувач, місто), де було більше одного запиту.
    cur.execute("""
        SELECT username, city, COUNT(*) AS cnt
        FROM weather_log
        GROUP BY username, city
        HAVING COUNT(*) > 1
        ORDER BY cnt DESC
    """)
    print_results(cur, "Хто запитував те саме місто ≥2 разів")

    print("  💡 Підсумок уроку:")
    print("     SELECT  колонки_GROUP_BY, агрегатні_функції(інші_колонки)")
    print("     FROM    таблиця")
    print("     [WHERE  умова_на_рядки]        -- фільтр ДО групування")
    print("     GROUP BY колонки                -- розбиваємо на групи")
    print("     [HAVING умова_на_агрегати]      -- фільтр груп ПІСЛЯ")
    print("     [ORDER BY що_сортувати DESC]    -- сортування результату")
    print("     [LIMIT  N]                      -- обрізати до N рядків\n")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                       ГОЛОВНА ФУНКЦІЯ                          ║
# ╚══════════════════════════════════════════════════════════════════╝

def main():
    """Запускає всі кроки уроку послідовно."""
    print("\n📊 SQL GROUP BY — Агрегації та групування")
    print("=" * 60)
    print()

    conn = connect_db()
    cur = conn.cursor()

    try:
        prepare_data(cur)        # створюємо таблицю та наповнюємо
        aggregates_basics(cur)   # КРОК 1: агрегати без GROUP BY
        group_by_basics(cur)     # КРОК 2: GROUP BY однією колонкою
        group_by_with_order(cur) # КРОК 3: GROUP BY + ORDER BY + LIMIT
        group_by_having(cur)     # КРОК 4: HAVING — фільтр по групах
        group_by_multi(cur)      # КРОК 5: GROUP BY кількома колонками

        print("=" * 60)
        print("🎉 Усі кроки виконано! Тепер ти знаєш GROUP BY та агрегати.")
        print("👉 Подивись команду /top_cities у bot.py — це GROUP BY у боті.")
        print("=" * 60)

    finally:
        # Завжди закриваємо курсор та з'єднання — навіть якщо була помилка.
        cur.close()
        conn.close()
        print("\n🔒 З'єднання з базою даних закрито.")


# Стандартний Python-патерн: код виконується тільки якщо файл запущено напряму.
if __name__ == "__main__":
    main()
