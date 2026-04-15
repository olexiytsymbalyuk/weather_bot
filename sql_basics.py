"""
SQL Basics — Основи SQL на прикладі погодних даних
===================================================
Навчальний скрипт для курсу програмування.
Демонструє базові SQL-операції через PostgreSQL,
використовуючи знайому тему погодних даних з попереднього уроку.
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

# Завантажуємо змінні середовища з файлу .env
# Це безпечний спосіб зберігати паролі та інші секрети —
# вони НЕ потрапляють у git, бо .env додано до .gitignore.
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
    # psycopg2.connect() — створює з'єднання з базою даних.
    # Параметри: назва бази, користувач, пароль, хост, порт.
    # Якщо база не існує — отримаємо помилку. Спочатку створіть базу:
    #   CREATE DATABASE weather_db;
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    # autocommit = True означає, що кожна SQL-команда виконується одразу,
    # без потреби викликати conn.commit() після кожної операції.
    # Для навчання це зручніше — результат видно миттєво.
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

    # cursor.description — містить інформацію про колонки результату.
    # Кожен елемент — це кортеж, де [0] — назва колонки.
    col_names = [desc[0] for desc in cursor.description]

    print(f"  {title}:")
    print(f"  {' | '.join(col_names)}")
    print(f"  {'─' * 60}")
    for row in rows:
        print(f"  {' | '.join(str(val) for val in row)}")
    print()


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 1: CREATE TABLE — Створення таблиці                 ║
# ══════════════════════════════════════════════════════════════════

def create_table(cur):
    """
    CREATE TABLE — створює нову таблицю в базі даних.

    Таблиця — це як Excel-таблиця: має колонки (стовпці) та рядки.
    Кожна колонка має:
      - Назву (наприклад, "city")
      - Тип даних (TEXT, INTEGER, REAL, TIMESTAMP тощо)
      - Обмеження (NOT NULL — обов'язкове поле, PRIMARY KEY — унікальний ідентифікатор)
    """
    print("═" * 60)
    print("SQL КРОК 1: CREATE TABLE — Створення таблиці")
    print("═" * 60)

    # DROP TABLE IF EXISTS — видаляє таблицю, якщо вона вже існує.
    # Це зручно для навчання: кожен запуск скрипта починає з чистого аркуша.
    # ⚠️  В реальному проєкті так НІКОЛИ не роблять — це видалить усі дані!
    cur.execute("DROP TABLE IF EXISTS weather_log")

    # CREATE TABLE weather_log (...) — створює таблицю з 8 колонками.
    #
    # Типи даних PostgreSQL:
    #   SERIAL       — автоматичний лічильник (1, 2, 3, ...). Ідеально для id.
    #   PRIMARY KEY  — головний ключ: унікальний ідентифікатор кожного рядка.
    #   TIMESTAMP    — дата і час (наприклад, "2026-04-15 14:30:00").
    #   INTEGER      — ціле число (наприклад, 361312515).
    #   VARCHAR(100) — текст до 100 символів. Як str у Python, але з обмеженням довжини.
    #   TEXT         — текст без обмеження довжини.
    #   REAL         — дробове число (наприклад, 15.3). Як float у Python.
    #   NOT NULL     — означає, що поле обов'язкове (не може бути порожнім).
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

    print("✅ Таблицю weather_log створено!\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 2: INSERT — Додавання даних                         ║
# ══════════════════════════════════════════════════════════════════

def insert_data(cur):
    """
    INSERT INTO — додає нові рядки (записи) до таблиці.

    Синтаксис:
      INSERT INTO таблиця (колонка1, колонка2, ...)
      VALUES (значення1, значення2, ...)

    ⚠️  ВАЖЛИВО: Ніколи не підставляйте значення напряму в SQL-рядок!
    Використовуйте параметризовані запити (%s) — це захищає від SQL-ін'єкцій.

    SQL-ін'єкція — це атака, коли зловмисник підставляє шкідливий SQL-код
    замість звичайних даних. Наприклад, замість назви міста вводить:
      "Київ'; DROP TABLE weather_log; --"
    Параметризовані запити (%s) автоматично екранують такі символи.
    """
    print("═" * 60)
    print("SQL КРОК 2: INSERT — Додавання даних")
    print("═" * 60)

    # Підготуємо тестові дані — погодні записи для українських міст.
    # Формат: (timestamp, user_id, username, city, temp, feels_like, description)
    sample_data = [
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
    ]

    # INSERT INTO ... VALUES (%s, %s, ...) — параметризований запит.
    # %s — це плейсхолдер (заповнювач). psycopg2 автоматично підставляє
    # значення з кортежу та екранує спеціальні символи.
    # Це БЕЗПЕЧНИЙ спосіб вставки даних.
    #
    # ❌ НЕБЕЗПЕЧНО (ніколи так не робіть!):
    #    cur.execute(f"INSERT INTO weather_log VALUES ('{city}', {temp})")
    #
    # ✅ БЕЗПЕЧНО (завжди так):
    #    cur.execute("INSERT INTO weather_log (city, temp) VALUES (%s, %s)", (city, temp))
    insert_query = """
        INSERT INTO weather_log (timestamp, user_id, username, city, temp, feels_like, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for row in sample_data:
        cur.execute(insert_query, row)

    print(f"✅ Додано {len(sample_data)} записів до таблиці weather_log!\n")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 3: SELECT — Читання даних                           ║
# ══════════════════════════════════════════════════════════════════

def select_basics(cur):
    """
    SELECT — найважливіша SQL-команда. Читає дані з таблиці.

    Синтаксис:
      SELECT колонка1, колонка2 FROM таблиця

    SELECT * — повертає ВСІ колонки. Зірочка (*) означає "все".
    LIMIT N  — обмежує кількість рядків у результаті.
    """
    print("═" * 60)
    print("SQL КРОК 3: SELECT — Читання даних")
    print("═" * 60)

    # ── 3.1: SELECT * — вибираємо всі колонки та всі рядки ──────
    # Це як відкрити CSV-файл і побачити все.
    # LIMIT 5 — показуємо лише перші 5 рядків (щоб не засмічувати екран).
    cur.execute("SELECT * FROM weather_log LIMIT 5")
    print_results(cur, "SELECT * (перші 5 рядків)")

    # ── 3.2: SELECT конкретних колонок ───────────────────────────
    # Замість * вказуємо лише ті колонки, які нам потрібні.
    # Це ефективніше — база повертає менше даних.
    cur.execute("SELECT city, temp, description FROM weather_log LIMIT 5")
    print_results(cur, "SELECT city, temp, description (перші 5)")

    # ── 3.3: SELECT з унікальними значеннями (DISTINCT) ──────────
    # DISTINCT — прибирає дублікати. Показує кожне місто лише раз.
    # Порівняй з pandas: df["city"].unique()
    cur.execute("SELECT DISTINCT city FROM weather_log")
    print_results(cur, "SELECT DISTINCT city (унікальні міста)")

    # ── 3.4: COUNT — підрахунок кількості рядків ─────────────────
    # COUNT(*) — рахує скільки рядків у таблиці.
    # Порівняй з pandas: len(df)
    cur.execute("SELECT COUNT(*) FROM weather_log")
    print_results(cur, "COUNT(*) — загальна кількість записів")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 4: WHERE — Фільтрація даних                         ║
# ══════════════════════════════════════════════════════════════════

def select_where(cur):
    """
    WHERE — фільтрує рядки за умовою. Повертає тільки ті, що відповідають.

    Порівняй з pandas: df[df["city"] == "Київ"]

    Оператори:
      =            — дорівнює
      > < >= <=    — більше, менше
      AND          — обидві умови мають бути TRUE
      OR           — хоча б одна умова TRUE
      BETWEEN a AND b — значення в діапазоні [a, b]
      LIKE         — пошук за шаблоном (% — будь-які символи)
    """
    print("═" * 60)
    print("SQL КРОК 4: WHERE — Фільтрація даних")
    print("═" * 60)

    # ── 4.1: WHERE з оператором = ───────────────────────────────
    # Вибираємо записи тільки для Києва.
    # Порівняй з pandas: df[df["city"] == "Київ"]
    cur.execute("""
        SELECT city, temp, description
        FROM weather_log
        WHERE city = 'Київ'
    """)
    print_results(cur, "WHERE city = 'Київ'")

    # ── 4.2: WHERE з оператором > ───────────────────────────────
    # Вибираємо записи, де температура вища за 14 градусів.
    # Порівняй з pandas: df[df["temp"] > 14]
    cur.execute("""
        SELECT city, temp, description
        FROM weather_log
        WHERE temp > 14
    """)
    print_results(cur, "WHERE temp > 14")

    # ── 4.3: WHERE з AND — кілька умов одночасно ────────────────
    # Записи для Києва, де температура вища за 13 градусів.
    # AND — обидві умови мають виконуватися.
    # Порівняй з pandas: df[(df["city"] == "Київ") & (df["temp"] > 13)]
    cur.execute("""
        SELECT city, temp, description
        FROM weather_log
        WHERE city = 'Київ' AND temp > 13
    """)
    print_results(cur, "WHERE city = 'Київ' AND temp > 13")

    # ── 4.4: WHERE з OR — хоча б одна умова ─────────────────────
    # Записи для Києва АБО Одеси.
    # Порівняй з pandas: df[df["city"].isin(["Київ", "Одеса"])]
    cur.execute("""
        SELECT city, temp, description
        FROM weather_log
        WHERE city = 'Київ' OR city = 'Одеса'
    """)
    print_results(cur, "WHERE city = 'Київ' OR city = 'Одеса'")

    # ── 4.5: WHERE з BETWEEN — діапазон значень ─────────────────
    # Температура від 12 до 14 градусів (включно з обома межами).
    # Порівняй з pandas: df[df["temp"].between(12, 14)]
    cur.execute("""
        SELECT city, temp
        FROM weather_log
        WHERE temp BETWEEN 12 AND 14
    """)
    print_results(cur, "WHERE temp BETWEEN 12 AND 14")

    # ── 4.6: WHERE з LIKE — пошук за шаблоном ───────────────────
    # % — означає "будь-які символи" (0 або більше).
    # 'невеликий%' — знаходить все, що починається з "невеликий".
    # Наприклад: "невеликий дощ", "невеликий сніг".
    cur.execute("""
        SELECT city, description
        FROM weather_log
        WHERE description LIKE 'невеликий%'
    """)
    print_results(cur, "WHERE description LIKE 'невеликий%'")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 5: ORDER BY — Сортування результатів                ║
# ══════════════════════════════════════════════════════════════════

def select_order(cur):
    """
    ORDER BY — сортує результати запиту.

    ASC  — за зростанням (від меншого до більшого). Це значення за замовчуванням.
    DESC — за спаданням (від більшого до меншого).

    Порівняй з pandas: df.sort_values("temp", ascending=False)
    """
    print("═" * 60)
    print("SQL КРОК 5: ORDER BY — Сортування результатів")
    print("═" * 60)

    # ── 5.1: ORDER BY ASC — від найхолоднішого до найтеплішого ──
    cur.execute("""
        SELECT city, temp
        FROM weather_log
        ORDER BY temp ASC
    """)
    print_results(cur, "ORDER BY temp ASC (найхолодніше → найтепліше)")

    # ── 5.2: ORDER BY DESC — від найтеплішого до найхолоднішого ─
    cur.execute("""
        SELECT city, temp
        FROM weather_log
        ORDER BY temp DESC
        LIMIT 3
    """)
    print_results(cur, "ORDER BY temp DESC LIMIT 3 (топ-3 найтепліших)")

    # ── 5.3: ORDER BY кількома колонками ─────────────────────────
    # Спочатку сортуємо за містом (A-Я), а в межах міста — за температурою.
    cur.execute("""
        SELECT city, temp, description
        FROM weather_log
        ORDER BY city ASC, temp DESC
    """)
    print_results(cur, "ORDER BY city ASC, temp DESC")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 6: UPDATE — Оновлення даних                         ║
# ══════════════════════════════════════════════════════════════════

def update_data(cur):
    """
    UPDATE — змінює значення в існуючих рядках.

    Синтаксис:
      UPDATE таблиця SET колонка = нове_значення WHERE умова

    ⚠️  ЗАВЖДИ використовуйте WHERE з UPDATE!
    Без WHERE — зміняться ВСІ рядки в таблиці!
    """
    print("═" * 60)
    print("SQL КРОК 6: UPDATE — Оновлення даних")
    print("═" * 60)

    # Подивимось на дані ДО зміни.
    cur.execute("SELECT id, city, temp FROM weather_log WHERE city = 'Вінниця'")
    print_results(cur, "ДО оновлення (Вінниця)")

    # UPDATE ... SET ... WHERE — оновлюємо температуру для Вінниці.
    # Уявімо, що прийшло оновлення погоди і стало тепліше.
    cur.execute("""
        UPDATE weather_log
        SET temp = 16.5, feels_like = 15.0, description = 'ясне небо'
        WHERE city = 'Вінниця'
    """)
    print("  ✏️  Виконано: UPDATE weather_log SET temp=16.5 WHERE city='Вінниця'")

    # Подивимось на дані ПІСЛЯ зміни.
    cur.execute("SELECT id, city, temp, description FROM weather_log WHERE city = 'Вінниця'")
    print_results(cur, "ПІСЛЯ оновлення (Вінниця)")


# ══════════════════════════════════════════════════════════════════
# ║  SQL КРОК 7: DELETE — Видалення даних                         ║
# ══════════════════════════════════════════════════════════════════

def delete_data(cur):
    """
    DELETE — видаляє рядки з таблиці.

    Синтаксис:
      DELETE FROM таблиця WHERE умова

    ⚠️  ОБОВ'ЯЗКОВО використовуйте WHERE!
    DELETE FROM weather_log — без WHERE видалить ВСІ записи!
    Це одна з найнебезпечніших SQL-команд, якщо забути WHERE.
    """
    print("═" * 60)
    print("SQL КРОК 7: DELETE — Видалення даних")
    print("═" * 60)

    # Подивимось скільки записів є зараз.
    cur.execute("SELECT COUNT(*) FROM weather_log")
    print_results(cur, "Кількість записів ДО видалення")

    # Видаляємо записи з описом "невеликий дощ".
    cur.execute("""
        DELETE FROM weather_log
        WHERE description = 'невеликий дощ'
    """)
    print("  🗑️  Виконано: DELETE WHERE description = 'невеликий дощ'")

    # Перевіряємо скільки записів залишилось.
    cur.execute("SELECT COUNT(*) FROM weather_log")
    print_results(cur, "Кількість записів ПІСЛЯ видалення")

    # Показуємо що залишилось.
    cur.execute("SELECT city, temp, description FROM weather_log ORDER BY city")
    print_results(cur, "Записи що залишились")


# ╔══════════════════════════════════════════════════════════════════╗
# ║                       ГОЛОВНА ФУНКЦІЯ                          ║
# ╚══════════════════════════════════════════════════════════════════╝

def main():
    """Запускає всі кроки уроку послідовно."""
    print("\n🗄️  SQL Basics — Основи SQL на прикладі погодних даних")
    print("=" * 60)
    print()

    # Підключаємось до бази даних.
    conn = connect_db()

    # cursor (курсор) — це об'єкт, через який ми відправляємо SQL-запити.
    # Уяви курсор як "вказівник", що виконує команди в базі даних
    # та повертає результати.
    cur = conn.cursor()

    try:
        create_table(cur)       # КРОК 1: Створення таблиці
        insert_data(cur)        # КРОК 2: Додавання даних
        select_basics(cur)      # КРОК 3: Базові SELECT-запити
        select_where(cur)       # КРОК 4: Фільтрація з WHERE
        select_order(cur)       # КРОК 5: Сортування ORDER BY
        update_data(cur)        # КРОК 6: Оновлення UPDATE
        delete_data(cur)        # КРОК 7: Видалення DELETE

        print("=" * 60)
        print("🎉 Усі кроки виконано! Тепер ти знаєш основи SQL!")
        print("=" * 60)

    finally:
        # Завжди закриваємо курсор та з'єднання — навіть якщо була помилка.
        # Це звільняє ресурси бази даних.
        cur.close()
        conn.close()
        print("\n🔒 З'єднання з базою даних закрито.")


# Стандартний Python-патерн: код виконується тільки якщо файл запущено напряму
# (а не імпортовано як модуль).
if __name__ == "__main__":
    main()
