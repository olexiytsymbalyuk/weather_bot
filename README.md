# Weather Bot — Telegram-бот з погодою та аналітикою

Навчальний проєкт для курсу програмування. Демонструє роботу з бібліотекою **pandas** на прикладі реального Telegram-бота.

Бот показує погоду для будь-якого міста, зберігає історію запитів у CSV та аналізує дані через pandas: статистика, групування, графіки.

## Що використовується

| Бібліотека | Для чого |
|---|---|
| `python-telegram-bot` | Telegram Bot API |
| `requests` | HTTP-запити до OpenWeatherMap |
| `pandas` | Аналіз даних (DataFrame, groupby, value_counts, агрегатні функції) |
| `matplotlib` | Побудова графіків температур |
| `python-dotenv` | Завантаження токенів з `.env` файлу |
| `psycopg2` | PostgreSQL-драйвер (уроки 2-4: SQL, GROUP BY, JOIN) |
| `bcrypt` | Хешування паролів (урок 5) |
| `cryptography` | Симетричне шифрування Fernet (урок 5) |

## Встановлення

```bash
# 1. Створюємо віртуальне середовище
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# 2. Встановлюємо залежності
pip install -r requirements.txt
```

## Налаштування

1. Отримай токен бота у [@BotFather](https://t.me/BotFather)
2. Отримай безкоштовний API-ключ на [OpenWeatherMap](https://openweathermap.org/api)
3. Створи файл `.env` у кореневій папці проєкту:

```env
TELEGRAM_TOKEN=твій_токен_від_botfather
WEATHER_API_KEY=твій_ключ_від_openweathermap
```

## Запуск

```bash
python bot.py
```

## Команди бота

| Команда | Що робить |
|---|---|
| `/start` | Привітання та список команд |
| `/help` | Довідка |
| `/stats` | Зведена статистика: топ міст, кількість запитів, температури (pandas + CSV) |
| `/plot` | Графік температур по всіх містах |
| `/plot Київ` | Графік температур для конкретного міста |
| `/top_cities` | Топ-5 міст через SQL `GROUP BY` з PostgreSQL (урок 3) |
| `/me` | Особиста статистика через SQL `JOIN` трьох таблиць (урок 4) |
| *будь-який текст* | Показує погоду для вказаного міста |

## Pandas-операції в коді

Код прокоментований покроково (`PANDAS КРОК 1..7`). Ось які операції pandas демонструються:

### У функції `stats()`
- `pd.read_csv()` — завантаження CSV у DataFrame
- `df.empty` — перевірка на порожність
- `df["city"].nunique()` — підрахунок унікальних значень
- `df["city"].value_counts().head(5)` — частота значень (топ-5)
- `df.groupby("city")["temp"].mean().round(1)` — групування та агрегація
- `df["temp"].min()`, `.max()`, `.mean()` — агрегатні функції

### У функції `plot()`
- `pd.to_datetime()` — конвертація тексту в дату
- `df[df["city"].str.lower() == ...]` — булеве індексування з `.str` accessor
- `.iloc[0]` — доступ за позицією
- `df.groupby("city")` — ітерація по групах

## Структура проєкту

```
weather_bot/
  bot.py             # Telegram-бот з погодою (урок pandas + SQL у /top_cities, /me)
  sql_basics.py      # основи SQL через PostgreSQL (урок 2)
  sql_groupby.py     # GROUP BY та агрегатні функції (урок 3)
  sql_join.py        # JOIN трьох таблиць (урок 4)
  crypto_basics.py   # хешування + симетричне шифрування (урок 5)
  .env               # токени та паролі (не комітити!)
  .env.example       # шаблон для .env
  requirements.txt   # залежності
  bot_log.csv        # лог запитів (створюється автоматично)
```

---

# SQL Basics — Основи SQL на прикладі погодних даних

Другий урок курсу. Демонструє базові SQL-операції через PostgreSQL, використовуючи знайому тему погодних даних.

## SQL-операції в коді

Код прокоментований покроково (`SQL КРОК 1..7`):

| Крок | SQL-команда | Що робить |
|------|------------|-----------|
| 1 | `CREATE TABLE` | Створення таблиці з колонками та типами даних |
| 2 | `INSERT INTO` | Додавання записів (з параметризованими запитами!) |
| 3 | `SELECT` | Читання даних: `*`, конкретні колонки, `DISTINCT`, `COUNT` |
| 4 | `WHERE` | Фільтрація: `=`, `>`, `AND`, `OR`, `BETWEEN`, `LIKE` |
| 5 | `ORDER BY` | Сортування: `ASC`, `DESC`, за кількома колонками |
| 6 | `UPDATE` | Оновлення існуючих записів |
| 7 | `DELETE` | Видалення записів (і чому завжди потрібен `WHERE`!) |

## Налаштування PostgreSQL

1. Встановіть PostgreSQL: [postgresql.org/download](https://www.postgresql.org/download/)

2. Створіть базу даних:

```sql
CREATE DATABASE weather_db;
```

3. Додайте до `.env` параметри підключення (див. `.env.example`):

```env
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## Запуск SQL-уроку

```bash
python sql_basics.py
```

Скрипт виконає всі 7 кроків послідовно та покаже результати кожного SQL-запиту.

---

# SQL GROUP BY — Агрегації та групування (урок 3)

Третій урок курсу. Продовжує `sql_basics.py`: вчимося зводити багато рядків в одну цифру через **агрегатні функції**, рахувати окремо по кожній групі через **GROUP BY**, і фільтрувати самі групи через **HAVING**.

Кінцева ідея уроку — учні бачать, що SQL-запит `SELECT city, COUNT(*) FROM weather_log GROUP BY city` — це майже дослівний переклад того, що вони вже робили в pandas: `df.groupby("city").size()`. Те ж саме мислення, інша мова.

## Що покрито

| Крок | Тема | Ключові SQL-конструкції |
|------|------|-------------------------|
| 1 | Агрегати без `GROUP BY` | `COUNT`, `COUNT(DISTINCT)`, `AVG`, `MIN`, `MAX`, `SUM`, `AS`, `ROUND` |
| 2 | `GROUP BY` однією колонкою | Класичне правило: всі неагреговані колонки SELECT — у GROUP BY |
| 3 | `GROUP BY` + `ORDER BY` + `LIMIT` | Топи (найпопулярніші, найтепліші, найактивніші) |
| 4 | `HAVING` vs `WHERE` | Фільтр **груп** після агрегації, фільтр **рядків** до неї |
| 5 | `GROUP BY` кількома колонками | Унікальні комбінації значень + підсумкова шпаргалка |

## Запуск

```bash
python sql_groupby.py
```

Скрипт сам перестворює таблицю `weather_log` на 16 тестових записах і прогонить усі 5 кроків з форматованим виводом.

## SQL у боті: команда `/top_cities`

Цей урок не лишається лише в навчальному скрипті — у `bot.py` додано команду `/top_cities`, яка робить запит до PostgreSQL і повертає топ-5 міст:

```sql
SELECT city, COUNT(*) AS query_count
FROM weather_log
GROUP BY city
ORDER BY query_count DESC
LIMIT 5;
```

Поруч з нею `/stats` робить рівно той самий підрахунок через `df["city"].value_counts().head(5)` у pandas — учні можуть запустити обидві команди й порівняти результати.

---

# SQL JOIN — З'єднання таблиць (урок 4)

Четвертий урок курсу. Продовжує `sql_groupby.py`: тепер у нас не одна таблиця, а **три** (`weather_log`, `users`, `cities`), і ми вчимося з'єднувати їх через **JOIN**.

Кінцева ідея уроку — учні бачать, що `JOIN ... ON ...` у SQL — це майже дослівний переклад `pd.merge(left, right, on=...)` у pandas. Те ж саме мислення, інша мова.

## Що покрито

| Крок | Тема | Ключові SQL-конструкції |
|------|------|-------------------------|
| 1 | INNER JOIN — основа основ | `JOIN ... ON`, `AS`-псевдоніми таблиць, колізія імен колонок |
| 2 | LEFT JOIN — "усі ліві, навіть без пари" | NULL з правої таблиці, `IS NULL` для пошуку юзерів без запитів |
| 3 | RIGHT JOIN та FULL OUTER JOIN | Симетрія LEFT/RIGHT, FULL OUTER для синхронізації двох списків |
| 4 | JOIN трьох таблиць | `weather_log JOIN users JOIN cities` + GROUP BY поверх; `IS DISTINCT FROM` |
| 5 | Self JOIN — таблиця сама з собою | Знаходження пар рядків в одній таблиці |
| 6 | Pandas-міст | `pd.read_sql_query` + `pd.merge(how="inner"/"left"/"outer")` |

## Структура даних

Урок не ламає схему `weather_log` (учні вже з нею знайомі). Замість цього додає поряд дві довідкові таблиці:

- `users` — `user_id`, `username`, `first_seen`, `language`, `home_city` (може бути `NULL`)
- `cities` — `name`, `country`, `region`, `population`, `latitude`, `longitude`

У тестових даних навмисно є "сирітські" рядки:
- юзер `оля_к` — є в `users`, але не зробив жодного запиту → демо LEFT JOIN
- міста `Полтава`, `Чернівці` — є в `cities`, але ніхто їх не запитував → демо RIGHT/FULL OUTER JOIN
- юзер `ірина_с` — `home_city = NULL` → демо `IS DISTINCT FROM`

## Запуск

```bash
python sql_join.py
```

Скрипт перестворює всі три таблиці на свіжих даних і прогонить усі 6 кроків.

## SQL у боті: команда `/me`

У `bot.py` додано команду `/me`, яка робить запит з **JOIN трьох таблиць** і повертає особисту статистику юзера:

```sql
SELECT
    u.username,
    u.language,
    u.home_city,
    COUNT(w.id) AS requests,
    (SELECT c.region
     FROM weather_log w2
     JOIN cities c ON w2.city = c.name
     WHERE w2.user_id = u.user_id
     GROUP BY c.region
     ORDER BY COUNT(*) DESC
     LIMIT 1) AS top_region
FROM users u
LEFT JOIN weather_log w ON u.user_id = w.user_id
WHERE u.user_id = %s
GROUP BY u.user_id, u.username, u.language, u.home_city;
```

Учень може спочатку запустити `python sql_join.py` (отримати тестові дані для 6 юзерів), а далі побачити, як та сама ідея з JOIN-ом працює в реальному боті.

---

# Crypto Basics — Хешування + симетричне шифрування (урок 5)

П'ятий урок курсу. Окремий трек — **безпека даних**. Не залежить від SQL-уроків. Сценарій береться з нашого ж бота: у `bot_log.csv` зберігаються `user_id` (персональні дані), у `.env` лежать API-токени.

Дві базові ідеї криптографії, які закривають ці сценарії:

| Що робимо | Інструмент | Сценарій у боті |
|---|---|---|
| Одностороннє перетворення для приватності | `hashlib.sha256` + сіль | `user_id` → хеш у `bot_log.csv` |
| Двостороннє шифрування секретів | `cryptography.fernet.Fernet` | Шифрування `WEATHER_API_KEY` (демо) |

## Що покрито

| Крок | Тема | Ключові інструменти |
|------|------|---------------------|
| 1 | Що таке хеш-функція | `hashlib.sha256`, властивості (детермінованість, лавинний ефект) |
| 2 | MD5/SHA-1 — антипатерн | Чому НЕ використовувати; rainbow-table як приклад атаки |
| 3 | Salt | `secrets.token_bytes`, навіщо солити паролі |
| 4 | bcrypt — правильно для паролів | `bcrypt.hashpw`, `bcrypt.checkpw`, cost factor |
| 5 | HMAC — підпис повідомлень | `hmac.new(key, msg, sha256)`, `hmac.compare_digest` (timing attack) |
| 6 | Симетричне шифрування | `Fernet.generate_key`, `encrypt/decrypt`, IV, перевірка цілісності |
| 7 | Зберігання ключів | `.env` як сховище, антипатерни (захардкодити ключ у код) |

## Залежності

```bash
pip install bcrypt cryptography
```

(Або просто `pip install -r requirements.txt` — вони вже там.)

## Запуск

```bash
python crypto_basics.py
```

Скрипт прогонить усі 7 кроків з форматованим виводом і покаже практичний приклад інтеграції з ботом.

## Інтеграція в бота: хешування `user_id` у логах

У `bot.py` функція `log_request()` тепер хешує `user_id` через SHA-256 з сіллю:

```python
def hash_user_id(uid: int) -> str:
    return hashlib.sha256(USER_ID_SALT.encode() + str(uid).encode()).hexdigest()
```

Що це дає:
- `bot_log.csv` більше не містить plain Telegram `user_id` — лише хеші.
- `/stats` далі правильно рахує `df["user_id"].nunique()`, бо однакові `user_id` дають однакові хеші.
- Витечка `bot_log.csv` не розкриває, ХТО запитував — лише факт запитів.

Щоб увімкнути сіль — додай `USER_ID_SALT` у `.env` (приклад в `.env.example`):

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Скопіюй вивід у `.env` як значення `USER_ID_SALT=...`.

> Якщо `USER_ID_SALT` порожній, бот стартує з попередженням, але працює — хешує без солі (краще, ніж plain, але слабше за хеш з сіллю).
