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
  bot.py             # Telegram-бот з погодою (урок pandas + SQL у /top_cities)
  sql_basics.py      # основи SQL через PostgreSQL (урок 2)
  sql_groupby.py     # GROUP BY та агрегатні функції (урок 3)
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
