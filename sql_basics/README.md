# SQL Basics — Основи SQL на прикладі погодних даних

Навчальний проєкт для курсу програмування. Демонструє базові SQL-операції через PostgreSQL, використовуючи знайому тему погодних даних з попереднього уроку (Telegram-бот).

## Що вивчаємо

| Крок | SQL-команда | Що робить |
|------|------------|-----------|
| 1 | `CREATE TABLE` | Створення таблиці з колонками та типами даних |
| 2 | `INSERT INTO` | Додавання записів (з параметризованими запитами!) |
| 3 | `SELECT` | Читання даних: `*`, конкретні колонки, `DISTINCT`, `COUNT` |
| 4 | `WHERE` | Фільтрація: `=`, `>`, `AND`, `OR`, `BETWEEN`, `LIKE` |
| 5 | `ORDER BY` | Сортування: `ASC`, `DESC`, за кількома колонками |
| 6 | `UPDATE` | Оновлення існуючих записів |
| 7 | `DELETE` | Видалення записів (і чому завжди потрібен `WHERE`!) |

## Що потрібно

- Python 3.10+
- PostgreSQL (встановлений та запущений)

## Встановлення

```bash
# 1. Створюємо віртуальне середовище
python -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# 2. Встановлюємо залежності
pip install -r requirements.txt
```

## Налаштування бази даних

1. Створіть базу даних у PostgreSQL:

```sql
CREATE DATABASE weather_db;
```

2. Скопіюйте файл конфігурації та заповніть свої дані:

```bash
cp .env.example .env
```

3. Відредагуйте `.env` — вкажіть свій пароль від PostgreSQL:

```env
DB_NAME=weather_db
DB_USER=postgres
DB_PASSWORD=your_password
DB_HOST=localhost
DB_PORT=5432
```

## Запуск

```bash
python sql_basics.py
```

Скрипт виконає всі 7 кроків послідовно та покаже результати кожного SQL-запиту.

## Корисні посилання

- [PostgreSQL — завантаження](https://www.postgresql.org/download/)
- [psycopg2 — документація](https://www.psycopg.org/docs/)
- [SQL Tutorial — W3Schools](https://www.w3schools.com/sql/)
