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
| `/stats` | Зведена статистика: топ міст, кількість запитів, температури |
| `/plot` | Графік температур по всіх містах |
| `/plot Київ` | Графік температур для конкретного міста |
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
  bot.py             # весь код бота (один файл)
  .env               # токени (не комітити!)
  requirements.txt   # залежності
  bot_log.csv        # лог запитів (створюється автоматично)
```
