# Event Analytics API

API-сервіс для збору, збереження та аналітики користувацьких подій.  
Побудований на **FastAPI**, **PostgreSQL**, **SQLAlchemy**, **Redis** і підтримує **rate limiting**.

---

## Основні можливості

- Прийом та UPSERT подій (`POST /events`)
- Обчислення **DAU** (Daily Active Users)
- Аналітика **топових подій**
- **Когортний retention** користувачів
- Фільтрація DAU за сегментом (`/stats/dau/filtered`)
- Rate limiting на IP через **Token Bucket**
- Асинхронна робота з базою даних (SQLAlchemy Async)
- Redis для кешу або throttling

---

## Технології

- **Python 3.11+**
- **FastAPI**
- **SQLAlchemy (async)**
- **PostgreSQL**
- **Redis**
- **dotenv** — для керування змінними оточення
- **Logging** — структуровані логи запитів і подій

---

## Встановлення та запуск

### Клонування репозиторію

```bash
git clone https://github.com/olehbd/robomate_task.git
cd robomate_task
```

### Запуск усіх сервісів через Docker Compose

Проєкт містить готовий `docker-compose.yml`, який автоматично піднімає:
- **PostgreSQL** (основна база даних)
- **Redis** 
- **FastAPI-сервіс** (API для аналітики подій)

Для запуску всього середовища виконайте:

```bash
docker compose up --build
```

### Команда для імпорту events з csv
```bash
python import_events.py events_sample.csv
```
### Команда для запуску тесту на 100 000 events
```bash
python test.py
```
Слабке міце велика кількість events може покласти базу,
тому краще спочатку завантажити в Redis і потім записувати 
частинами у базу.
Але тест на 100 000 пройшов успішно.

### Також можна скористатися SwaggerUI для тесту усіх endpoints
```bash
http://127.0.0.1:8000/docs
```
