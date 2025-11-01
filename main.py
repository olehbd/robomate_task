import os
from datetime import datetime, date, timedelta, UTC
import time

from redis.asyncio import Redis
from fastapi import FastAPI, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, bindparam, JSON
from typing import List
from db import schemas, models
from db.database import Base, engine, AsyncSessionLocal
import logging
from bucket import TokenBucket
from fastapi import Query
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BASE_URL")
REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = os.getenv("REDIS_PORT")

# --- Конфігурація логів ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()  # показує логи у терміналі (stdout)
    ]
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Event Analytics API")
# redis: Redis | None = None

buckets = {}


# Dependency для сесії БД
async def get_db():
    async with AsyncSessionLocal() as db:
        yield db


@app.on_event("startup")
async def startup():
    global redis
    try:
        redis = Redis(host=f'{REDIS_HOST}', port=REDIS_PORT, decode_responses=True)
        pong = await redis.ping()
        logger.info(f"Redis connected: {pong}")

        # ініціалізуємо БД
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            logger.info(f"DB connected:")

    except Exception as e:
        logger.error("Error connected Redis")


@app.on_event("shutdown")
async def shutdown():
    if redis:
        await redis.close()
        logger.info("Redis disconnected")


# --------------------------
# POST /events
# --------------------------
@app.post("/events")
async def ingest_events(
        request: Request,
        events: List[schemas.EventSchema],
        db: AsyncSession = Depends(get_db)
):
    logger.info({
        "event": "events_received",
        "count": len(events),
    })
    client_ip = request.client.host

    # бакет для кожного користувача
    bucket = buckets.get(client_ip)

    if not bucket:
        bucket = TokenBucket(capacity=10, refill_rate=1)  # макс 10 запитів, +5 токенів/сек
        buckets[client_ip] = bucket

    if not bucket.allow_request():
        raise HTTPException(status_code=429, detail="Too many requests. Try again later 🫣")

    try:
        start_time = time.time()
        # Конвертуємо у dict-и
        event_dicts = [e.dict() for e in events]

        # Використовуємо сирий UPSERT (PostgreSQL)
        insert_stmt = text("""
            INSERT INTO events (event_id, user_id, event_type, occurred_at, properties)
            VALUES (:event_id, :user_id, :event_type, :occurred_at, :properties)
            ON CONFLICT (event_id)
            DO UPDATE SET
                user_id = EXCLUDED.user_id,
                event_type = EXCLUDED.event_type,
                occurred_at = EXCLUDED.occurred_at,
                properties = EXCLUDED.properties;
        """).bindparams(
            bindparam("event_id"),
            bindparam("user_id"),
            bindparam("event_type"),
            bindparam("occurred_at"),
            bindparam("properties", type_=JSON)
        )

        await db.execute(insert_stmt, event_dicts)
        await db.commit()

        duration = round(time.time() - start_time, 3)
        events_second = len(events) / duration
        logger.info(f"Upserted {len(events)} events in {duration}s Events received: {int(events_second)}")

        return {"status": "ok", "upserted": len(events)}

    except Exception as e:
        await db.rollback()
        logger.error(f"Error while upserting events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --------------------------
# GET /stats/dau
# --------------------------
@app.get("/stats/dau")
async def get_dau(
        from_: date,
        to: date,
        db: AsyncSession = Depends(get_db)
):
    try:
        start_time = datetime.now(UTC)
        logger.info(f"🔹 DAU request started: from_={from_}, to={to}")

        if from_ > to:
            raise HTTPException(status_code=400, detail="from_ must be before to")
        query = text("""
            SELECT DATE(occurred_at) AS day, COUNT(DISTINCT user_id) AS unique_users
            FROM events
            WHERE occurred_at BETWEEN :from_ AND :to
            GROUP BY day
            ORDER BY day;
        """)
        to = to + timedelta(days=1)
        result = await db.execute(query, {"from_": from_, "to": to})
        rows = result.fetchall()

        elapsed = (datetime.now(UTC) - start_time).total_seconds()
        logger.info(f"✅ DAU request finished in {elapsed:.3f}s, rows returned: {len(rows)}")

        return [{"day": str(r.day), "unique_users": r.unique_users} for r in rows]
    except Exception as e:
        logger.error(f"Error: {e}")


@app.get("/stats/top-events")
async def get_top_events(
    from_: date,
    to: date,
    limit: int,
    db: AsyncSession = Depends(get_db)
):
    query = text("""
        SELECT event_type, COUNT(*) AS count
        FROM events
        WHERE occurred_at BETWEEN :from_ AND :to
        GROUP BY event_type
        ORDER BY count DESC
        LIMIT :limit;
    """)
    result = await db.execute(query, {"from_": from_, "to": to, "limit": limit})
    rows = result.fetchall()
    return [{"event_type": r.event_type, "count": r.count} for r in rows]


@app.get("/stats/retention")
async def get_retention(
    start_date: date,
    windows: int = Query(3, ge=1, le=30),
    db: AsyncSession = Depends(get_db)
):
    """
    Обчислює денний когортний retention для користувачів,
    які вперше з’явилися у start_date.
    """

    # 1️⃣ Знаходимо користувачів когорти
    cohort_users_query = text("""
        SELECT DISTINCT user_id
        FROM events
        WHERE DATE(occurred_at) = :start_date
    """)
    result = await db.execute(cohort_users_query, {"start_date": start_date})
    cohort_users = [r.user_id for r in result]

    if not cohort_users:
        return {"start_date": str(start_date), "cohort_size": 0, "retention": []}

    cohort_size = len(cohort_users)

    # 2️⃣ Для кожного дня retention вікна
    retention_data = []
    for i in range(windows + 1):
        day = start_date + timedelta(days=i)
        next_day = day + timedelta(days=1)

        query = text("""
            SELECT COUNT(DISTINCT user_id) AS active_users
            FROM events
            WHERE user_id IN :users
            AND occurred_at >= :day
            AND occurred_at < :next_day
        """).bindparams(bindparam("users", expanding=True))
        result = await db.execute(
            query,
            {"users": tuple(cohort_users), "day": day, "next_day": next_day}
        )
        row = result.fetchone()

        active_users = row.active_users if row else 0
        retention_rate = round(active_users / cohort_size, 3)

        retention_data.append({
            "day": i,
            "date": str(day),
            "active_users": active_users,
            "retention_rate": retention_rate
        })

    return {
        "start_date": str(start_date),
        "cohort_size": cohort_size,
        "retention": retention_data
    }


@app.get("/stats/dau/filtered")
async def get_dau_filtered(
        from_: date,
        to: date,
        segment: str = Query(
            None,
            description="Фільтр: field:value або properties.field=value"
        ),
        db: AsyncSession = Depends(get_db)
):
    if from_ > to:
        raise HTTPException(status_code=400, detail="from_ must be before to")

    filters = []
    params = {"from_": from_, "to": to + timedelta(days=1)}

    # --- Обробка segment ---
    if segment:
        print(segment)
        clean = segment.strip().replace('"', '').replace("'", "")
        # приклад: properties.country=UA або event_type:purchase
        if clean.startswith("properties."):
            if "=" not in clean:
                raise HTTPException(status_code=400, detail="Use '=' for properties filters")
            key, value = clean.split("=", 1)
            prop_key = key.split(".", 1)[1]
            filters.append("properties ->> :prop_key = :prop_value")
            params["prop_key"] = prop_key.strip()
            params["prop_value"] = value.strip()
        elif ":" in clean:
            field, value = clean.split(":", 1)
            filters.append(f"{field.strip()} = :segment_value")
            params["segment_value"] = value.strip()
        else:
            raise HTTPException(status_code=400, detail="Invalid segment format")

    filter_sql = " AND ".join(filters) if filters else "1=1"

    query = text(f"""
        SELECT DATE(occurred_at) AS day, COUNT(DISTINCT user_id) AS unique_users
        FROM events
        WHERE occurred_at BETWEEN :from_ AND :to
        AND {filter_sql}
        GROUP BY day
        ORDER BY day;
    """)

    logger.info(f"Executing query with params: {params}")
    result = await db.execute(query, params)
    row = result.fetchall()

    return [{"day": str(r.day), "unique_users": r.unique_users} for r in row]
