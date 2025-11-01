import asyncio

from redis.asyncio import Redis
import json
from sqlalchemy import text, bindparam, JSON
from db.database import Base, engine, AsyncSessionLocal


async def redis_to_db_worker(redis: Redis):
    """Worker, який постійно читає з Redis і вставляє в Postgres"""
    print("🚀 Redis → DB worker started")
    while True:
        try:
            batch = []
            # Забираємо до 100 подій за раз
            for _ in range(10000):
                raw = await redis.lpop("events_queue")
                print(raw)
                if not raw or None:
                    break
                batch.append(json.loads(raw))

            if batch:
                async with AsyncSessionLocal() as db:
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

                    await db.execute(insert_stmt, batch)
                    await db.commit()
                    print(f"✅ Inserted {len(batch)} events into DB")

            # Якщо черга порожня — трохи відпочинь
            await asyncio.sleep(1)

        except Exception as e:
            print(f"❌ Worker error: {e}")
            await asyncio.sleep(5)
