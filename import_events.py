import asyncio
import csv
import logging
from datetime import datetime
import json
import time
from db import models
from db.database import AsyncSessionLocal, Base, engine
from sqlalchemy import text, bindparam, JSON


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def init_db():
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            print("conectado com sucesso")
    except Exception as e:
        print(f"Error connecting to database: {e}")


async def import_events(csv_path: str):
    start_time = time.time()
    async with AsyncSessionLocal() as db:
        try:
            with open(csv_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                events = list(reader)
                for e in events:
                    e["user_id"] = int(e["user_id"]) if e["user_id"] else None
                    e["occurred_at"] = datetime.strptime(e["occurred_at"], "%Y-%m-%dT%H:%M:%S%z")

                insert_stmt = text("""
                    INSERT INTO events (event_id, user_id, event_type, occurred_at, properties)
                    VALUES (:event_id, :user_id, :event_type, :occurred_at, :properties_json)
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
                    bindparam("properties_json", type_=JSON)
                )

                await db.execute(insert_stmt, events)
                await db.commit()

            duration = round(time.time() - start_time, 3)
            logger.info(f"✅ Imported {len(events)} events from {csv_path} in {duration}s")

        except Exception as e:
            await db.rollback()
            logger.error(f"❌ Import failed: {e}")

async def main(csv_path: str):
    await init_db()
    await import_events(csv_path)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python import_events.py <path-to-csv>")
        sys.exit(1)

    csv_path = sys.argv[1]
    asyncio.run(main(csv_path))
