import os
import random
from datetime import datetime, timedelta, UTC, date
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = os.getenv("BASE_URL")

def generting_events():
    types = ["page_view", "user_signup", "purchase", "user_logout", "comment"]
    countries = ["UA", "PL", "US", "DE", "FR"]
    sources = ["web", "mobile", "api"]
    events = []
    for i in range(1, 100001):
        event = {
            "event_id": str(f'25698{i}'),
            "event_type": random.choice(types),
            "occurred_at": (datetime.now(UTC) - timedelta(seconds=i*30)).isoformat(),
            "user_id": 1000 + i,
            "properties": {
                "source": random.choice(sources),
                "country": random.choice(countries)
            }
        }
        events.append(event)
    return events

from fastapi.testclient import TestClient
from main import app  # або як у тебе називається модуль


def test_ingest_events_performance():
    data_events = generting_events()

    # Крок 1: інгест подій
    response = requests.post(f"{BASE_URL}/events", json=data_events)

    # Перевірка відповіді
    assert response.status_code == 200
    result = response.json()
    assert result["status"] == "ok"
    assert result["upserted"] == 100000

    # Крок 2: визначаємо період для DAU статистики
    today = date.today()
    from_ = today - timedelta(days=15)  # наприклад, останні 7 днів
    to = today

    # Крок 3: запит статистики DAU
    stats_response = requests.get(
        f"{BASE_URL}/stats/dau",
        params={"from_": from_.isoformat(), "to": to.isoformat()}
    )

    # Перевірка відповіді
    assert stats_response.status_code == 200
    stats = stats_response.json()

    # Базові перевірки
    assert isinstance(stats, list)
    assert len(stats) > 0

    for row in stats:
        assert "day" in row
        assert "unique_users" in row
        assert isinstance(row["unique_users"], int)

    # print(
    #     f"Ingested {result['upserted']} events in {duration:.2f}s "
    #     f"→ DAU stats for {from_}–{to}: {stats}"
    # )

    # # Вивід результатів
    # print(f"\n✅ Ingested 100,000 events in {duration:.2f} seconds")
    # print(f"➡️  Average speed: {100000 / duration:.2f} events/sec")


test_ingest_events_performance()
