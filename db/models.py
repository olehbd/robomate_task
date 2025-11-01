from sqlalchemy import Column, String, DateTime, JSON, Integer
from db.database import Base


class Event(Base):
    __tablename__ = "events"

    event_id = Column(String, primary_key=True, index=True)
    occurred_at = Column(DateTime(timezone=True), index=True)
    user_id = Column(Integer, index=True)
    event_type = Column(String, index=True)
    properties = Column(JSON)
