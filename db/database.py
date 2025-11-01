import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

# створюємо async engine
engine = create_async_engine(
    DATABASE_URL,
    echo=False,  # лог SQL-запитів
)

# створюємо асинхронну сесію
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

# базовий клас моделей
Base = declarative_base()
