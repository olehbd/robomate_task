# Базовий Python образ
FROM python:3.11-slim

# Встановлюємо робочу директорію
WORKDIR /app

# Встановлюємо залежності
COPY req.txt .
RUN pip install --no-cache-dir -r req.txt

# Копіюємо решту файлів
COPY . .

# Експортуємо порт
EXPOSE 8000

# Запускаємо застосунок
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
