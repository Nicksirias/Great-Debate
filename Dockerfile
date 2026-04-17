FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

# Single worker: SQLite does not handle concurrent writes across multiple processes.
CMD ["gunicorn", "-w", "1", "-b", "0.0.0.0:5000", "app:app"]
