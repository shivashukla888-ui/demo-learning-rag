FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
COPY scripts scripts
COPY config config
EXPOSE 8000
CMD ["sh", "-c", "if [ ! -f /data/models/surveillance-model.joblib ]; then PYTHONPATH=/app python scripts/train_model.py --output /data/models/surveillance-model.joblib --demo-rows 8000; fi && exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1"]
