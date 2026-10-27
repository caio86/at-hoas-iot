FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/app/config.yaml

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app app

COPY --chown=app:app publisher.py mqttsn_client.py config.yaml ./

USER app

CMD ["python", "-u", "publisher.py"]


