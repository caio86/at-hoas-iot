FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CONFIG_PATH=/app/config.yaml

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system app \
    && useradd --system --gid app --home-dir /app app

COPY --chown=app:app mqttsn_gateway.py config.yaml README.md ./

USER app

EXPOSE 1884/udp

CMD ["python", "-u", "mqttsn_gateway.py"]
