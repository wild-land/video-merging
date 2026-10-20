FROM python:3.12-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY app.py /app/app.py
COPY static /app/static

ENV PYTHONUNBUFFERED=1 \
    PORT=8080 \
    DATA_DIR=/data \
    SOURCE_ROOT=/recordings \
    OUTPUT_ROOT=/merged

EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=3)" || exit 1
CMD ["python", "/app/app.py"]
