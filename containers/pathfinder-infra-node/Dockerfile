FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY pathfinder ./pathfinder

RUN pip install --no-cache-dir --no-deps . \
    && useradd --uid 10001 --create-home --shell /usr/sbin/nologin pathfinder \
    && mkdir /state \
    && chown 10001:10001 /state

USER 10001:10001

EXPOSE 9080

VOLUME ["/state"]

ENTRYPOINT ["python", "-m", "pathfinder"]
