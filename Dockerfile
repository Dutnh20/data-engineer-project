# syntax=docker/dockerfile:1
FROM python:3.12.12-bookworm AS wheels
WORKDIR /build
COPY requirements.lock .
RUN pip wheel --no-cache-dir --only-binary=:all: --wheel-dir /wheels -r requirements.lock

FROM python:3.12.12-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONUTF8=1
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 libgomp1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.lock .
RUN --mount=type=bind,from=wheels,source=/wheels,target=/wheels \
    pip install --no-cache-dir --no-index --find-links=/wheels -r requirements.lock
RUN useradd --uid 10001 --create-home generator && mkdir /data && chown generator:generator /data
COPY --chown=generator:generator edai_generator /app/edai_generator
COPY --chown=generator:generator pipelines /app/pipelines
COPY --chown=generator:generator configs /app/configs
COPY --chown=generator:generator tests /app/tests
COPY --chown=generator:generator generate_data.py /app/generate_data.py
USER generator
ENTRYPOINT ["python", "-m", "edai_generator"]
CMD ["--help"]
