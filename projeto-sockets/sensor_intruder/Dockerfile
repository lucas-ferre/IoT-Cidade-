FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./sensor_intruder/requirements.txt /app/sensor_intruder/requirements.txt
RUN pip install --no-cache-dir -r /app/sensor_intruder/requirements.txt && pip check
COPY ./sensor_intruder /app/sensor_intruder
COPY ./common /app/common
RUN protoc -I=/app/common --python_out=/app /app/common/messages.proto \
    && useradd --system --uid 10002 intruder
USER intruder
CMD ["python", "-m", "sensor_intruder.main"]
