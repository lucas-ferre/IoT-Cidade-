FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY ./hubs/requirements.txt /app/hubs/requirements.txt
RUN pip install --no-cache-dir -r /app/hubs/requirements.txt && pip check
COPY ./hubs /app/hubs
COPY ./common /app/common
RUN protoc -I=/app/common --python_out=/app /app/common/messages.proto \
    && useradd --system --uid 10001 hub
USER hub
EXPOSE 5000/udp 5002/udp 5001/tcp 5010/tcp
CMD ["python", "-m", "hubs.main", "sensores"]
