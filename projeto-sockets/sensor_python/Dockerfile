FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY ./sensor_python/requirements.txt ./sensor_python/requirements.txt
RUN python -m pip install --no-cache-dir --requirement ./sensor_python/requirements.txt \
    && python -m pip check

COPY ./sensor_python ./sensor_python
COPY ./common ./common

RUN protoc -I=./common --python_out=./sensor_python ./common/messages.proto

WORKDIR /app/sensor_python

EXPOSE 5004

CMD ["python", "sensor.py"]
