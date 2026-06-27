FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir flask ask-sdk-core protobuf redis

COPY ./alexa_skill ./alexa_skill
COPY ./common ./common

RUN protoc -I=./common --python_out=./alexa_skill ./common/messages.proto

WORKDIR /app/alexa_skill

EXPOSE 5008

CMD ["python", "app.py"]
