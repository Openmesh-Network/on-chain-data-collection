# syntax=docker/dockerfile:1

FROM python:3.8.13-alpine3.15
WORKDIR /src
COPY src/ .
RUN apk add --no-cache gcc musl-dev librdkafka-dev
COPY requirements.txt requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt
COPY config.ini ./
ENTRYPOINT ["python3"]
CMD ["uniswap.py"]
