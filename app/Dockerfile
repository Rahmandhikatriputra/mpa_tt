FROM python:3.12-alpine

WORKDIR /app
COPY . /app

# Add system dependencies and install 
RUN apk update && \
    apk add --no-cache build-base python3-dev libpq-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del build-base python3-dev && \
    rm -rf /var/cache/apk/*