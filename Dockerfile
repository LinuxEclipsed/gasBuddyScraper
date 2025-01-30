FROM python:3.9-alpine

WORKDIR /app

COPY src/main.py .
COPY src/requirements.txt .

# Install dependencies and remove cache to reduce image size
RUN apk add --no-cache gcc musl-dev && \
    pip install --no-cache-dir -r requirements.txt && \
    apk del gcc musl-dev

CMD ["python", "main.py"]