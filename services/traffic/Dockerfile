FROM python:3.11-slim

WORKDIR /app

# Instalamos dependencias necesarias
RUN pip install httpx numpy

COPY main.py .

CMD ["python", "main.py"]