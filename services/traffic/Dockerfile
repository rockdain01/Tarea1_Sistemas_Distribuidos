FROM python:3.11-slim

WORKDIR /app

#dependencias necesarias
RUN pip install httpx numpy kafka-python==2.0.2

COPY main.py .

CMD ["python", "main.py"]