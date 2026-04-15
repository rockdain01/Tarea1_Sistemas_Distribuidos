# Usamos una imagen ligera de Python
FROM python:3.10-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos los requisitos e instalamos librerías
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código fuente y los datos procesados
COPY src/ ./src/
COPY data/processed/ ./data/processed/

# Comando por defecto para ejecutar tu generador
CMD ["python", "src/generador.py"]