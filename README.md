# Tarea 1: Plataforma de análisis de preguntas y respuestas en Internet

Este proyecto implementa un sistema de microservicios utilizando **Docker** y **Redis** para simular y analizar el rendimiento de un sistema de caché distribuido. El sistema procesa datos de tráfico de diversas zonas de Santiago y evalúa métricas críticas como Hit Rate, Latencia y Eficiencia de Caché entre otros.

## Estructura del Proyecto

El sistema se divide en 5 microservicios independientes:

* **Traffic**: Generador de carga que simula peticiones de usuarios.
* **Cache**: Intermediario que gestiona la lógica de almacenamiento en Redis.
* **Responder**: Simulación de base de datos.
* **Metrics**: Servicio estadístico que calcula el rendimiento y consulta evicciones reales en Redis.
* **Redis**: Motor de almacenamiento en memoria.

## Requisitos Previos

* **Docker** y **Docker Compose** instalados.
* Conexión a internet para la descarga de imágenes base.

## Instrucciones del Despliegue

para ejecutar el sistema siga estas instrucciones cero:
1. **Abrir Docker**

2. **Abrir la terminal desde el editor de codigo pero simpre desde la raiz del sistema**

3.  **Limpiar el entorno (es opcional pero recomendado):**
    Si desea asegurar que Redis comience sin datos previos y sin volúmenes antiguos:
    ```bash
    docker compose down -v
    ```

4.  **Construir y levantar el sistema:**
    Este comando descarga las dependencias, construye las imágenes y levanta los servicios:
    ```bash
    docker compose up --build
    ```

5.  **Verificar la ejecución:**
    Una vez que el servicio `traffic` termine su simulación, verá el mensaje: `Simulación completada con éxito`.

## Consulta de Métricas

Para obtener los resultados finales de la simulación, ejecute el siguiente comando en una nueva terminal:

```bash
curl http://localhost:8003/summary
