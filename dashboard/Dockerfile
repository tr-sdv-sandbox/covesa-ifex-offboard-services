FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY fleet_api.py .
COPY fleet_dashboard.html .
COPY service_schemas.json .

# PostgreSQL defaults
ENV POSTGRES_HOST=postgres
ENV POSTGRES_PORT=5432
ENV POSTGRES_DB=ifex_offboard
ENV POSTGRES_USER=ifex
ENV POSTGRES_PASSWORD=ifex_dev

# MQTT defaults (for RPC and Scheduler c2v publishing)
ENV MQTT_HOST=mosquitto
ENV MQTT_PORT=1883

# Dashboard port
ENV DASHBOARD_PORT=8000

EXPOSE 8000

CMD ["python", "fleet_api.py"]
