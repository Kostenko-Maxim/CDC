FROM python:3.12.9-slim-bookworm
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY cdc_pipeline cdc_pipeline
COPY connectors connectors
ENTRYPOINT ["python", "-m", "cdc_pipeline"]
