FROM python:3.12-slim

WORKDIR /app

# System deps: build tools + PostgreSQL client libs + ODBC Driver 18 for SQL Server
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev curl gnupg unixodbc-dev && \
    # Add Microsoft package signing key and repo
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
        -o /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

COPY . .
RUN pip install --no-cache-dir -e ".[llm,dev,bigquery,databricks,sqlserver]"

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
