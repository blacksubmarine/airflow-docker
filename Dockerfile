FROM apache/airflow:2.9.2-python3.9

# Install necessary system dependencies
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    unixodbc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install ODBC Driver 17 for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
RUN curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list
RUN apt-get update && ACCEPT_EULA=Y apt-get install -y \
    msodbcsql17

# Switch back to airflow user
USER airflow

# Copy the requirements.txt file into the image
COPY requirements.txt /requirements.txt

# Upgrade pip and install the packages specified in requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

# Continue with the rest of your Dockerfile instructions...
