FROM apache/airflow:2.9.3

# Switch to airflow user for installing Python packages
USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
