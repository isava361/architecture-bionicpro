FROM apache/airflow:2.9.2

ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.11
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
ARG PIP_DEFAULT_TIMEOUT=120
ARG PIP_RETRIES=5

USER root
COPY --chown=airflow:airflow requirements.txt /opt/airflow/requirements.txt
USER airflow
RUN pip install --no-cache-dir --default-timeout "${PIP_DEFAULT_TIMEOUT}" --retries "${PIP_RETRIES}" \
  --constraint "${CONSTRAINTS_URL}" -r /opt/airflow/requirements.txt
