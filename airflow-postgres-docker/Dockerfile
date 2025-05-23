FROM apache/airflow:2.5.0

USER root
RUN apt-get update && apt-get install -y gcc python3-dev
USER airflow

RUN pip install --no-cache-dir \
    numpy \
    pandas \
    psycopg2-binary \
    bottleneck>=1.3.6 \
    pyarrow \
    numexpr
