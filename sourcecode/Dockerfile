FROM apache/airflow:2.9.3-python3.12

# install requirements.txt disini agar dijalankan ketika build image, bukan setiap jalankan container
COPY ./requirements.txt /opt/airflow/requirements.txt
RUN if [ -f /opt/airflow/requirements.txt ] && [ -s /opt/airflow/requirements.txt ]; then \
      pip install --no-cache-dir -r /opt/airflow/requirements.txt; \
    else \
      echo "requirements.txt tidak ada, skip"; \
    fi

# git: untuk fitur manage git di dbt
USER root
RUN apt update && apt install -y git && apt clean
USER airflow

