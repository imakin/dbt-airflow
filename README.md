# DWIB Pipeline Analitik Data NYC Taxi

## Deskripsi Repo:

Dikerjakan sebagai UAS / Proyek Akhir ke2 DWIB

Oleh: Muhammad Ramdan Izzulmakin (551362)

Pembuatan datamart dari new york city taxi https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page.
Pembuatan datamart menggunakan DBT (data build tools). Dijalankan dengan pipeline/scheduler Apache Airflow.
Semua dijalankan didalam docker (Airflow, DBT, Mailhog untuk test email).

# 3.1 Apache Airflow

- 3 files python DAG: [sourcecode/dags](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/dags)
  
  - sourcecode/dags/nyc_taxi_ingestion.py
  - sourcecode/dags/nyc_taxi_transform.py
  - sourcecode/dags/nyc_taxi_monitor.py

- Docker compose: [sourcecode/docker-compose.yaml](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/docker-compose.yaml), [sourcecode/Dockerfile](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/Dockerfile)

- Screenshots

- Readme setup: [sourcecode/README.md](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/README.md)
  
        ## Intro
      
        semua proses berjalan di dalam docker.
        sudah ada service untuk airflow dan mailhog.
        Secara default email dikirim ke mailhog, bila ingin pakai smtp google bisa atur
        di `.env`.
      
        ## prerequisite
      
        Sudah install docker terbaru (sudah support docker compose)
        kosongkan port 8081, 8025, 1025, atau atur di .env/docker-compose.yaml untuk pakai port lain
      
        ## how to run
      
        1. buka terminal (bash/zsh/cmd/ps dll) masuk ke folder sourcecode
      
        2. buat file `.env` untuk override pengaturan lewat environment variable 
      
                # sourcecode/.env
      
                # supaya file2 milik bersama dengan host
                # id -g
                AIRFLOW_GUID=1000
      
                # python -c 'import secrets; print(secrets.token_urlsafe(32))'
                AIRFLOW__WEBSERVER__SECRET_KEY=Dul583vYiwkuKoP1qInVbk5Dq7z43jw7JUpYQHA5PgM
      
                # python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'
                AIRFLOW__CORE__FERNET_KEY=es7vdIxpLvYnezIlRVF3h4lcOck0cwGVzvQDaXzILNc=
      
                AIRFLOW_WEB_PORT=8081
      
                #user dan password untuk dashboard web
                AIRFLOW_WEB_USER=makin
                AIRFLOW_WEB_PASSWORD=makin
      
                DWIB_UAS_EMAIL_TO=you@mail.ugm.ac.id
      
                AIRFLOW__SMTP__SMTP_HOST=dwib-uas-mailhog
                AIRFLOW__SMTP__SMTP_STARTTLS=False
                AIRFLOW__SMTP__SMTP_SSL=False
                AIRFLOW__SMTP__SMTP_USER=you@gmail.com
                AIRFLOW__SMTP__SMTP_PASSWORD=passwordsmtp
                AIRFLOW__SMTP__SMTP_PORT=485
                AIRFLOW__SMTP__SMTP_MAIL_FROM=from@gmail.com
      
        3. jalankan `docker compose build`
      
        4. jalankan `docker compose up -d`
      
        5. buka browser di 127.0.0.1:8081 untuk GUI admin page airflow
      
        6. buka browser di 127.0.0.1:8025 untuk GUI email mailhog
      
        7. **Simulate 2023-01:** simulasi pipeline tanggal 2023-01-01 hingga 2023-03-31 dengan script yang sudah ada `simulate.sh` (might take long)
      
            - `docker exec -it dwib-uas-scheduler bash`
            - `cd /opt/airflow`
            - `source simulate.sh`

# 3.2 Data Build Tool

- Complete DBT project: [sourcecode/dbt_nyc_taxi](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/dbt_nyc_taxi)

- Semua SQL models: [sourcecode/dbt_nyc_taxi/models](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/dbt_nyc_taxi/models)
  
  - sourcecode/dbt_nyc_taxi/models/staging/stg_taxi_trips.sql
  - sourcecode/dbt_nyc_taxi/models/staging/stg_taxi_zones.sql
  - sourcecode/dbt_nyc_taxi/models/intermediate/int_daily_metrics.sql
  - sourcecode/dbt_nyc_taxi/models/intermediate/int_trips_enhanced.sql
  - sourcecode/dbt_nyc_taxi/models/marts/dim_zones.sql
  - sourcecode/dbt_nyc_taxi/models/marts/fct_trips.sql
  - sourcecode/dbt_nyc_taxi/models/marts/agg_daily_stats.sql

- Schema YAML dengan tests: [sourcecode/dbt_nyc_taxi/models](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/dbt_nyc_taxi/models)
  
  - sourcecode/dbt_nyc_taxi/models/staging/schema.yml
  - sourcecode/dbt_nyc_taxi/models/intermediate/schema.yml
  - sourcecode/dbt_nyc_taxi/models/marts/schema.yml

- Generated docs (HTML): [sourcecode/dbt_nyc_taxi/target](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/dbt_nyc_taxi/target)

- Screenshots:

- README setup: [sourcecode/dbt_nyc_taxi/README.md](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/dbt_nyc_taxi/README.md)
  
        # Deskripsi
      
        Baca dulu [sourcecode/README.md](https://github.com/imakin/dbt-airflow/tree/main/sourcecode/README.md)

        Folder project DBT ini sudah jadi dan akan di panggil di dalam task di apache airflow yang berjalan di project ini.
        Bila menggunakan docker (instruksi ../README.md) dbt akan terinstall di dalam docker.
    
        # Untuk mengakses dbt project ini di dalam docker
    
        buka terminal ke dalam docker (docker sudah jalan)
        ```
        docker exec -it dwib-uas-scheduler bash
        ```
    
        ```
        # di dalam docker
        cd /opt/airflow/dbt_nyc_taxi
        dbt run
        dbt test
        dbt docs generate
        ```
        Document server tidak saya forward ke-host. Sebagai gantinya bisa buka terminal pada host
        ```
        # terminal host (bukan docker)
        # sesuaikan  path bila perlu, sesuaikan port server bila perlu
        cd sourcecode/dbt_nyc_taxi/target
        python -m http.server 8080
        ```
        sehingga bisa buka di port tersbut
    
        # Bila ingin menjalankan manual tanpa airflow:
    
        1. Buat folder `.dbt`-nya di folder user bila belum ada, Kopi file `sourcecode/config/dbt/profiles.yml` ke:
        - Linux & MacOS: `~/.dbt/profiles.yml` (/home/namaakun/.dbt/)
        - Windows 11: `%USERPROFILE%\.dbt\profiles.yml` (C:/Users/NamaAkun/.dbt/profiles.yml)
    
    
        2. install requirements: **(NOTE: sesuaikan dengan posisi cwd terminal sekarang)** 
    
            ```
            # terminal host (bukan docker)
            # sesuaikan path terhadap working directory terminal sekarang
            cd sourcecode/dbt_nyc_taxi
            pip install -r ../requirements.txt
            ```
    
        3. jalankan dbt
    
            ```
            dbt run
            dbt test
            dbt docs generate
            # untuk serve sourcecode/dbt_nyc_taxi/target ke local server
            dbt docs serve
            ```
