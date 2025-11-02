## Intro
semua proses berjalan di dalam docker.
sudah ada service untuk

- airflow dashboard http://127.0.0.1:8081
- mailhog http://127.0.0.1:8025
- dokumentasi model hasil DBT http://127.0.0.1:8083
- dashboard visualisasi http:127.0.0.1:8082

Secara default email dikirim ke mailhog, bila ingin pakai smtp google bisa atur
di `.env`

## prerequisite
Sudah install docker terbaru (sudah support docker compose)
kosongkan port 8081, 8082, 8025, 1025, atau atur di `.env` atau `docker-compose.yaml` untuk pakai port lain

## how to run
1. buka terminal (bash/zsh/cmd/ps dll) masuk ke folder sourcecode
2. buat file `sourcecode/.env` untuk override pengaturan lewat environment variable  contoh

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

8. Bila pipeline sudah dijalankan, dapat mengakses dokumentasi model di 127.0.0.1:8083
8. Bila pipeline sudah dijalankan, dapat mengakses Visualisasi Data di 127.0.0.1:8082

