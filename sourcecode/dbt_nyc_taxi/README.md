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
Document server telah di forward dari docker ke host di port 8083 dengan `python -m http.server`,
jadi bisa dibuka disitu http://127.0.0.1:8083 (tidak perlu jalankan `dbt docs serve`)

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
