#!/bin/bash

# Script untuk memperbaiki permission folder logs/scheduler yang dibuat oleh
# root
# Akan berjalan sebagai background daemon untuk memantau folder baru

LOG_DIR="/opt/airflow/logs"
AIRFLOW_UID=50000
GID=1000
SCRIPT_LOG="/opt/airflow/logs/fix-log-permission.log"

touch $SCRIPT_LOG
chown $AIRFLOW_UID:$GID $SCRIPT_LOG
chmod 664 $SCRIPT_LOG

# Fungsi logging
log_message() {
    # echo (STDOUT) ditangkap juga oleh docker logs, tee masuk ke file log
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$SCRIPT_LOG"
}

fix_permissions(){
    if [ -d "$LOG_DIR" ]; then
        chown -R $AIRFLOW_UID:$GID "$LOG_DIR"
        chmod -R 775 "$LOG_DIR"
        log_message "Permissions fixed for $LOG_DIR"
    else
        log_message "Directory $LOG_DIR does not exist. Creating it now."
        mkdir -p "$LOG_DIR"
        chown -R $AIRFLOW_UID:$GID "$LOG_DIR"
        chmod -R 775 "$LOG_DIR"
        log_message "Directory $LOG_DIR created and permissions set."
    fi
}

daemon() {
    checksum1=""

    while [[ true ]]
    do
        checksum2=`find $LOG_DIR -print | sort | md5sum`
        if [[ $checksum1 != $checksum2 ]] ; then           
            fix_permissions
            checksum1=$checksum2
        fi
        sleep 30
    done
}

log_message "Starting permission fix daemon"
daemon
