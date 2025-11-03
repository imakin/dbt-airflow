#!/usr/bin/env python3
"""Check progress from logs
Because airflow web only shows last 3 days for 6hr interval tasks
Only check from logs, queued task before its log created wont be shown
@author: izzulmakin 2025
"""

import sys
import os
from pathlib import Path
from collections import defaultdict

from dags.scripts.utils import BASE_DIR
os.chdir(BASE_DIR)

dag_name = sys.argv[1] if len(sys.argv) > 1 else "nyc_taxi_monitor"
LOG_DIR = Path(f"logs/dag_id={dag_name}")

def get_task_status(log_file):
    """Baca status dari log file"""
    try:
        with open(log_file, 'r') as f:
            content = f.read()
            if "Marking task as SUCCESS" in content:
                return "success"
            elif "Marking task as FAILED" in content or "ERROR" in content:
                return "failed"
            elif "up_for_retry" in content.lower():
                return "retry"
            else:
                return "unknown"
    except:
        return "unknown"

def main():
    if not LOG_DIR.exists():
        print(f"Error: {LOG_DIR} not found")
        return
    
    runs = defaultdict(lambda: defaultdict(str))
    all_tasks = set()
    
    # Scan log dir
    for run_dir in sorted(LOG_DIR.glob("run_id=backfill__*")):
        run_id = run_dir.name.replace("run_id=", "")
        
        # Scan tasks
        for task_dir in run_dir.glob("task_id=*"):
            task_name = task_dir.name.replace("task_id=", "")
            all_tasks.add(task_name)
            
            # ambil latest attempt
            log_files = list(task_dir.glob("attempt=*.log"))
            if log_files:
                latest_log = max(log_files, key=lambda x: int(x.stem.split("=")[1]))
                status = get_task_status(latest_log)
                runs[run_id][task_name] = status
    
    # urutan tasks (hardcode untuk nyc_taxi_monitor)
    if dag_name == "nyc_taxi_monitor":
        task_order = ["check_freshness", "validate_counts", "compare_patterns", 
                    "detect_anomalies", "generate_report", "send_monitoring_report"]
        all_tasks = sorted(all_tasks, key=lambda x: task_order.index(x) if x in task_order else 999)
    
    # Count statistics
    total_runs = len(runs)
    success_count = 0
    failed_retry_count = 0
    scheduled_count = 0
    
    print(f"\total_runs: {total_runs}")

    # Display run status
    print(f"{'Run ID':<35} {'Status':<50}")
    print(f"{'-'*80}")

    printed_tasks = []
    
    for run_id in sorted(runs.keys()):
        statuses = []
        run_success = 0
        run_failed = 0
        
        for task in all_tasks:
            status = runs[run_id].get(task, "scheduled")
            if status == "success":
                statuses.append("S")
                run_success += 1
            elif status in ["failed", "retry"]:
                statuses.append("F")
                run_failed += 1
            else:
                statuses.append(".")
        
        status_str = " ".join(statuses)
        
        # Determine overall run status
        if run_success == len(all_tasks):
            overall = "ALL SUCCESS"
            success_count += 1

        elif run_failed > 0:
            overall = f"FAILED/RETRY ({run_failed} tasks)"
            failed_retry_count += 1
        else:
            overall = f"IN PROGRESS ({run_success}/{len(all_tasks)})"
            scheduled_count += 1

        printed_tasks.append(run_id)
        print(f"{run_id:<35} {status_str:<15} {overall}")

    # check lagi bila ada yang terlewat
    s = sorted(LOG_DIR.glob("run_id=backfill__*"))
    s.reverse()
    for run_dir in s:
        run_id = run_dir.name.replace("run_id=", "")
        if run_id not in printed_tasks:
            print("incoming", run_id)

if __name__ == "__main__":
    main()
