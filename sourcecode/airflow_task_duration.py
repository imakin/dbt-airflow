#!/usr/bin/env python3
"""
Measure task duration from logs
@author: izzulmakin 2025
"""
import os
import re
from datetime import datetime
from collections import defaultdict

from dags.scripts.utils import BASE_DIR
os.chdir(BASE_DIR)

LOG_DIR = "logs/dag_id=nyc_taxi_monitor"

def parse_log_for_duration(log_file):
    """Extract start & end time dari log"""
    try:
        with open(log_file, 'r') as f:
            content = f.read()
        
        # Find start_date and end_date
        start_match = re.search(r'start_date=(\d{8}T\d{6})', content)
        end_match = re.search(r'end_date=(\d{8}T\d{6})', content)
        
        if start_match and end_match:
            start_str = start_match.group(1)
            end_str = end_match.group(1)
            
            start_time = datetime.strptime(start_str, '%Y%m%dT%H%M%S')
            end_time = datetime.strptime(end_str, '%Y%m%dT%H%M%S')
            
            duration = (end_time - start_time).total_seconds()
            return duration
    except Exception as e:
        pass
    
    return None

def main():
    task_durations = defaultdict(list)

    for run_dir in os.listdir(LOG_DIR):
        if not run_dir.startswith("run_id=backfill"):
            continue
        
        run_path = os.path.join(LOG_DIR, run_dir)
        
        for task_dir in os.listdir(run_path):
            if not task_dir.startswith("task_id="):
                continue
            
            task_name = task_dir.replace("task_id=", "")
            task_path = os.path.join(run_path, task_dir)
            
            # cari log file
            for log_file in os.listdir(task_path):
                if log_file.endswith(".log"):
                    log_path = os.path.join(task_path, log_file)
                    duration = parse_log_for_duration(log_path)
                    
                    if duration is not None:
                        task_durations[task_name].append(duration)
    
    print(f"{'Task Name':<30} {'Count':<8} {'Avg (s)':<10} {'Min (s)':<10} "
          f"{'Max (s)':<10}")
    
    # sort average desc
    sorted_tasks = sorted(task_durations.items(), 
                         key=lambda x: sum(x[1])/len(x[1]) if x[1] else 0, 
                         reverse=True)
    
    for task_name, durations in sorted_tasks:
        if durations:
            avg_dur = sum(durations) / len(durations)
            min_dur = min(durations)
            max_dur = max(durations)
            count = len(durations)
            
            # print dgn spasi
            print(f"{task_name:<30} {count:<8} {avg_dur:<10.2f} "
                  f"{min_dur:<10.2f} {max_dur:<10.2f}")

if __name__ == "__main__":
    main()
