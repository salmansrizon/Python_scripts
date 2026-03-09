"""
DEPRECATED: This script has been replaced by Prefect-based scheduling.

See: prefect_scheduler.py for the modern Prefect-native implementation.

This file is kept for reference only. The Prefect scheduler provides:
  ✓ Robust retry logic with exponential backoff
  ✓ Persistent task history and run logs
  ✓ Web UI for monitoring and manual triggers
  ✓ No daemon thread issues (daily jobs now run reliably)
  ✓ Better error handling and notifications

To use Prefect instead of this script:
  1. Run: prefect deploy
  2. Start a worker: prefect worker start
  3. View dashboard: prefect server start (then http://localhost:4200)
"""

import time
import subprocess
import threading
import re

# Use PollingObserver for cloud-synced folders like OneDrive
from watchdog.observers.polling import PollingObserver
from watchdog.events import FileSystemEventHandler
import schedule


# Path to Python executable in your virtual environment
VENV_PYTHON = r'C://Python_scripts//.venv//Scripts//python.exe'

# Map of watched folders to their respective processing scripts
WATCHED_FOLDERS = {
    r'C://Users//metabase//OneDrive - Shoplover Ltd//3pl_partner//': r'C://Python_scripts//scripts//3pl_script.py',
    # Add more folder/script pairs as needed
}

# List of scripts to run on a schedule, with interval in hours or specific time of day
SCHEDULED_SCRIPTS = [
    # {'path': r'C://Python_scripts//scripts//cr_data.py', 'interval': 12},
    # {'path': r'C://Python_scripts//scripts//overall_pkg_journey.py', 'interval': 6},
    # {'path': r'C://Python_scripts//scripts//3pl_script.py', 'interval': 24},
    # {'path': r'C://Python_scripts//scripts//category_tree.py', 'interval': 168},
    # {'path': r'C://Python_scripts//scripts//order_master.py', 'interval': 1},
    # {'path': r'C://Python_scripts//scripts//payment_order.py', 'interval': 24},

    # Example of daily execution at a specific time
    # {'path': r'C://Python_scripts//scripts//mkt_summary.py', 'time_of_day': '10:00'},
    # {'path': r'C://Python_scripts//scripts//all_sku_list.py', 'time_of_day': '06:00'},
    # {'path': r'C://Python_scripts//scripts//customer_risk_data.py', 'time_of_day': '20:00'}
    {'path': r'C://Python_scripts//scripts//risk_raw_data.py', 'time_of_day': '03:30'},
    {'path': r'C://Python_scripts//scripts//risk_engine.py', 'time_of_day': '04:30'},
    {'path': r'C://Python_scripts//scripts//mkt_com_dump.py', 'time_of_day': '08:30'},
    {'path': r'C://Python_scripts//scripts//mkt_summary.py', 'time_of_day': '09:30'}
]


class FolderSpecificHandler(FileSystemEventHandler):
    """Handles file system events for a specific folder."""
    def __init__(self, script_path):
        self.script_path = script_path

    def on_created(self, event):
        if not event.is_directory:
            print(f"New file detected: {event.src_path}")
            try:
                subprocess.run(
                    [VENV_PYTHON, self.script_path, event.src_path],
                    check=True
                )
            except subprocess.CalledProcessError as e:
                print(f"Error executing script: {e}")


def is_valid_time(time_str):
    """Validate time string in HH:MM format."""
    return re.match(r'^([01]\d|2[0-3]):([0-5]\d)$', time_str) is not None


def run_script(script_path):
    """Run a single script."""
    print(f"Scheduled task triggered: {script_path}")
    try:
        subprocess.run([VENV_PYTHON, script_path], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running scheduled script {script_path}: {e}")


def run_scheduled_scripts():
    """Runs all scheduled scripts based on their interval or time of day."""
    while True:
        schedule.run_pending()
        time.sleep(1)


def start_scheduler():
    """Set up and start background thread for scheduled scripts."""
    for task in SCHEDULED_SCRIPTS:
        script_path = task['path']

        if 'interval' in task:
            interval = task['interval']
            print(f"Scheduling {script_path} every {interval} hours")
            schedule.every(interval).hours.do(run_script, script_path=script_path)

        elif 'time_of_day' in task:
            time_of_day = task['time_of_day']
            if not is_valid_time(time_of_day):
                raise ValueError(f"Invalid time_of_day format: {time_of_day}")
            print(f"Scheduling {script_path} daily at {time_of_day}")
            schedule.every().day.at(time_of_day).do(run_script, script_path=script_path)

    print("Scheduler started.")
    thread = threading.Thread(target=run_scheduled_scripts)
    thread.daemon = True
    thread.start()


def start_watcher():
    """Start watching folders for new files using PollingObserver."""
    observer = PollingObserver(timeout=28800)  # Poll every 8 hours

    for folder, script in WATCHED_FOLDERS.items():
        print(f"Watching: {folder} -> Script: {script}")
        event_handler = FolderSpecificHandler(script)
        observer.schedule(event_handler, path=folder, recursive=False)

    observer.start()
    print("File watcher running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping watcher...")
        observer.stop()

    observer.join()


if __name__ == "__main__":
    # Start the scheduler in a background thread
    start_scheduler()

    # Start the file watcher (this blocks the main thread)
    start_watcher()