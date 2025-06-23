# logger.py

import csv
import os
import uuid
from datetime import datetime

# === CONFIGURATION ===
LOG_DIR = "Logs"
LOG_FILE = os.path.join(LOG_DIR, "job_monitoring_log.csv")
AUTHOR = "Augustin"

# Colonnes fixes
LOG_FIELDS = [
    "run_id", "job_name", "status", "start_time", "end_time", "duration_s",
    "rows_processed", "error_message", "table_name", "source", "author", "created_at"
]

# Variable globale (on la réutilise pour chaque étape du pipeline)
RUN_ID = None
START_TIMES = {}  # start_time par job_name


def init_run(run_id: str = None):
    """Initialise le run_id, utilisé pour tracer un run complet."""
    #on utilise une variable globale pour qu'elle soit réutilisée entre les différentes fonctions
    global RUN_ID
    if run_id:
        RUN_ID = run_id
    else:
        RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]
    return RUN_ID


def log_job_start(job_name: str, source: str, table_name: str = None):
    """Démarre le log d'un job en stockant son heure de début."""
    START_TIMES[job_name] = datetime.now()


def log_job_end(job_name: str, status: str, rows_processed: int = None,
                error_message: str = None, table_name: str = None, source: str =None):
    """Finalise le log d’un job et écrit une ligne dans le fichier CSV."""
    if job_name not in START_TIMES:
        raise ValueError(f"Job '{job_name}' n’a pas été démarré avec log_job_start()")

    start_time = START_TIMES[job_name]
    end_time = datetime.now()
    duration_s = round((end_time - start_time).total_seconds(), 3)

    log_row = {
        "run_id": RUN_ID,
        "job_name": job_name,
        "status": status,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_s": duration_s,
        "rows_processed": rows_processed,
        "error_message": error_message,
        "table_name": table_name,
        "source": source,
        "author": AUTHOR,
        "created_at": end_time.isoformat(),
    }

    _write_log(log_row)


def _write_log(row: dict):
    """Écrit la ligne de log dans le fichier CSV (append)."""
    os.makedirs(LOG_DIR, exist_ok=True)
    file_exists = os.path.isfile(LOG_FILE)

    with open(LOG_FILE, mode="a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
