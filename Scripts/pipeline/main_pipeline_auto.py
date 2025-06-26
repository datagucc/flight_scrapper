#Import libraries
import sys
import os
import time
import logging
from datetime import datetime
# Add the path to the modules directory
sys.path.append('/Users/focus_profond/GIT_repo/flight_price_tracker')
from Scripts.pipeline import bronze_layer, silver_layer, gold_layer

# On va scheduler notre pipeline pour qu'il tourne une fois par jour, via un crontab
# Et je vais executer le script sous l'environnement virtuel, directement dans le crontab.

# Setup du logging
folder_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Pipeline/schedule_pipeline/crontab_logs/'
log_filename = datetime.now().strftime(f"{folder_path}cron_main_pipeline_%Y-%m-%d %-Hh%-M.log")
logging.basicConfig(
    filename=log_filename,
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)

def log_and_print(message):
    print(message)
    logging.info(message)


if __name__ == "__main__":
    try:
        log_and_print("▶️ Starting Bronze Layer...")
        bronze_layer.main()
        log_and_print("✅ Bronze Layer completed.\n")

        log_and_print("▶️ Starting Silver Layer...")
        silver_layer.main()
        log_and_print("✅ Silver Layer completed.\n")

        log_and_print("▶️ Starting Gold Layer...")
        gold_layer.main()
        log_and_print("✅ Gold Layer completed.\n")

        log_and_print("🎉 All layers executed successfully.")

    except Exception as e:
        logging.exception("❌ An error occurred during pipeline execution")
        print("❌ An error occurred. Check the log for details.")
