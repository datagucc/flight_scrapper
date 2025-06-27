#Import libraries
import sys
#import os
import time
import logging
from datetime import datetime
# Add the path to the modules directory
# ATTENTION ICI ON TRICHE CAR ON ECRIT LE CHEMIN EN DUR, NORMALEMENT ON DEVRAIT FAIRE CA DIFFERMEENT !!!
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if project_path not in sys.path:
    sys.path.append(project_path)
from Scripts.pipeline import bronze_layer, silver_layer, gold_layer
from Config.constants import PATH
# On va scheduler notre pipeline pour qu'il tourne une fois par jour, via un crontab
# Et je vais executer le script sous l'environnement virtuel, directement dans le crontab.

# Setup du logging
data_path = PATH['data_path']
log_path = PATH['logs_path']
folder_path  = f"{log_path}/Pipeline/Execution_logs/"


#storing the logs
log_filename = datetime.now().strftime(f"{folder_path}cron_main_pipeline_%Y-%m-%d %-Hh%-M.log")  
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if __name__ == "__main__":
    try:

        start_time = time.time()
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        print('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
        print(f"Starting the main pipeline script at {timestamp}")
        print("▶️ Starting Bronze Layer...")
        log_bronze = bronze_layer.main()
        print("✅ Bronze Layer completed.\n")
        print("▶️ Starting Silver Layer...")
        log_silver = silver_layer.main()
        print("✅ Silver Layer completed.\n")

        print("▶️ Starting Gold Layer...")
        log_gold = gold_layer.main()
        print("✅ Gold Layer completed.\n")
        with open(log_filename, "a") as f:
            f.write(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX")
            f.write(f"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX\n")
            f.write(f"The main pipeline script started at {timestamp}")
            f.write(log_bronze)
            f.write(log_silver)
            f.write(log_gold)
            f.write("🎉 All layers executed successfully.")


    except Exception as e:
        logging.exception("❌ An error occurred during pipeline execution")
        print("❌ An error occurred. Check the log for details.")

    finally:
        #storing the logs
        end_time = time.time()
        total_duration = round(end_time - start_time, 2)
        status_log =  log_path+'/Pipeline/Scheduling/main_pipeline.log'   
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(status_log, "a") as f:
            f.write(f"{timestamp} - 3 layers processed - {total_duration} en secondes.\n")
