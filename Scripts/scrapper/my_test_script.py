from datetime import datetime
log_file = "/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/auto_schedule/main_scrapper_test_gus.log"

with open(log_file, "a") as f:
    f.write(f"{datetime.now().isoformat()} - main_scrapper.py executed successfully\n")
