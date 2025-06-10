import random
import plistlib
import os
import subprocess
from datetime import datetime

HOME = "/Users/focus_profond/GIT_repo/flight_price_tracker"
plist_dir = os.path.join(os.path.expanduser("~"), "Library", "LaunchAgents")
plist_name = "com.user.main_scrapper_daily.plist"
plist_path = os.path.join(plist_dir, plist_name)

python_venv = os.path.join(HOME, "flight_env", "bin", "python")
main_scrapper_path = os.path.join(HOME, "Scripts", "scrapper", "main_scrapper.py")
log_dir = os.path.join(HOME, "Logs", "auto_schedule")
os.makedirs(log_dir, exist_ok=True)

def generate_random_hour_and_minute():
    hour = random.randint(10, 16)  # de 10h à 16h (inclus)
    #pour tester une heure fixe
    #hour = 16
    minute = random.randint(0, 59)
    #minute = 5
    return hour, minute

def create_plist(hour, minute):
    log_out = os.path.join(log_dir, "main_scrapper.out.log")
    log_err = os.path.join(log_dir, "main_scrapper.err.log")

    plist_content = {
        'Label': 'com.user.main_scrapper_daily',
        'ProgramArguments': [
            python_venv,
            main_scrapper_path
        ],
        'StartCalendarInterval': {
            'Hour': hour,
            'Minute': minute,
        },
        'StandardOutPath': log_out,
        'StandardErrorPath': log_err,
        'RunAtLoad': False,
        'KeepAlive': False,
    }

    os.makedirs(plist_dir, exist_ok=True)

    with open(plist_path, 'wb') as f:
        plistlib.dump(plist_content, f)

    print(f"Plist créé pour exécution à {hour:02d}:{minute:02d}")
    print(f"Logs stdout : {log_out}")
    print(f"Logs stderr : {log_err}")

def load_plist():
    # Décharge si déjà chargé pour éviter doublons
    subprocess.run(['launchctl', 'unload', plist_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # Charge la nouvelle tâche
    result = subprocess.run(['launchctl', 'load', plist_path], capture_output=True, text=True)
    if result.returncode == 0:
        print("Tâche launchd chargée avec succès.")
    else:
        print(f"Erreur lancement launchd: {result.stderr}")

def log_execution_status(success: bool, message: str):
    status_log = os.path.join(log_dir, "execution_status.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "SUCCES" if success else "ECHEC"
    with open(status_log, "a") as f:
        f.write(f"{timestamp} - {status} - {message}\n")

def main():
    hour, minute = generate_random_hour_and_minute()
    create_plist(hour, minute)
    load_plist()
    # On logue ici que la tâche a bien été programmée
    log_execution_status(True, f"Tâche programmée pour {hour:02d}:{minute:02d}")

if __name__ == "__main__":
    main()
