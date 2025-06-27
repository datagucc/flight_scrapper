import random
import plistlib
import os
import sys
import time
import subprocess
from datetime import datetime
# ATTENTION ICI ON TRICHE CAR ON ECRIT LE CHEMIN EN DUR, NORMALEMENT ON DEVRAIT FAIRE CA DIFFERMEENT !!!
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
if project_path not in sys.path:
    sys.path.append(project_path)
from Config.constants import PATH


#HARD CODED PATHS 
#virtual_env_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/flight_env/bin/python'
#script_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Scripts/scrapper/script_scrapping.py'
#script_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/loop.py'
#log_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Pipeline/schedule_scrapping'
#on veut le run comme script utilisateur et non root
#plist_file_path = '/Users/focus_profond/Library/LaunchAgents/'
#plist_name = "com.user.main_scrapper_daily.plist"
#plist_file_full_path = plist_file_path+plist_name

# DYNAMIC PATHS
virtual_env_path = PATH['venv_envi_path']
name_script = 'script_scrapping.py'
script_path = f"{PATH['scripts_path']}/scrapper/{name_script}"
log_path = f"{PATH['logs_path']}/Scrapping/Scheduling"
plist_file_path = PATH['plist_file_path']
plist_name = "com.user.main_scrapper_daily.plist"
plist_file_full_path = plist_file_path+plist_name



def generate_random_hour_and_minute():
    """
    Generate a random time between 12:00 and 15:59.
    Returns:
        tuple: A pair of integers (hour, minute) where hour in[12, 16] and minute in [0, 59].
    """
    
    hour = random.randint(12, 15)  # de 12h à 16h (inclus)
    minute = random.randint(0, 59)
    return hour, minute


def generate_plist(hour, minute,project_path, virtual_env_path, script_path, log_path):
    """
    Generate a launchd-compatible .plist file to schedule a Python script execution.

    Args:
        hour (int): Hour of the day (0–23) to schedule the script.
        minute (int): Minute of the hour (0–59) to schedule the script.
        project_path (str): Base directory where the .plist file will be saved.
        virtual_env_path (str): Path to the Python interpreter inside the virtual environment.
        script_path (str): Path to the Python script to execute.
        log_path (str): Directory where log files (stdout/stderr) will be written.

    Side Effects:
        Writes a .plist file to `~/Library/LaunchAgents` (or other target path).
    """
    
    label = 'com.user.main_scrapper_daily'

    # The Python executable (here the virtual env) and the target script to run
    program_args = [virtual_env_path, script_path]

    # Time-based schedule for the job
    calendar_execution = {"Hour":hour, "Minute":minute}
    #Logs file paths
    #log_out = log_path+'/main_scrapper_output.log' f"{PATH['logs_path']}/Scrapping/Execution_logs/main_scrapper_output.log"
    #log_err = log_path+'/main_scrapper_errors.log'
    log_out = f"{PATH['logs_path']}/Scrapping/Execution_logs/main_scrapper_output.log"
    log_err = f"{PATH['logs_path']}/Scrapping/Execution_logs/main_scrapper_errors.log"

    plist_dict = {
        "Label" : label
        ,"ProgramArguments":program_args
        ,"StartCalendarInterval":calendar_execution
        ,"RunAtLoad": False
        ,'KeepAlive': False
        ,"StandardOutPath": log_out
        ,"StandardErrorPath": log_err
    }

    # Save to user's LaunchAgents folder
    with open(plist_file_full_path, "wb") as f:
        plistlib.dump(plist_dict, f)
    
    print(f"File .plist  generated : {plist_file_full_path} ; ")

def unloading_plist(plist_name= plist_file_full_path):
    """
    Unload a launchd .plist file using launchctl.

    Args:
        plist_name (str): Full path to the .plist file to be unloaded.

    Side Effects:
        Attempts to unload the .plist using `launchctl`. Logs status to stdout.
    """
    if not os.path.exists(plist_file_path):
        print("Plist file does not exist.")
        return
    else:
        try:
            subprocess.run(["launchctl", "unload", plist_name],  stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            print("Plist unload successfully ;")
            time.sleep(1)
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Error during the unload : {e}")


def loading_plist(plist_name= plist_file_full_path):
    """
    Load a launchd .plist file using launchctl.

    Args:
        plist_name (str): Full path to the .plist file to load.

    Side Effects:
        Attempts to load the .plist into launchd. Logs success or error to stdout.
    """

    try:
            result = subprocess.run(["launchctl", "load", plist_name],  capture_output=True, text=True, check=True)
            if result.returncode == 0:
                print("✅Task loaded successfully into launchd.")
                print('--------------------------------------------------')
            else:
                print(f"Error during execution launchd: {result.stderr}")
    except subprocess.CalledProcessError as e:
            print(f"⚠️ Error during load : {e}")

def log_execution_status(success: bool, message: str):
    """
    Append execution status (success/failure) with timestamp and message to a log file.

    Args:
        success (bool): True if execution was successful, False otherwise.
        message (str): Custom message describing the context or outcome.
        log_path (str): Directory where the status log will be stored.

    Side Effects:
        Appends a line to `main_scrapper_execution_status.log` in the given log_path.
    """

    status_log =  log_path+'/auto_scheduling_main_scrapper_plist.log'   
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "✅ SUCCESS" if success else "❌ FAILURE"
    with open(status_log, "a") as f:
        f.write(f"{timestamp} - {status} - {message}\n")
        f.write("--------------------------------------------------\n")

def main():
    hour,minute= generate_random_hour_and_minute()
    #minute = 25
    #hour = 22
    unloading_plist(plist_file_full_path)
    generate_plist(hour, minute,project_path,virtual_env_path,script_path,log_path)
    loading_plist(plist_file_full_path) 
    log_execution_status(True, f"Task scheduled for : {hour:02d}:{minute:02d}")
     
if __name__ == "__main__":
     main()





#EXAMPLE XML SCRIPT 
xml_script = """
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
 "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>

  <!-- Nom du job -->
  <key>Label</key>
  <string>com.user.auto_script</string>

  <!-- Commande exécutée -->
  <key>ProgramArguments</key>
  <array>
    <string>/Users/ton_nom/GIT_repo/flight_price_tracker/flight_env/bin/python</string>
    <string>/Users/ton_nom/GIT_repo/flight_price_tracker/Scripts/auto_script.py</string>
  </array>

  <!-- Exécute au démarrage -->
  <key>RunAtLoad</key>
  <true/>

  <!-- Exécution quotidienne à 9h58 -->
  <key>StartCalendarInterval</key>
  <dict>
    <key>Hour</key><integer>9</integer>
    <key>Minute</key><integer>58</integer>
  </dict>

  <!-- Redirection des logs -->
  <key>StandardOutPath</key>
  <string>/tmp/auto_script.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/auto_script.err</string>

</dict>
</plist>
"""

