import random
import plistlib
import os
import time
import subprocess
from datetime import datetime
from Config.constants import PATH


#IMPORTANT PATHS 
project_path = '/Users/focus_profond/GIT_repo/flight_price_tracker'
#ne pas oublier de rajouter le bin/python --> sinon on arrive pas à activer notre virtuel env
virtual_env_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/flight_env/bin/python'
script_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Scripts/scrapper/main_scrapper.py'
#script_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/loop.py'
log_path = '/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Pipeline/schedule_scrapping'
#on veut le run comme script utilisateur et non root
plist_file_path = '/Users/focus_profond/Library/LaunchAgents/'
plist_name = "com.user.main_scrapper_daily.plist"
plist_file_full_path = plist_file_path+plist_name

#FIRST WE CREATE A FUNCTION TO GENERATE RANDOMLY AN HOUR AND A MINUTE TO EXECUTE THE SCRIPT
def generate_random_hour_and_minute():
    hour = random.randint(12, 16)  # de 12h à 16h (inclus)
    minute = random.randint(0, 59)
    return hour, minute

# We create a file .plist (which is a string)
def generate_plist(hour, minute,project_path, virtual_env_path, script_path, log_path):
    
    label = 'com.user.main_scrapper_daily'
    program_args = [virtual_env_path, script_path]
   #pogram_args = ['/Users/focus_profond/GIT_repo/flight_price_tracker/loop.py']
    calendar_execution = {"Hour":hour, "Minute":minute}
    log_out = log_path+'/main_scrapper_output.log'
    log_err = log_path+'/main_scrapper_errors.log'
    plist_dict = {
        "Label" : label
        ,"ProgramArguments":program_args
        ,"StartCalendarInterval":calendar_execution
        ,"RunAtLoad": False
        ,'KeepAlive': False
        ,"StandardOutPath": log_out
        ,"StandardErrorPath": log_err
    }

    with open(plist_file_full_path, "wb") as f:
        plistlib.dump(plist_dict, f)
    
    print(f"✅ Fichier .plist généré : {plist_file_full_path}")

def unloading_plist(plist_name= plist_file_full_path):
    if not os.path.exists(plist_file_path):
        print("Plist file does not exist")
        return
    else:
        try:
            subprocess.run(["launchctl", "unload", plist_name],  stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
            print("Plist unload with success.")
            time.sleep(1)
        except subprocess.CalledProcessError as e:
            print(f"⚠️ Error during the unload : {e}")

def loading_plist(plist_name= plist_file_full_path):
    try:
            result = subprocess.run(["launchctl", "load", plist_name],  capture_output=True, text=True, check=True)
            if result.returncode == 0:
                print("Succes with the loading of the task.")
            else:
                print(f"Error during execution launchd: {result.stderr}")
    except subprocess.CalledProcessError as e:
            print(f"⚠️ Erreur pendant le load : {e}")

def log_execution_status(success: bool, message: str):
    status_log =  log_path+'/main_scrapper_execution_status.log'   #os.path.join(log_path, "execution_status.log")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "SUCCES" if success else "ECHEC"
    with open(status_log, "a") as f:
        f.write(f"{timestamp} - {status} - {message}\n")

def main():
    hour,minute= generate_random_hour_and_minute()
    #minute = 53
   #hour = 12
    unloading_plist(plist_file_full_path)
    generate_plist(hour, minute,project_path,virtual_env_path,script_path,log_path)
    loading_plist(plist_file_full_path) 
    # On logue ici que la tâche a bien été programmée
    log_execution_status(True, f"Tâche programmée pour {hour:02d}:{minute:02d}")
     
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

