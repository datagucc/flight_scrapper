import random
import datetime
#pour lancer des commandes système, dans notre cas "at"
import subprocess
 

# === CONFIGURATION ===
#SCRIPT_PATH = "/Users/focus_profond/GIT_repo/flight_price_tracker/Scripts/scrapper/main_scrapper.py"  # TO MODIFY IF PATH IS DIFFERENT
# tester avec un script simple et bidon
SCRIPT_PATH = "/Users/focus_profond/GIT_repo/flight_price_tracker/Scripts/scrapper/my_test_script.py"

VENV_PATH = "/Users/focus_profond/GIT_repo/flight_price_tracker/flight_env"  # TO MODIFY IF PATH IS DIFFERENT
PYTHON_CMD = "python3"  # ou "poetry run python3", etc. selon ton environnement

# === 1. GÉNÉRER UNE HEURE ALÉATOIRE ENTRE 8H ET 18H ===
#hour = random.randint(10, 17)  
hour = 12 # pour tester à une heure précise
#minute = random.randint(0, 59)
minute = 55 #pour tester à une heure précise 


# Formater l’heure pour `at`
# on formatte l'heure pour qu'elle soit au format HH:MM
run_time = f"{hour:02d}:{minute:02d}"

# === 2. Commande pour activer l'environnement virtuel et pour lancer le script python ===
#command = f"source {VENV_PATH}/bin/activate && python {SCRIPT_PATH}"

#pour afficher la sortie de ma commande 'atq' dans un fichier log
command = f"source {VENV_PATH}/bin/activate && python {SCRIPT_PATH} > /Users/focus_profond/GIT_repo/flight_price_tracker/Logs/auto_schedule/at_log.txt 2>&1 "



# === 3. LANCER LA COMMANDE AVEC at ===
try:
    #subprocess.run = execute la commande sysètme comme si on était dans le terminal
    # input = injecte le contenu du script à exécuter (la commande = command)
    #capture output= pour récuperer les sorties (stdout et stderr) si besoin
    # check = True pour lever une erreur si at échoue
    #at_process = subprocess.run(['at', run_time], input=command.encode(), capture_output=True, check=True, text=True)
    at_process = subprocess.run(['at', run_time], input=command, capture_output=True, check=True, text=True)

    #pour s'entrainer sur un scirpt simple
    #at_process = subprocess.run(['at', run_time], input=command.encode(), capture_output=True, stdout=open("test_output_gus.log", 'a'), check=True)
    print(f"✅ Bot scheduled today at {run_time} to run: {command}")
    # affiche la sortie standard de la commande ar, qui est en général du type : job 7 at Mon May 27 14:42:00 2025
    #print(at_process.stdout.decode().strip())


    print("stdout:", at_process.stdout.strip())
    print("stderr:", at_process.stderr.strip())

    # ce qu'il se passe si la commande at échoue :
except subprocess.CalledProcessError as e:
    print("❌ Échec de la programmation avec `at`.")
    print(e.stderr.decode().strip())


# Pour vérifier que le job a bien été programmé, on peut utiliser la commande `atq`, dans notre terminal