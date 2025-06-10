#Import libraries
import sys
import os
import shutil
import time
import pandas as pd
#test OCR
import pytesseract
import cv2
from PIL import Image
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import calendar
from deltalake import write_deltalake, DeltaTable
from deltalake.table import TableOptimizer
from deltalake.exceptions import TableNotFoundError
import re
import openpyxl



# Add the path to the modules directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

#Importing personal modules
#from DF_functions import *
from OCR_google_flight import *
from Modules.metadata_functions import *

# faire de l'OCR sur un dossier.

start_time = time.time()

my_folder= '/Users/focus_profond/GIT_repo/flight_price_tracker/Data/raw/screenshots/2025-05-26'
names = os.listdir(my_folder)
for name in names:
    if name != '.DS_Store':
        bif_df = ocr_on_folder(f'{my_folder}/{name}/')
        storing_data(bif_df)

end_time = time.time()
print('totale duration : ',round(end_time - start_time, 2))
