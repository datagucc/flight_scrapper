# Script to automate Google Flights calendar screenshot capturing using Playwright.
# This module simulates user interactions to collect visual data from the calendar interface.

import time
import datetime
from dateutil.relativedelta import relativedelta
import os
import sys
import csv
import json
import random
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

#from playwright.sync_api import Page, PlaywrightTimeoutError
from playwright.sync_api import Page, TimeoutError
from Scripts.scrapper.modules.utils import launch_browser, handle_cookies_popup, go_to_url,move_mouse_randomly
from Config.constants import SELECTORS, PATH 

#I HARDCODED THE PATH TO THE DEBUG SCREENSHOTS

def click_departure_date(page: Page):
    """
    Opens the departure date calendar by clicking on the corresponding UI element.

    Args:
        page (Page): The Playwright page object currently open.

    Raises:
        Exception: If the departure input cannot be found or clicked.

        WAIT MAX TOTAL = 29,5 secondes
    """
    try:

        # Petite pause pour éviter un comportement de bot (attente aléatoire): max 1 sec 
        page.wait_for_timeout(500 + int(450 * random.random()))
        # Ensure the page is interactive by clicking a neutral area : max 0.5 sec
        page.mouse.click(int(200 * random.random()), int(200 * random.random()))
        # Petite pause pour éviter un comportement de bot (attente aléatoire) : max 1,05 sec
        page.wait_for_timeout(500 + int(550 * random.random()))

        # Wait for the "Departure" label and click it
        input_departure_label = SELECTORS['input_departure_label']

        #we try to wait for the selector, and if it does not appear, we still try to continue
        try:
            #wait max 10 secondes 
            page.wait_for_selector(input_departure_label, timeout=10000)
        except:
            print("timeout exceeded")

        #as we had a lot of issue with this locator, we also try to look at the second departure
        #departure_input = page.locator(input_departure_label).first
        locator = page.locator(input_departure_label)
        try:
            # Essaye le premier élément
            departure_input = locator.nth(0)
            # wait max 10 secondes
            departure_input.wait_for(state="visible", timeout=10000)
        except TimeoutError:
            try:
                # Si le premier échoue, essaye le deuxième
                departure_input = locator.nth(1)
                # wait max 5 secondes 
                departure_input.wait_for(state="visible", timeout=5000)
            except Exception as e:
                print(f"Aucun champ de départ visible trouvé avec le sélecteur : {input_departure_label}")
                raise e



        # Scroll jusqu'à l'élément si nécessaire (pour éviter les erreurs si l'élément est hors de l'écran)
        departure_input.scroll_into_view_if_needed()
        # wait max 0.5 secondes
        time.sleep(0.2 + random.random() * 0.3)

        #click simulé avec un petit délai
        # Hover pour simuler un comportement humain (ca correspond à un survol de la souris)
        departure_input.hover()
        # wait max 0.5 secondes
        time.sleep(0.2 + random.random() * 0.3)
        departure_input.click()
        #Petite pause pour éviter un comportement de bot (attente aléatoire)
        # wait max 1.1 secondes
        page.wait_for_timeout(500 + int(600 * random.random()))
    except Exception as e:
        # Capture screenshot on failure to aid debugging
        timestampstr = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_S')
        page.screenshot(path=f"/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Scrapping/Screenshots/debug_click_departure_{timestampstr}.png", full_page=True)
        print("[❌ Erreur] Impossible de cliquer sur le champ de date de départ.")
        raise e


def click_next_page(page: Page):
    """
    Clicks the "Next" button on the calendar to switch to the next month.

    Args:
        page (Page): The Playwright page object.

    Raises:
        Exception: If the next button is not found or the click fails.

        WAIT MAX TOTAL = 14 secondes
    """
    try:
        # wait max 1,4 secondes
        page.wait_for_timeout(600+random.randint(200, 800)) #to simulate human behavior
        next_button_label = SELECTORS['next_button_label']

        # wait max 10 secondes
        page.wait_for_selector(next_button_label, timeout=10000)
        next_input = page.locator(next_button_label).first

        # Simulation réaliste : scroll + hover + clic avec délais randomisés
        next_input.scroll_into_view_if_needed()
        #wait max 0.5 secondes
        page.wait_for_timeout(200 + random.randint(100, 300))
        next_input.hover()
        #wait max 0.3 secondes
        page.wait_for_timeout(100 + random.randint(50, 200))

        next_input.click()

        # Pause après clic pour attendre le rechargement de l'UI
        # wait max 1,8 secondes
        page.wait_for_timeout(1000 + random.randint(400, 800))
    except Exception as e:
        # Capture screenshot on failure to aid debugging
        timestampstr = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        page.screenshot(path=f"/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Scrapping/Screenshots/debug_next_page_{timestampstr}.png", full_page=True)
        print("[❌ Erreur] Impossible de cliquer sur le bouton 'Next'.")
        raise e


def take_screenshot_ele(
        page: Page
        ,current_date: str
        ,trip: str
        ,name_image: str
        ,nb: int = 0
        ,delay_range_ms: tuple=(400,800)):
    """
    Takes a screenshot of a specific calendar section (month).

    Args:
        page (Page): The Playwright page object.
        current_date (str): Date when the script is executed (used for folder naming).
        trip (str): Trip name used to categorize screenshots.
        name_image (str): Base name for the screenshot file.
        nb (int): Index of the calendar container to target (default is 0).

        WAIT MAX TOTAL = 11 secondes
    """
    try:
        calendar_container = SELECTORS['calendar_container']
        element = page.locator(calendar_container).nth(nb)
        final_path = f"{PATH['screenshot_path']}/{current_date}/{trip}/{name_image}.png"
        

        
        # Attendre que l'élément soit visible : max 10 secondes
        element.wait_for(state="visible", timeout=10000)

        # Pause réaliste avant capture : max 1 secondes
        wait_ms = random.randint(*delay_range_ms)
        page.wait_for_timeout(wait_ms)

        # Ensure target directory exists
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        element.screenshot(path=final_path)
        print(f"📸 Screenshot saved to {final_path}")

    except Exception as e:
        timestampstr = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        page.screenshot(path=f"/Users/focus_profond/GIT_repo/flight_price_tracker/Logs/Scrapping/Screenshots/debug_screenshot_{timestampstr}.png", full_page=True)
        print(f"[❌ ERREUR] Échec de la capture pour {trip} → {name_image}: {e}")
        raise e



def scrapping_url(
    url: str
    ,trip: str
    ,month_to_capture: int = 12
    ,headless: bool = True
    ,waiting_time: tuple=(2.5,5)
) -> dict:
    """
    Automates browsing to a Google Flights calendar and captures screenshots for multiple months.

    Args:
        url (str): Full URL to the Google Flights search page.
        trip (str): Identifier for the trip used in naming screenshots.
        month_to_capture (int): Number of months to capture (default: 12).
        headless (bool): Whether to run the browser in headless mode (default: True).
        waiting_time (int): Seconds to wait after rendering before taking screenshots (default: 3).

    Returns:
        dict: Summary information for logging and monitoring.

        WAIT MAX TOTAL = 400 secondes (grand max ) --> max 7 minutes par trip
    """
    date_obj = datetime.datetime.now()
    current_date_full = date_obj.strftime("%Y-%m-%d")
    start_time = time.time()
    log_path = f"{PATH['log_path']}/Scrapping/Logs/scraping_log_{current_date_full}.csv"

    log_data = {
        "trip": trip,
        "date_scrapped":current_date_full,
        "start_time": datetime.datetime.now().isoformat(),
        "end_time": None,
        "duration_sec": None,
        "total_months": month_to_capture,
        "errors": [],
        "status": "started",
        "url": url
    }



    try:
        #PHASE 1 : BROWSER
        try:
            playwright, browser, context, page = launch_browser(headless=headless)
        except Exception as e:
            log_data["errors"].append({"phase":"launch_browser","error":str(e)})
            raise

        #PHASE 2: NAVIGATION
        try:
            go_to_url(page, url)
        except Exception as e:
            log_data["errors"].append({"phase":"go_to_url","error":str(e)})
            raise

        #elle ne sert à rien ici car jamais de cookies
        #handle_cookies_popup(page)
        #move_mouse_randomly(page)

        #PHASE 3 : DATE PICKER : MAX 30 secondes d'attente 
        try:
            click_departure_date(page)
        except Exception as e:
            log_data["errors"].append({"phase":"click_departure_date","error":str(e)})
            raise
    
        
        #PHASE 4 : MONTH LOOP : max 32 secondes par boucle, donc x12 = 360 secondes 
        for i in range(month_to_capture):
            try:
                #attente réalistelc
                wait_seconds = random.uniform(*waiting_time)
                # wait max 5 secondes 
                page.wait_for_timeout(wait_seconds * 1000)

                #formattage du nom de l'image
                month_year = (date_obj + relativedelta(months=i)).strftime("%m_%Y")
                image_name = f"{current_date_full}_XxX_{trip}_XxX_{month_year}"

                #on prend un screenshot de l'élément du calendrier
                #move_mouse_randomly(page) : max 11 secondes 
                take_screenshot_ele(page, current_date_full, trip, image_name, nb=i, delay_range_ms=(400,900))

                if i < 10:
                    # Pause humaine
                    # wait max 2 secondes 
                    page.wait_for_timeout(random.randint(800, 2100))
                    #move_mouse_randomly(page)
                    # wait max 14 secondes 
                    click_next_page(page)

            except Exception as e:
                log_data["errors"].append({
                    "phase":"monthly_loop",
                    "month_index": i,
                    "error": str(e)
                })

        log_data["status"] = "success"

    except Exception as main_err:
        log_data["status"] = "failure"
        # Ajoute une erreur globale (utile si levée depuis un sous-bloc)
        if not any(e.get("phase") == "init_or_main_loop" for e in log_data["errors"]):
            log_data["errors"].append({
                "phase": "init_or_main_loop",
                "error": str(main_err)
            })

    finally:
        try:
            browser.close()
            playwright.stop()
        except:
            pass
        end_time = time.time()
        log_data["end_time"] = datetime.datetime.now().isoformat()
        log_data["duration_sec"] = round(end_time - start_time, 2)

           # Log the run
        if len(log_data["errors"]) > 0:
            errors_str = json.dumps(log_data["errors"], ensure_ascii=False)
        else:
            errors_str="[]"
        with open(log_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:  # Write header if new file
                writer.writerow([
                    "trip", "date_scrapped", "start_time", "end_time",
                    "duration_sec", "total_months",
                    "status", "errors_count","errors", "url"
                ])
            writer.writerow([
                log_data["trip"],
                log_data['date_scrapped'],
                log_data["start_time"],
                log_data["end_time"],
                log_data["duration_sec"],
                log_data["total_months"],
                log_data["status"],
                len(log_data["errors"]),
                errors_str,
                log_data["url"]
            ])
        print(f"✅ Log saved in {log_path}")

    return log_data
