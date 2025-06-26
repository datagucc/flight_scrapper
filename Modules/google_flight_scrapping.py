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
import logging
from Config.constants import PATH
root_dir = PATH['main_path']
log_path = f"{PATH['logs_path']}/Scrapping/"
screenshot_debug_path = f'{log_path}/Screenshots_debug'
data_path = PATH['data_path']
screenshot_path = f'{data_path}/raw/screenshots'

if root_dir not in sys.path:
    sys.path.append(root_dir)

#sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

#from playwright.sync_api import Page, PlaywrightTimeoutError
from playwright.sync_api import Page, TimeoutError
from Modules.scrapping_utils import launch_browser, handle_cookies_popup, go_to_url,move_mouse_randomly
from Config.constants import SELECTORS, PATH 



def click_departure_date(page: Page):
    """
    Opens the departure date calendar by clicking on the corresponding UI element.

    Args:
        page (Page): The Playwright page object currently open.

    Raises:
        Exception: If the departure input cannot be found or clicked.
    
    Maximum running time : 29,5 secondes

    """
    try:
        # Short delay to avoid bot-like behavior (random wait): up to 1 sec
        page.wait_for_timeout(500 + int(450 * random.random()))

        # Ensure the page is interactive by clicking a neutral area: up to 0.5 sec
        page.mouse.click(int(200 * random.random()), int(200 * random.random()))

        # Another short delay to avoid bot-like behavior (random wait): up to 1.05 sec
        page.wait_for_timeout(500 + int(550 * random.random()))

        # Wait for the "Departure" label and attempt to click it
        input_departure_label = SELECTORS['input_departure_label']

        # Try to wait for the selector; if it doesn't appear, continue anyway
        try:
            # Wait up to 10 seconds
            page.wait_for_selector(input_departure_label, timeout=10000)
        except:
            print("Timeout exceeded")

        # This locator often fails, so we attempt to find a backup one
        locator = page.locator(input_departure_label)
        try:
            # Try the first matching element
            departure_input = locator.nth(0)
            # Wait up to 10 seconds
            departure_input.wait_for(state="visible", timeout=10000)
        except TimeoutError:
            try:
                # If the first fails, try the second
                departure_input = locator.nth(1)
                # Wait up to 5 seconds
                departure_input.wait_for(state="visible", timeout=5000)
            except Exception as e:
                print(f"No visible departure input found for selector: {input_departure_label}")
                raise e

        # Scroll to the element if needed (prevents interaction errors if off-screen)
        departure_input.scroll_into_view_if_needed()
        # Wait up to 0.5 seconds
        time.sleep(0.2 + random.random() * 0.3)

        # Simulated click with small delay
        # Hover to simulate human behavior (mouse-over)
        departure_input.hover()
        # Wait up to 0.5 seconds
        time.sleep(0.2 + random.random() * 0.3)
        departure_input.click()

        # Final random wait to avoid bot-like patterns: up to 1.1 seconds
        page.wait_for_timeout(500 + int(600 * random.random()))

    except Exception as e:
        # Capture screenshot on failure to aid debugging
        timestampstr = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        page.screenshot(path=f"{screenshot_debug_path}/debug_click_departure_{timestampstr}.png", full_page=True)
        print("[❌ Error] Failed to click on the departure date field.")
        raise e



def click_next_page(page: Page):
    """
    Clicks the "Next" button on the calendar to switch to the next month.

    Args:
        page (Page): The Playwright page object.

    Raises:
        Exception: If the next button is not found or the click fails.
    
    Maximum running time : 14 secondes

    """
    
    try:
        # Wait up to 1.4 seconds to simulate human behavior
        page.wait_for_timeout(600 + random.randint(200, 800))

        next_button_label = SELECTORS['next_button_label']

        # Wait up to 10 seconds for the "Next" button to appear
        page.wait_for_selector(next_button_label, timeout=10000)
        next_input = page.locator(next_button_label).first

        # Realistic interaction: scroll + hover + click with randomized delays
        next_input.scroll_into_view_if_needed()
        # Wait up to 0.5 seconds
        page.wait_for_timeout(200 + random.randint(100, 300))
        next_input.hover()
        # Wait up to 0.3 seconds
        page.wait_for_timeout(100 + random.randint(50, 200))

        next_input.click()

        # Pause after click to allow UI to reload
        # Wait up to 1.8 seconds
        page.wait_for_timeout(1000 + random.randint(400, 800))

    except Exception as e:
        # Capture screenshot on failure to aid debugging
        timestampstr = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        page.screenshot(path=f"{screenshot_debug_path}/debug_next_page_{timestampstr}.png", full_page=True)
        print("[❌ Error] Failed to click the 'Next' button.")
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

        Maximum running time : 11 secondes
    """
    try:
        calendar_container = SELECTORS['calendar_container']
        element = page.locator(calendar_container).nth(nb)
        final_path = f"{screenshot_path}/{current_date}/{trip}/{name_image}.png"

        # Wait until the element is visible (max 10 seconds)
        element.wait_for(state="visible", timeout=10000)

        # Human-like pause before screenshot (up to 1 second)
        wait_ms = random.randint(*delay_range_ms)
        page.wait_for_timeout(wait_ms)

        # Ensure the target directory exists
        os.makedirs(os.path.dirname(final_path), exist_ok=True)

        # Take the screenshot
        element.screenshot(path=final_path)
        print(f"📸 Screenshot saved to {final_path}")

    except Exception as e:
        timestampstr = datetime.datetime.now().strftime('%Y-%m-%d %H_%M_%S')
        page.screenshot(path=f"{screenshot_debug_path}/debug_screenshot_{timestampstr}.png", full_page=True)
        print(f"[❌ ERROR] Failed to capture screenshot for {trip} → {name_image}: {e}")
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

    Maximum running time : 400 secondes --> so max 7 minutes per trip 

    """
    date_obj = datetime.datetime.now()
    current_date_full = date_obj.strftime("%Y-%m-%d")
    start_time = time.time()
    log_path_a = f"{PATH['logs_path']}/Scrapping/"
    log_path = f"{log_path_a}/Execution_logs/scraping_log_{current_date_full}.csv"
    

    log_data = {
        "trip": trip,
        "date_scrapped": current_date_full,
        "start_time": datetime.datetime.now().isoformat(),
        "end_time": None,
        "duration_sec": None,
        "total_months": month_to_capture,
        "errors": [],
        "status": "started",
        "url": url
    }

    try:
        # PHASE 1: BROWSER INITIALIZATION
        
        try:
            #print('hello 1')
            playwright, browser, context, page = launch_browser(headless=headless)
        except Exception as e:
            print(str(e))
            log_data["errors"].append({"phase": "launch_browser", "error": str(e)})
            raise
        
    

        # PHASE 2: NAVIGATION TO TARGET URL
        try:
            #print('hello 2')
            go_to_url(page, url)
            
        except Exception as e:
            log_data["errors"].append({"phase": "go_to_url", "error": str(e)})
            raise

        # PHASE 3: DATE PICKER – MAX WAIT: 30 seconds
        is_second_try = False
        try:
            #print('hello 3')
            click_departure_date(page)
        except Exception as e:
            log_data["errors"].append({"phase": "click_departure_date", "error": str(e)})
            is_second_try = True
            raise
        

        # This step often fails. Reloading the browser or page might help.
        # We retry the entire sequence: new browser → new page → click again
        if is_second_try:
            try:
                browser.close()
                playwright.stop()
                playwright, browser, context, page = launch_browser(headless=headless)
                go_to_url(page, url)
                click_departure_date(page)
                log_data["errors"].append({"phase":"second_click_departure WORKING", "error": "none"})
            except Exception as e:
                log_data["errors"].append({"phase": "second_click_departure_date", "error": str(e)})
                raise

        # PHASE 4: MONTH LOOP — max 32 seconds per loop, ~360s total for 12 months
        for i in range(month_to_capture):
            try:
                #print('hello 4')
                # Realistic delay before actions
                wait_seconds = random.uniform(*waiting_time)
                page.wait_for_timeout(wait_seconds * 1000)  # max 5 seconds

                # Format the image name
                month_year = (date_obj + relativedelta(months=i)).strftime("%m_%Y")
                image_name = f"{current_date_full}_XxX_{trip}_XxX_{month_year}"

                # Take a screenshot of the calendar element
                take_screenshot_ele(
                    page, current_date_full, trip, image_name,
                    nb=i, delay_range_ms=(400, 900)
                )

                if i < 10:
                    # Human pause before going to next calendar month
                    page.wait_for_timeout(random.randint(800, 2100))  # max 2 seconds
                    click_next_page(page)  # max 14 seconds
            except Exception as e:
                log_data["errors"].append({
                    "phase": "monthly_loop",
                    "month_index": i,
                    "error": str(e)
                })

        log_data["status"] = "success"

    except Exception as main_err:
        log_data["status"] = "failure"
        # Add a global error if not already present
        if not any(e.get("phase") == "init_or_main_loop" for e in log_data["errors"]):
            log_data["errors"].append({
                "phase": "init_or_main_loop",
                "error": str(main_err)
            })

    finally:
        try:
            #print('hello 5')
            browser.close()
            playwright.stop()
        except:
            pass

        end_time = time.time()
        log_data["end_time"] = datetime.datetime.now().isoformat()
        log_data["duration_sec"] = round(end_time - start_time, 2)

        # Save log to CSV
        errors_str = json.dumps(log_data["errors"], ensure_ascii=False) if log_data["errors"] else "[]"
        with open(log_path, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if f.tell() == 0:  # Write header if file is new
                writer.writerow([
                    "trip", "date_scrapped", "start_time", "end_time",
                    "duration_sec", "total_months",
                    "status", "errors_count", "errors", "url"
                ])
            writer.writerow([
                log_data["trip"],
                log_data["date_scrapped"],
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

