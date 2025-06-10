# Script to automate Google Flights calendar screenshot capturing using Playwright.
# This module simulates user interactions to collect visual data from the calendar interface.

import time
import datetime
from dateutil.relativedelta import relativedelta
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from playwright.sync_api import Page
from Scripts.scrapper.utils import launch_browser, handle_cookies_popup, go_to_url
from Config.constants import SELECTORS, SCREENSHOT_PATH


def click_departure_date(page: Page):
    """
    Opens the departure date calendar by clicking on the corresponding UI element.

    Args:
        page (Page): The Playwright page object currently open.

    Raises:
        Exception: If the departure input cannot be found or clicked.
    """
    try:
        # Ensure the page is interactive by clicking a neutral area
        page.mouse.click(100, 100)
        page.wait_for_timeout(1000)

        # Wait for the "Departure" label and click it
        input_departure_label = SELECTORS['input_departure_label']
        page.wait_for_selector(input_departure_label, timeout=10000)
        departure_input = page.locator(input_departure_label).first
        departure_input.click()

        page.wait_for_timeout(1000)
    except Exception as e:
        # Capture screenshot on failure to aid debugging
        page.screenshot(path="debug_departure_input_fail.png", full_page=True)
        raise e


def click_next_page(page: Page):
    """
    Clicks the "Next" button on the calendar to switch to the next month.

    Args:
        page (Page): The Playwright page object.

    Raises:
        Exception: If the next button is not found or the click fails.
    """
    try:
        page.wait_for_timeout(1000)
        next_button_label = '[aria-label="Next"]'

        page.wait_for_selector(next_button_label, timeout=10000)
        next_input = page.locator(next_button_label).first
        next_input.click()

        page.wait_for_timeout(1000)
    except Exception as e:
        # Capture screenshot on failure to aid debugging
        page.screenshot(path="debug_next_input_fail.png", full_page=True)
        raise e


def take_screenshot_ele(page: Page, current_date: str, trip: str, name_image: str, nb: int = 0):
    """
    Takes a screenshot of a specific calendar section (month).

    Args:
        page (Page): The Playwright page object.
        current_date (str): Date when the script is executed (used for folder naming).
        trip (str): Trip name used to categorize screenshots.
        name_image (str): Base name for the screenshot file.
        nb (int): Index of the calendar container to target (default is 0).
    """
    calendar_container = 'div[jsname="Mgvhmd"]'
    element = page.locator(calendar_container).nth(nb)
    final_path = f"{SCREENSHOT_PATH}/{current_date}/{trip}/{name_image}.png"

    # Ensure target directory exists
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    element.screenshot(path=final_path)
    print(f"📸 Screenshot saved to {final_path}")



def scrapping_url(url: str, trip: str,month_to_capture : int = 12, headless: bool = True, waiting_time: int = 3):
    """
    Automates browsing to a Google Flights calendar and captures screenshots for multiple months.

    Args:
        url (str): Full URL to the Google Flights search page.
        trip (str): Identifier for the trip used in naming screenshots.
        headless (bool): Whether to run the browser in headless mode (default: True).
        waiting_time (int): Seconds to wait after rendering before taking screenshots (default: 3).

    Notes:
        This function captures screenshots for two consecutive months from the calendar.
    """
    # Initialize date variables
    date_obj = datetime.datetime.now()
    current_date_full = date_obj.strftime("%Y-%m-%d")
    start_time = time.time()

    # Launch browser session
    playwright, browser, context, page = launch_browser(headless=headless)
    go_to_url(page, url)
    handle_cookies_popup(page)

    # Open the departure date calendar
    click_departure_date(page)

    # Capture screenshots of the first two visible months
    for i in range(month_to_capture):
        time.sleep(waiting_time)
        month_year = (date_obj + relativedelta(months=i)).strftime("%m_%Y")
        image_name = f"{current_date_full}_XxX_{trip}_XxX_{month_year}"
        
        take_screenshot_ele(page, current_date_full, trip, image_name, nb=i)

        if i < 10:  # Still valid if reused with >2 iterations
            click_next_page(page)

    # Clean up browser
    browser.close()
    playwright.stop()
    end_time = time.time()

