# script to fill Google Flights form using Playwright
import time
import datetime
from dateutil.relativedelta import relativedelta
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..','..')))
from playwright.sync_api import Page

from Scripts.scrapper.testing.utils_old import launch_browser, handle_cookies_popup, go_to_url
from Config.constants import SELECTORS, SCREENSHOT_PATH





    


def click_departure_date(page: Page):
    try:
        # Clique d'abord dans la section départ
        page.mouse.click(100, 100)

        # Petite pause pour laisser la page réagir
        page.wait_for_timeout(1000)

        # ATTEND que le champ "Departure" soit présent
        input_departure_label = SELECTORS['input_departure_label']
        page.wait_for_selector(input_departure_label, timeout=10000)
        departure_input = page.locator(input_departure_label).first
        departure_input.click()
        page.wait_for_timeout(1000)


    except Exception as e:
        page.screenshot(path="debug_departure_input_fail.png", full_page=True)
        raise e

def click_next_page(page: Page):
    try:


        # Petite pause pour laisser la page réagir
        page.wait_for_timeout(1000)
        next_button_label= '[aria-label="Next"]'

        # ATTEND que le champ "Departure" soit présent
        page.wait_for_selector(next_button_label, timeout=10000)

        next_input = page.locator(next_button_label).first

        next_input.click()
        page.wait_for_timeout(1000)


    except Exception as e:
        page.screenshot(path="debug_next_input_fail.png", full_page=True)
        raise e



def take_screenshot_ele(page: Page,current_date,trip, name_image, nb=0):
    calendar_container = 'div[jsname="Mgvhmd"]'
    element = page.locator(calendar_container).nth(nb)
    path = SCREENSHOT_PATH
    # 📸 Screenshot uniquement de ce div

    final_path = f"{path}/{current_date}/{trip}/{name_image}.png"
    print(final_path)
    element.screenshot(path=final_path)
    print(f"📸 Screenshot enregistré dans {final_path}")




def scrapping_url(url, trip, headless=True, waiting_time=3):
    """ 
    Cette fonction permet de scrapper les mois de un voyage donné, à partir d'une URL de Google Flights.
    headless = True permet de naviguer de manière furtive. SI on met = False, alors chromium s'ouvre et c'est moins discret.

    """

    # Etape 0 : definir les variables
    date_obj = datetime.datetime.now()
    current_date_full = datetime.datetime.now().strftime("%Y-%m-%d")

    # Etape 1: lancer le navigateur
    playwright, browser, context, page = launch_browser(headless=headless)
    go_to_url(page, url)
    handle_cookies_popup(page)
    

    # Etape 2 : cliquer sur le selecteur aria-label = Departure
    click_departure_date(page)
    
    # Etape 3 : lancer la boucle pour prendre un screenshot de chaque mois
    count = 0 
    for i in range(2):
        # On attend un peu pour que la page charge
        time.sleep(waiting_time)
        
        # Prendre un screenshot de l'élément
        month_year = date_obj + relativedelta(months=i)
        month_year = month_year.strftime("%m_%Y")
        #print('count : ', i)
        #print('month_year : ', month_year)
        #print('current_date_full : ', current_date_full)

        take_screenshot_ele(page,current_date_full, trip, f"{current_date_full}_XxX_{trip}_XxX_{month_year}", count)

        # On clique sur le bouton "Next" pour aller au mois suivant
        if i < 10:
            click_next_page(page)
        
        count += 1

    browser.close()
    playwright.stop()

