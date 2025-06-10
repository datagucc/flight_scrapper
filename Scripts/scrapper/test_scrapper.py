import pandas as pd
import scrapper as scrapper
from playwright.sync_api import Page
from Scripts.scrapper.utils import launch_browser, handle_cookies_popup, go_to_url,tri

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
tri()
"""
playwright, browser, context, page = launch_browser()
go_to_url(page, 'https://www.google.com/travel/flights')
handle_cookies_popup(page)

inputs = page.query_selector_all("input")
for i, el in enumerate(inputs):
    label = el.get_attribute("aria-label")
    print(f"[{i}] aria-label = {label}")


elements = page.query_selector_all('input[aria-label="Where else?"]')
if not elements:
    print("Aucun champ de saisie trouvé pour 'Where to?'")
    exit(1)
for i, el in enumerate(elements):
    print(f"[{i}] visible={el.is_visible()} | value={el.get_attribute('value')}")
for i, el in enumerate(elements):
    print(f"[{i}] visible={el.is_visible()} | value={el.input_value()}")

for i, el in enumerate(elements):
    #if el.is_visible():
        print(f"[{i}] Input visible, contenu: {el.input_value()}")
        el.click()  # ou el.fill("BRU")
        el.fill("BRU")
        page.wait_for_timeout(1000)
        page.screenshot(path="screenshots/page_complete.png", full_page=True)
        print(f"✅ Nouvelle valeur (via input_value) : {el.input_value()}")

        #break

        """