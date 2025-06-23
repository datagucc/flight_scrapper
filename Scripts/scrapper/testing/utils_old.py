#  fonctions génériques (ex: fonction pour prendre screenshot, gérer logs, etc).

from playwright.sync_api import sync_playwright, Page
import time


USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

def launch_browser(headless: bool = True):
    """
    Lance le navigateur Chromium avec contexte personnalisé (user-agent).
    """
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(
        user_agent=USER_AGENT,
        viewport={"width": 1920, "height": 1080},
        device_scale_factor=2 #pour doubler la densité des pixels et donc augmenter la qualité des captures d'écran
    )
    page = context.new_page()
    return playwright, browser, context, page

def handle_cookies_popup(page: Page):
    """
    Gère la popup cookies si présente.
    """
    try:
        # Exemple : bouton "Accepter tout" (attention au texte selon langue)
        accept_button = page.query_selector('button:has-text("Accept all")')
        if accept_button:
            accept_button.click()
            time.sleep(1)
    except Exception as e:
        print("Pas de popup cookies détectée ou erreur:", e)

def go_to_url(page: Page, url: str = "https://www.google.com/travel/flights"):
    """
    Charge la page URL et attend que l’élément principal soit visible.
    """
    page.goto(url)
    time.sleep(2)  # Petite pause pour être sûr que tout soit bien chargé


def get_input_homepage(page : Page):

    inputs = page.locator("input").all()
    for i, el in enumerate(inputs):
        try:
            label = el.get_attribute("aria-label")
            print(f"[{i}] aria-label = {label}")
            visible_inputs = inputs.filter(has_text="", has_not_text="", has=None).filter(lambda el: el.is_visible())
            print(visible_inputs)
        except:
            continue
   # inputs = page.locator('input[aria-label="Where from?"]')
    #visible_inputs = inputs.filter(has_text="", has_not_text="", has=None).filter(lambda el: el.is_visible())
    #print(visible_inputs)
    #print(inputs)


def get_all_buttons(page: Page):
  
        # Sélecteurs multiples combinés
        selectors = [
            "button",
            "input[type='submit']",
            "input[type='button']",
            "[role='button']",
            "a[class*=button]",
            "a[class*=btn]",
            "[class*=btn]",
        ]

        all_buttons = []
        for selector in selectors:
            elements = page.query_selector_all(selector)
            for el in elements:
                try:
                    text = el.text_content().strip()
                    tag = el.evaluate("el => el.tagName")
                    all_buttons.append((selector, tag, text))
                except:
                    pass

        for s in all_buttons:
            print(f"🔘 {s[1]} | Sélecteur: {s[0]} | Texte: {s[2]}")


def identify_element(page: Page):


        # Cibler le div par sa classe
        container = page.locator('div.d53ede.rQItBb.FfP4Bc.Gm3csc')

        # Chercher tous les boutons à l’intérieur de ce div
        buttons = container.locator("button")

        count = buttons.count()
        print(f"\n✅ {count} boutons trouvés dans le div ciblé:\n")

        for i in range(count):
            btn = buttons.nth(i)
            aria_label = btn.get_attribute("aria-label")
            text = btn.text_content()
            print(f"🔘 Bouton {i+1}")
            print(f"    aria-label : {aria_label}")
            print(f"    text        : {text}\n")



#SCREENSHOTER TOUTE LA PAGE
def take_screenshot_pixels(page:Page,name_image, type="full"):
    page.wait_for_timeout(1000)
    page.screenshot(
            path="screenshots/test_1.png",
            clip={
                "x": 500,           # position horizontale depuis la gauche
                "y": 500,           # position verticale depuis le haut
                "width": 600,       # largeur du rectangle
                "height": 400       # hauteur du rectangle
            }
        )
    
