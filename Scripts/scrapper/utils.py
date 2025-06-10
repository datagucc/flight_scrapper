# Generic utility functions for Playwright automation.
# Includes browser launching, handling UI elements like cookies, buttons, and taking screenshots.

from playwright.sync_api import sync_playwright, Page
import time
import os
#pour simuler le comportement d'un utilisateur réel
from Config.constants import USER_AGENT
import random

def tri():
    print(random.choice(list(USER_AGENT.values())))
    print(random.choice(list(USER_AGENT.values())))
    print(random.choice(list(USER_AGENT.values())))
    print(random.choice(list(USER_AGENT.values())))
    print(random.choice(list(USER_AGENT.values())))
    print(random.choice(list(USER_AGENT.values())))

def launch_browser(headless: bool = True):
    """
    Launches a Chromium browser with a custom user agent and high DPI for better screenshot quality.

    Args:
        headless (bool): Whether to run the browser in headless mode. Default is True.

    Returns:
        tuple: (playwright instance, browser object, context, page)
    """
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=headless
        ,args=[
            "--disable-blink-features=AutomationControlled",  # enlève trace visible de l'automatisation
        ]
        )
    context = browser.new_context(
        user_agent=random.choice(list(USER_AGENT.values())) #randomly select an user agent from the predefined list
        ,viewport={"width": 1920, "height": 1080}
        ,device_scale_factor=2  # Improves screenshot quality on Retina-like displays
        ,locale="en-US"
        ,timezone_id="America/Argentina/Buenos_Aires"  # Set timezone for buenos aires
        ,geolocation={"latitude": -34.6037, "longitude": -58.3816} #geolocation for Buenos Aires
        ,permissions=["geolocation"]
        ,java_script_enabled=True
        ,extra_http_headers={"Accept-Language": "en-US,en;q=0.9"}

    )
    page = context.new_page()
    # Supprime les traces d'automatisation connues
    page = context.new_page()
    page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        window.navigator.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'plugins', {
            get: () => [1, 2, 3, 4, 5],
        });
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en'],
        });
    """)

    return playwright, browser, context, page


def handle_cookies_popup(page: Page, timeout: int = 3000):
    """
    Handles cookie consent pop-up if present, in a stealthy and multilingual way.

    Args:
        page (Page): The current Playwright page.
        timeout (int): Maximum wait time in milliseconds to detect cookie banner.
    """
    try:
        # Essayer différents textes selon les langues (EN, FR, ES, etc.)
        buttons_text = ["Accept all", "Tout accepter", "Aceptar todo", "Akzeptieren", "Alle akzeptieren"]
        for text in buttons_text:
            try:
                # Cherche un bouton avec le texte, timeout court pour rester furtif
                accept_button = page.wait_for_selector(f'button:has-text("{text}")', timeout=timeout)
                if accept_button:
                    accept_button.click()
                    time.sleep(0.5)  # pause brève pour laisser le temps au DOM de s'adapter
                    return
            except TimeoutError:
                continue  # On essaie le texte suivant
    except Exception as e:
        print("[⚠️ Cookie Handler] Aucun pop-up ou erreur : ", e)
        pass


def go_to_url(page: Page, url: str = "https://www.google.com/travel/flights", timeout: int = 10000):
    """
    Navigates to the given URL and waits briefly to ensure the page has fully loaded.

    Args:
        page (Page): Current page instance.
        url (str): Target URL to visit.
    """
    try:
        page.goto(url, timeout=timeout)
        time.sleep(random.uniform(1,3))  # bref délai pour simuler un comportement humain
    except Exception as e:
        print(f"[❌ Navigation Error] {e}")

def move_mouse_randomly(page: Page):
    width, height = random.randint(200, 800), random.randint(200, 600)
    page.mouse.move(width, height, steps=random.randint(10, 40))

def get_input_homepage(page: Page):
    """
    Logs all input elements and their aria-labels on the homepage.

    Args:
        page (Page): Current page instance.
    """
    inputs = page.locator("input").all()
    for i, el in enumerate(inputs):
        try:
            label = el.get_attribute("aria-label")
            print(f"[{i}] aria-label = {label}")
            # Filter visible inputs
            visible_inputs = inputs.filter(
                has_text="", has_not_text="", has=None
            ).filter(lambda el: el.is_visible())
            print(visible_inputs)
        except Exception:
            continue


def get_all_buttons(page: Page):
    """
    Collects and logs all buttons and clickable elements on the current page.

    Args:
        page (Page): Current page instance.
    """
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
            except Exception:
                pass

    for s in all_buttons:
        print(f"🔘 {s[1]} | Selector: {s[0]} | Text: {s[2]}")


def identify_element(page: Page, element):
    """
    Identifies and logs buttons inside a specific container div by class.

    Args:
        page (Page): Current page instance.
    """
    container = page.locator('div.d53ede.rQItBb.FfP4Bc.Gm3csc')
    buttons = container.locator("button")
    count = buttons.count()
    print(f"\n✅ {count} buttons found in the targeted container:\n")

    for i in range(count):
        btn = buttons.nth(i)
        aria_label = btn.get_attribute("aria-label")
        text = btn.text_content()
        print(f"🔘 Button {i+1}")
        print(f"    aria-label : {aria_label}")
        print(f"    text        : {text}\n")


def take_screenshot_pixels(page: Page, name_image: str, type: str = "full"):
    """
    Takes a screenshot of a specific pixel region on the page.

    IT WOULD BE NICE TO PUT CLIP INTO THE PARAMETERS
    Args:
        page (Page): Current page instance.
        name_image (str): Base name for the screenshot file.
        type (str): Type of screenshot (currently unused, reserved for future use).
    """
    page.wait_for_timeout(1000)

    # Ensure output directory exists
    output_dir = "screenshots"
    os.makedirs(output_dir, exist_ok=True)

    # Capture a specific area of the screen
    page.screenshot(
        path=os.path.join(output_dir, f"{name_image}.png"),
        clip={
            "x": 500,
            "y": 500,
            "width": 600,
            "height": 400
        }
    )
