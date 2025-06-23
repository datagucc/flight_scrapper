# centralisation des sélecteurs CSS, URL de base, etc.

# scripts/constants.py

# === SELECTORS ===
SELECTORS = {

    'input_departure_label':'input[aria-label="Departure"]'
    ,'next_button_label': '[aria-label="Next"]'
    ,"calendar_container": 'div[jsname="Mgvhmd"]'

}

Image_SIZE= {
    'height_big' : '640'
    ,'hieght_small': '536'
     ,'weight': '672'
    }

PATH = {
    'screenshot_path':'/Users/focus_profond/GIT_repo/flight_price_tracker/Data/raw/screenshots'
    ,'log_path':'/Users/focus_profond/GIT_repo/flight_price_tracker/Logs'

}

USER_AGENT =  {
    # Liste de vrais User-Agents pour faire tourner aléatoirement
    'user1': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ,'user2': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
    ,'user3': "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"


        # Chrome – Windows
    ,'user4':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ,'user5':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    
    # Chrome – macOS
    ,'user6':"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_2_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ,'user7':"Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    # Chrome – Linux
    ,'user8':"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.78 Safari/537.36"
    ,'user9':"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"

    # Firefox – Windows
    ,'user9':"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0"
    ,'user10':"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"

    # Firefox – macOS
    ,'user11':"Mozilla/5.0 (Macintosh; Intel Mac OS X 13.4; rv:125.0) Gecko/20100101 Firefox/125.0"
    ,'user12':"Mozilla/5.0 (Macintosh; Intel Mac OS X 12.6; rv:124.0) Gecko/20100101 Firefox/124.0"

    # Edge – Windows
    ,'user13':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.2478.80"
    ,'user14':"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.2420.65"

    # Safari – macOS
    ,'user15':"Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.4 Safari/605.1.15"
    ,'user16':"Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_8) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Safari/605.1.15"


}