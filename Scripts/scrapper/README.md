Plan pour le scrapping :

1. On part d'une adresse URL
2. On ouvre l'adresse URL, on clique sur "Departure"
3. On attend
4. On screen le premier mois (avec le div). nth(0)
5. On click sur le bouton suivant
6. On screen le second mois
7. On fait ça pour les 12 mois.

Il faut stocker les screenshots selon un certain nom :
date_scrapping_XxX_VILLEORI_VILLEDESTI_MOIS_ANNEE

Constant :
bouton date = input[aria-label="Departure"]
         page.wait_for_selector('input[aria-label="Departure"]', timeout=10000)
         departure_input = page.locator('input[aria-label="Departure"]').first

bouton mois suivant = '[aria-label="Next"]'
         page.wait_for_selector('[aria-label="Next"]', timeout=10000)
         next_input = page.locator('[aria-label="Next"]').first

dictionnaire d'URL : {EZE_BRU : url} ou tuple (EZE_BRU, url)
dictionnaire avec : {EZE_BRU : url, ville ori, ville desti, aeropo_ori, aeropo_desti}
On doit créer un document qui comprend une liste de tuples : (EZE_BRU, url) 

element calendrier =  div[jsname="Mgvhmd"]
            element = page.locator('div[jsname="Mgvhmd"]').nth(2)






Pour charger toutes les photos du 26 mai 2025, dans un dataframe, il a fallu 11 minutes via un notebook. et ca a generé 8000lignes +-.