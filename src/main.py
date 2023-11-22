""" 
Author: Daniel Saleh

"""

import requests
import json
import time
import pandas as pd
import logging



class HouseListingsScraper:
    """  
    Using the requests lib, this scraper accesses the underlying and publicly accessible, API of
    the real estate listings website "Boligsiden.dk", to retrieve miscellaneous types of house listings data

    The wanted type of listings are parameterized inline as a string, in to the API request it self, 
    with "per_page"(int param. formatted to string), being the maximum amount of observations per page.   
    Each type is requested sequentially from the server - this lessens/avoids the risk of overloading the target 
    server which could potentially be illegal.

    """

    BASE_URL = 'https://api.boligsiden.dk/search/cases?'
    PAGE_NUM = "page="
    PER_PAGE = "per_page=1000"
    LISTING_TYPE = [
        'villa', 'condo', 'terraced+house', 'holiday+house', 'cooperative',
        'farm', 'hobby+farm', 'full+year+plot', 'villa+apartment', 'holiday+plot'
    ]
    ADDRESS_TYPE = "addressTypes="

    def __init__(self, file_path=None):
        self.file_path = file_path
        self.logger = logging.getLogger(__name__)

    def fetch_json_data(self, url):
        try:
            response = requests.get(url)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error occurred: {e}")

    def fetch_house_listings(self, page_num):
        url = f"{self.BASE_URL}{self.PAGE_NUM}{page_num}&{self.PER_PAGE}"
        return self.fetch_json_data(url)
        
    def parse_json_data(self, json_data):
        return pd.DataFrame(json_data['cases'])


    def page_loop(self):
        for i in range(1, 10):
            json_data = self.fetch_house_listings(i)
            if json_data:
                self.logger.info(f"JSON added: {type(json_data)}")
                
                df = self.parse_json_data(json_data)
                PATH = 'src/data/raw_data_%s.csv' % i
                df.to_csv(PATH, index=False)

    def main(self):
        self.page_loop()


if __name__ == '__main__':

    LOG_FILE = r"src/logs/scraper_test.log"
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO,
                        format=log_format, filename=LOG_FILE)

    house_scraper = HouseListingsScraper()
    house_scraper.main()



"""
SINGLE PAGE REQ EXAMPLE

r = requests.get("https://api.boligsiden.dk/search/cases?")
json_data = json.loads(r.text)
json_data.pop('_links')
json_data = json_data['totalHits']

print(json_data)

# open a file for writing
with open('json_data.json', 'w') as f:
    # use the json.dump() function to write the dictionary to the file
    json.dump(json_data, f)
"""
