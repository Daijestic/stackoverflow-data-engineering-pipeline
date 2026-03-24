import os
import requests
from dotenv import load_dotenv

load_dotenv()

class StackExchangeClient:
    
    BASE_URL = "https://api.stackexchange.com/2.3"

    def __init__(self, site="stackoverflow"):
        self.site = site
        self.api_key = os.getenv("STACKEXCHANGE_KEY")

    def call_api(self, endpoint, params=None):

        url = f"{self.BASE_URL}/{endpoint}"

        if params is None:
            params = {}
        
        params["site"] = self.site

        if self.api_key:
            params["key"] = self.api_key
        
        response = requests.get(url, params=params)

        response.raise_for_status()

        return response.json()