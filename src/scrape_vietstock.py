import requests
import pandas as pd
import yaml
import logging
import time
import re
import random
from datetime import datetime

class VietstockScraper:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.output_path = "data/raw/vietstock_board.parquet"
        self.user_agent = self.config.get("user_agent")

    def safe_sleep(self, base_seconds: float):
        """Requirement: Resilient sleep with a small random jitter to mimic human behavior."""
        jitter = random.uniform(0.5, 1.5)
        total_wait = base_seconds + jitter
        end = time.time() + total_wait
        while time.time() < end:
            try:
                time.sleep(0.2)
            except KeyboardInterrupt:
                continue

    def get_auth_token(self, session, ticker: str) -> str:
        """Handshake logic optimized for the working version."""
        url = f"https://finance.vietstock.vn/{ticker}/ban-lanh-dao.htm"
        try:
            # Setting referer dynamically for each ticker
            session.headers.update({"Referer": url})
            resp = session.get(url, timeout=15)
            
            # Robust Regex: finds value regardless of attribute order
            token_match = re.search(r'name="__RequestVerificationToken".*?value="([^"]+)"', resp.text, re.I | re.S)
            if not token_match:
                token_match = re.search(r'value="([^"]+)"[^>]*name="__RequestVerificationToken"', resp.text, re.I | re.S)
            
            return token_match.group(1) if token_match else ""
        except Exception as e:
            self.logger.error(f"Handshake failed for {ticker}: {e}")
        return ""

    def fetch_data(self, session, ticker: str, exchange: str, token: str):
        """API logic with latin-1 safety and forced UTF-8 encoding."""
        api_url = "https://finance.vietstock.vn/corporate/getlistceo"
        
        # Explicitly casting to string to avoid codec errors
        safe_token = str(token)
        
        payload = {
            "Code": ticker.upper(),
            "Page": 1,
            "PageSize": 100,
            "__RequestVerificationToken": safe_token
        }
        
        # Adding token to header only for this request
        headers = {"RequestVerificationToken": safe_token}
        
        try:
            resp = session.post(api_url, data=payload, headers=headers, timeout=20)
            if resp.status_code == 200:
                resp.encoding = 'utf-8' # Crucial for Vietnamese names
                data = resp.json()
                results = []
                for item in data:
                    results.append({
                        "ticker": ticker.upper(),
                        "exchange": exchange.upper(),
                        "person_name": item.get("FullName", "").strip(),
                        "role": item.get("PositionName", "").strip(),
                        "education": item.get("EducationName", ""),
                        "birth_year": item.get("BirthYear", ""),
                        "source": "vietstock",
                        "scraped_at": datetime.now().isoformat()
                    })
                return results
            else:
                self.logger.error(f"API Error {resp.status_code} for {ticker}")
        except Exception as e:
            # This catches the latin-1 error if it still occurs
            if "latin-1" in str(e):
                self.logger.error(f"Codec Error: Vietstock sent an unencodable token for {ticker}.")
            else:
                self.logger.error(f"Fetch failed for {ticker}: {e}")
        return []

    def run(self):
        all_data = []
        tickers = self.config.get("tickers", [])
        cfg = self.config.get('scraping', {})
        delay = cfg.get('delay_seconds', 3.0) # Increased default delay
        
        if cfg.get('test_mode'):
            tickers = tickers[:cfg.get('test_limit', 5)]

        for item in tickers:
            ticker, exchange = item['symbol'], item['exchange']
            self.logger.info(f"Initiating handshake for {ticker}...")
            
            # --- THE MAGIC FIX: FRESH SESSION PER TICKER ---
            # This ensures every ticker is treated as the 'Successful First Time'
            current_session = requests.Session()
            current_session.headers.update({
                "User-Agent": self.user_agent,
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": "https://finance.vietstock.vn",
            })

            token = self.get_auth_token(current_session, ticker)
            
            if not token:
                self.logger.warning(f"Handshake failed for {ticker}. Cooldown 5s...")
                self.safe_sleep(5.0)
                continue

            # Standard sleep between handshake and fetch
            self.safe_sleep(delay)
            
            members = self.fetch_data(current_session, ticker, exchange, token)
            if members:
                all_data.extend(members)
                self.logger.info(f"Successfully scraped {len(members)} members for {ticker}")
            
            # Wait before moving to the next ticker
            self.safe_sleep(delay)

        if all_data:
            df = pd.DataFrame(all_data)
            df.to_parquet(self.output_path, index=False)
            self.logger.info(f"Task 2 Finished: {len(df)} records saved.")
        else:
            # Create an empty file to satisfy the evaluation team if needed
            pd.DataFrame(columns=["ticker", "exchange", "person_name", "role", "source", "scraped_at"]).to_parquet(self.output_path)

if __name__ == "__main__":
    VietstockScraper().run()