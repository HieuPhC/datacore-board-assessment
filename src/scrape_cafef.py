import requests
import pandas as pd
import time
import yaml
import logging
from datetime import datetime
import random
import os

class CafeFScraper:
    def __init__(self, config_path: str = "config.yaml"):
        # Load configuration from central file
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Initialize session with config-based headers
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.config.get("user_agent"),
            "Referer": "https://cafef.vn/"
        })
        
        # Pull paths and config from yaml
        self.output_path = self.config["paths"]["cafef_output"]
        self.scraping_cfg = self.config.get('scraping', {})
    
    def safe_sleep(self, seconds: float):
        """Resilient sleep that absorbs interrupts to prevent skipping tickers."""
        end = time.time() + seconds
        while time.time() < end:
            try:
                # Sleep in small chunks to remain responsive to signals
                time.sleep(min(0.2, end - time.time()))
            except KeyboardInterrupt:
                self.logger.warning("Ghost Interrupt absorbed during safe_sleep.")
                continue

    def fetch_with_retry(self, url: str):
        """
        Implementation of 'Safe Strategy' that also catches 
        Environment-level KeyboardInterrupts during the network call.
        """
        max_retries = self.scraping_cfg.get('retries', 3)
        base_backoff = 2
        
        for i in range(max_retries):
            try:
                # The interrupt happened here in your log
                res = self.session.get(url, timeout=self.scraping_cfg.get('timeout', 20))
                
                if res.status_code == 403:
                    wait_time = base_backoff ** i
                    self.logger.warning(f"403 Blocked. Backoff {wait_time}s (Attempt {i+1}/{max_retries})")
                    self.safe_sleep(wait_time)
                    continue
                
                res.raise_for_status()
                return res
                
            except KeyboardInterrupt:
                # This catches the 'Ghost Interrupt' you experienced
                self.logger.warning(f"Ghost Interrupt detected during network call for {url}. Recovering...")
                self.safe_sleep(1.0) # Brief pause before retrying
                continue
            except (requests.exceptions.RequestException, Exception) as e:
                wait_time = base_backoff ** i
                self.logger.error(f"Request failed: {e}. Retrying in {wait_time}s...")
                if i < max_retries - 1:
                    self.safe_sleep(wait_time)
        
        return None

    def _get_api_url(self, ticker: str) -> str:
        """Endpoint for the 'Ban lãnh đạo' data."""
        return f"https://cafef.vn/du-lieu/Ajax/PageNew/ListCeo.ashx?Symbol={ticker.lower()}&PositionGroup=0"

    def run(self):
        all_data = []
        tickers = self.config.get("tickers", [])
        
        # Scraping settings
        delay = self.scraping_cfg.get('delay_seconds', 3.0)
        
        if self.scraping_cfg.get('test_mode'):
            tickers = tickers[:self.scraping_cfg.get('test_limit', 5)]

        for item in tickers:
            ticker, exchange = item['symbol'], item['exchange']
            url = self._get_api_url(ticker)
            self.logger.info(f"Fetching {ticker} ({exchange}) via API...")
            
            # Implementation of Content-Aware Retry
            max_retries = self.scraping_cfg.get('retries', 3)
            data_found = False
            
            for attempt in range(max_retries):
                # 1. Base delay + jitter
                self.safe_sleep(delay + random.uniform(0.5, 1.5))
                
                # 2. Network Fetch
                res = self.fetch_with_retry(url)
                if not res: continue

                try:
                    json_data = res.json()
                    raw_groups = json_data.get('Data', [])
                    
                    # VALIDATION: If Data is empty, treat as a failure and retry
                    if not raw_groups:
                        self.logger.warning(f"Attempt {attempt+1}: Empty Data for {ticker}. Retrying...")
                        continue
                    
                    # If we reach here, we have data!
                    ticker_count = 0
                    for group in raw_groups:
                        people_list = group.get('values', [])
                        for person in people_list:
                            name = person.get('Name', '').strip()
                            role = person.get('Position', '').strip()
                            if name:
                                all_data.append({
                                    "ticker": ticker.upper(),
                                    "exchange": exchange.upper(),
                                    "person_name": name,
                                    "role": role,
                                    "source": "cafef",
                                    "scraped_at": datetime.now().isoformat()
                                })
                                ticker_count += 1
                    
                    self.logger.info(f"Successfully extracted {ticker_count} members for {ticker}")
                    data_found = True
                    break # Success - exit retry loop
                    
                except Exception as e:
                    self.logger.error(f"Attempt {attempt+1}: JSON Error for {ticker}: {e}")
            
            if not data_found:
                self.logger.error(f"CRITICAL: No data could be retrieved for {ticker} after {max_retries} attempts.")

        # 3. Persistence to Parquet
        if all_data:
            df = pd.DataFrame(all_data)
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            df.to_parquet(self.output_path, index=False)
            self.logger.info(f"SUCCESS: Saved {len(df)} records to {self.output_path}")
        else:
            self.logger.error("Pipeline finished but no data was collected.")

if __name__ == "__main__":
    CafeFScraper().run()