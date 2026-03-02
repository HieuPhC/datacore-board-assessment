import requests
import pandas as pd
import time
import yaml
import logging
from datetime import datetime

class CafeFScraper:
    def __init__(self, config_path: str = "config.yaml"):
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
        
        # Pull paths from config
        self.output_path = self.config["paths"]["cafef_output"]
    
    def safe_sleep(self, seconds: float):
        """
        Custom sleep that absorbs interrupts to prevent 
        skipping tickers during the wait period.
        """
        end = time.time() + seconds
        while time.time() < end:
            try:
                # Small intervals make the script responsive but resilient
                time.sleep(0.2)
            except KeyboardInterrupt:
                self.logger.warning("Ghost Interrupt absorbed during safe_sleep.")
                continue

    def _get_api_url(self, ticker: str) -> str:
        """Endpoint for the 'Ban lãnh đạo' data."""
        return f"https://cafef.vn/du-lieu/Ajax/PageNew/ListCeo.ashx?Symbol={ticker.lower()}&PositionGroup=0"

    def run(self):
        all_data = []
        tickers = self.config.get("tickers", [])
        
        # Scraping settings from config
        scraping_cfg = self.config.get('scraping', {})
        delay = scraping_cfg.get('delay_seconds', 1.0)
        
        if scraping_cfg.get('test_mode'):
            tickers = tickers[:scraping_cfg.get('test_limit', 5)]

        for item in tickers:
            ticker, exchange = item['symbol'], item['exchange']
            url = self._get_api_url(ticker)
            self.logger.info(f"Fetching {ticker} via API...")
            
            try:
                # Use the delay defined in config.yaml
                self.safe_sleep(delay) 
                
                res = self.session.get(url, timeout=20)
                res.raise_for_status()
                json_data = res.json()
                
                # Navigate the nested JSON structure: Data -> values
                raw_groups = json_data.get('Data', [])
                if not raw_groups:
                    self.logger.warning(f"No Data groups found for {ticker}")
                    continue

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
                
            except Exception as e:
                self.logger.error(f"Failed to scrape {ticker}: {e}")

        if all_data:
            df = pd.DataFrame(all_data)
            # Save to the path specified in config.yaml
            df.to_parquet(self.output_path, index=False)
            self.logger.info(f"SUCCESS: Saved {len(df)} records to {self.output_path}")
        else:
            self.logger.error("No data collected. Verify API connectivity or config tickers.")

if __name__ == "__main__":
    CafeFScraper().run()