import requests
import pandas as pd
import yaml
import logging
import os
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup

class VietstockScraper:
    def __init__(self, config_path: str = "config.yaml"):
        # Load configuration
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger("VietstockScraper")
        
        # Load settings from config
        self.output_path = self.config["paths"]["vietstock_output"]
        self.user_agent = self.config.get("user_agent")
        self.scraping_cfg = self.config.get("scraping", {})

    def safe_sleep(self, seconds: float):
        """Resilient sleep that absorbs interrupts to prevent skipping tickers."""
        end = time.time() + seconds
        while time.time() < end:
            try:
                time.sleep(min(0.2, end - time.time()))
            except KeyboardInterrupt:
                self.logger.warning("Ghost Interrupt absorbed during safe_sleep.")
                continue

    def fetch_with_retry(self, session, url):
        """
        Implementation of 'Safe Strategy' for Vietstock.
        Now specifically handles 'Ghost Interrupts' during network I/O.
        """
        max_retries = self.scraping_cfg.get('retries', 3)
        base_backoff = 2
        headers = {"User-Agent": self.user_agent}
        
        for i in range(max_retries):
            try:
                # This is where the 'Ghost Interrupt' typically occurs
                resp = session.get(
                    url, 
                    headers=headers, 
                    timeout=self.scraping_cfg.get("timeout", 20)
                )
                
                if resp.status_code == 403:
                    wait_time = base_backoff ** i
                    self.logger.warning(f"403 Forbidden. Backing off for {wait_time}s (Attempt {i+1}/{max_retries})")
                    self.safe_sleep(wait_time)
                    continue

                resp.raise_for_status()
                return resp
                
            except KeyboardInterrupt:
                # Capture the environment-level signal and force a recovery
                self.logger.warning(f"Ghost Interrupt detected during network call for {url}. Attempting recovery...")
                self.safe_sleep(1.0) 
                continue
            except (requests.exceptions.RequestException, Exception) as e:
                wait_time = base_backoff ** i
                self.logger.error(f"Request failed: {e}. Retrying in {wait_time}s...")
                if i < max_retries - 1:
                    self.safe_sleep(wait_time)
        
        return None

    def scrape_ticker(self, session, ticker, exchange):
        """
        Scrapes the most recent board table for a given ticker.
        Implements Content-Aware Retries: If the table is missing from the 
        HTML response, it triggers a retry.
        """
        url = f"https://finance.vietstock.vn/{ticker}/ban-lanh-dao.htm"
        max_retries = self.scraping_cfg.get('retries', 3)
        delay = self.scraping_cfg.get('delay_seconds', 3.0)
        
        for attempt in range(max_retries):
            # 1. Network Fetch with Ghost Interrupt protection
            resp = self.fetch_with_retry(session, url)
            
            if resp is None:
                continue

            try:
                # 2. Parse HTML
                soup = BeautifulSoup(resp.content, 'lxml')
                table = soup.find('table', class_='table')
                
                # VALIDATION: If the table tag is missing, the page likely glitched
                if not table:
                    self.logger.warning(f"Attempt {attempt+1}: No table tag found for {ticker}. Retrying...")
                    self.safe_sleep(delay)
                    continue

                rows = table.find('tbody').find_all('tr')
                
                # VALIDATION: If tbody exists but has no rows
                if not rows:
                    self.logger.warning(f"Attempt {attempt+1}: Table found but no rows for {ticker}. Retrying...")
                    self.safe_sleep(delay)
                    continue

                members = []
                for row in rows:
                    cells = [td.get_text(" ", strip=True) for td in row.find_all('td', recursive=False)]
                    
                    if not cells or len(cells) < 6:
                        continue

                    # Handle Rowspan logic
                    if len(cells) >= 7 and "/" in cells[0]: 
                        p_name, p_role, p_yob, p_edu, p_shares, p_tenure = cells[1], cells[2], cells[3], cells[4], cells[5], cells[6]
                    else:
                        p_name, p_role, p_yob, p_edu, p_shares, p_tenure = cells[0], cells[1], cells[2], cells[3], cells[4], cells[5]

                    if "***" in p_name or not p_name:
                        continue

                    # Clean shares: remove commas and handle blanks
                    p_shares_clean = p_shares.replace(',', '').strip() if p_shares else None

                    members.append({
                        "ticker": ticker.upper(),
                        "exchange": exchange.upper(),
                        "person_name": p_name,
                        "role": p_role,
                        "year_of_birth": p_yob,
                        "education": p_edu,
                        "shares": p_shares_clean,
                        "tenure": p_tenure,
                        "source": "vietstock",
                        "scraped_at": datetime.now().isoformat()
                    })
                
                # If we collected members, the attempt is successful
                if members:
                    return members
                else:
                    self.logger.warning(f"Attempt {attempt+1}: No valid members parsed for {ticker}. Retrying...")
                    self.safe_sleep(delay)

            except Exception as e:
                self.logger.error(f"Attempt {attempt+1}: Parsing error for {ticker}: {e}")
                self.safe_sleep(delay)

        self.logger.error(f"CRITICAL: Failed to retrieve board data for {ticker} after {max_retries} attempts.")
        return []

    def run(self):
        all_data = []
        session = requests.Session()
        
        tickers = self.config.get("tickers", [])
        
        if self.scraping_cfg.get('test_mode'):
            limit = self.scraping_cfg.get('test_limit', 5)
            self.logger.info(f"Test Mode: Limiting to {limit} tickers.")
            tickers = tickers[:limit]

        for item in tickers:
            symbol = item['symbol']
            ex_name = item['exchange']
            self.logger.info(f"Processing {symbol} ({ex_name})...")
            
            # 1. Base delay + jitter
            delay = self.scraping_cfg.get('delay_seconds', 3.0)
            jitter = random.uniform(0.5, 1.5)
            self.safe_sleep(delay + jitter)
            
            # 2. Scrape with retry integration
            data = self.scrape_ticker(session, symbol, ex_name)
            
            if data:
                all_data.extend(data)
                self.logger.info(f"Retrieved {len(data)} members for {symbol}.")
            else:
                self.logger.warning(f"No data retrieved for {symbol}.")

        # 3. Save to Parquet
        if all_data:
            df = pd.DataFrame(all_data)
            os.makedirs(os.path.dirname(self.output_path), exist_ok=True)
            df.to_parquet(self.output_path, index=False)
            self.logger.info(f"Scrape Complete. Total records saved: {len(df)}")
        else:
            self.logger.error("Pipeline failed to collect any data.")

if __name__ == "__main__":
    VietstockScraper().run()