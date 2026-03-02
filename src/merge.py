import pandas as pd
import yaml
import os
import logging
from utils import normalize_name # This confirms utils.py is working

class BoardMerger:
    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Paths from config and repo structure
        self.raw_cafef = self.config["paths"]["cafef_output"]
        self.proc_cafef = "data/processed/cafef_processed.parquet"
        self.final_output = "data/final/board_golden.parquet"

    def process_individual_source(self, input_path: str, output_path: str, source_name: str):
        """Logic for Task 3a: Cleaning and saving individual source data."""
        if not os.path.exists(input_path):
            self.logger.warning(f"Raw file not found: {input_path}. Skipping {source_name}.")
            return None
        
        self.logger.info(f"Processing {source_name} data...")
        df = pd.read_parquet(input_path)
        
        # Apply normalization to create the join key
        df['normalized_name'] = df['person_name'].apply(normalize_name)
        
        # Save to processed/
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_parquet(output_path, index=False)
        self.logger.info(f"Saved {source_name} processed data to {output_path}")
        return df

    def run(self):
        """Evaluation-ready sequence."""
        # 1. Process CafeF
        df_cafef = self.process_individual_source(
            self.raw_cafef, self.proc_cafef, "CafeF"
        )
        
        # 2. Process Vietstock (Placeholder for now)
        raw_vietstock = "data/raw/vietstock_board.parquet"
        proc_vietstock = "data/processed/vietstock_processed.parquet"
        df_vs = self.process_individual_source(
            raw_vietstock, proc_vietstock, "Vietstock"
        )
        
        if df_cafef is not None and df_vs is not None:
            self.logger.info("Both sources found. Starting Golden Merge...")
            # We will implement merge_logic here once Task 2 is complete
        else:
            self.logger.info("Single source mode: Merge waiting for Vietstock data.")

if __name__ == "__main__":
    BoardMerger().run()