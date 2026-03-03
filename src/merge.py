import pandas as pd
import yaml
import os
import unicodedata
from datetime import datetime
from utils import process_silver_record

class BoardMerger:
    def __init__(self, config_path: str = "config.yaml"):
        # Load configuration for file paths
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)
        self.paths = self.config["paths"]
        
        # Internal paths for the Silver Layer
        self.silver_cf_path = "data/processed/cafef_silver.parquet"
        self.silver_vs_path = "data/processed/vietstock_silver.parquet"

    def _make_match_key(self, name: str) -> str:
        """
        Requirement 3a: Creates an accent-free, lowercase slug.
        This handles 'Nguyễn' vs 'Nguyen' variations during the merge.
        """
        if not name or not isinstance(name, str): 
            return ""
        # Decompose Unicode characters and strip non-spacing marks (accents)
        name = "".join(c for c in unicodedata.normalize('NFD', name) 
                       if unicodedata.category(c) != 'Mn')
        # Standardize for join: lowercase and no spaces
        return name.lower().replace(" ", "").strip()

    def run(self):
        # --- PHASE 1: SILVER LAYER (Process, Aggregate, & Save) ---
        print("🚀 Starting Silver Layer processing...")
        df_cf_raw = pd.read_parquet(self.paths["cafef_output"])
        df_vs_raw = pd.read_parquet(self.paths["vietstock_output"])
        
        # 1. Atomic Standardization (Cleaning structural noise/honorifics)
        silver_cf_base = df_cf_raw.apply(process_silver_record, axis=1)
        silver_vs_base = df_vs_raw.apply(process_silver_record, axis=1)
        
        # Re-attach ticker for entity grouping
        silver_cf_base['ticker'] = df_cf_raw['ticker']
        silver_vs_base['ticker'] = df_vs_raw['ticker']

        # 2. Entity Aggregation (Handling "Multi-Hat" individuals)
        # Collapses multiple rows for one person into a single unique entity per ticker.
        agg_logic = {
            'role': lambda x: " / ".join(sorted(list(dict.fromkeys(x)))),
            'is_independent': 'max',
            'year_of_birth': 'first',
            'education': 'first',
            'tenure': 'first',
            'shares': 'first'
        }

        silver_cf = silver_cf_base.groupby(['ticker', 'person_name']).agg(agg_logic).reset_index()
        silver_vs = silver_vs_base.groupby(['ticker', 'person_name']).agg(agg_logic).reset_index()

        # 3. Save Processed Files (Persistence for Audit)
        os.makedirs("data/processed", exist_ok=True)
        silver_cf.to_parquet(self.silver_cf_path, index=False)
        silver_vs.to_parquet(self.silver_vs_path, index=False)
        print(f"💾 Silver files saved to {self.silver_cf_path} and {self.silver_vs_path}")

        # --- PHASE 2: GOLDEN LAYER (Load, Merge, & Resolve) ---
        print("🏆 Starting Golden Layer merge...")
        df_cf = pd.read_parquet(self.silver_cf_path)
        df_vs = pd.read_parquet(self.silver_vs_path)

        # 4. Handle Variations during Merge (Requirement 3a)
        df_cf['match_name'] = df_cf['person_name'].apply(self._make_match_key)
        df_vs['match_name'] = df_vs['person_name'].apply(self._make_match_key)

        # Full Outer Join: Flags and retains unmatched records separately (Req 3a)
        merged = pd.merge(
            df_cf, df_vs, 
            on=['ticker', 'match_name'], 
            how='outer', 
            suffixes=('_cf', '_vs')
        )

        # 5. Strict Conflict Resolution (Requirement 3b)
        golden_records = merged.apply(self.resolve_record, axis=1)
        
        # 6. Save Final Unified Record
        os.makedirs(os.path.dirname(self.paths["golden_output"]), exist_ok=True)
        golden_records.to_parquet(self.paths["golden_output"], index=False)
        
        print(f"✅ SUCCESS: Golden table created with {len(golden_records)} records.")
        return golden_records

    def resolve_record(self, row):
        """Strict resolution strategy based on source presence and attribute agreement."""
        has_cf = pd.notnull(row.get('role_cf'))
        has_vs = pd.notnull(row.get('role_vs'))
        
        # --- Requirement 3a: Name Selection (Best Available) ---
        # Select the name with accents (usually results in longer string length)
        name_cf = str(row.get('person_name_cf', ''))
        name_vs = str(row.get('person_name_vs', ''))
        best_name = name_vs if len(name_vs) >= len(name_cf) else name_cf

        # --- Requirement 3b: Attribute Resolution (Vietstock Priority) ---
        res = {
            'ticker': row['ticker'],
            'person_name': best_name,
            'role': row['role_vs'] if has_vs else row['role_cf'],
            'is_independent': row['is_independent_vs'] if has_vs else row['is_independent_cf'],
            
            # Enriched demographic fields (Vietstock Exclusive)
            'year_of_birth': row.get('year_of_birth_vs'),
            'education': row.get('education_vs'),
            'tenure': row.get('tenure_vs'),
            'shares': row.get('shares_vs'),
            
            'merged_at': datetime.now().isoformat()
        }

        # --- Conflict Detection (Common fields ONLY) ---
        if has_cf and has_vs:
            # Check semantic agreement on shared fields: Role and Independence
            role_match = row['role_cf'] == row['role_vs']
            indep_match = row['is_independent_cf'] == row['is_independent_vs']
            
            if role_match and indep_match:
                res['source_agreement'] = 'both'
                res['confidence_score'] = 1.0
            else:
                # Documented Conflict: Prioritize Vietstock but flag for audit
                res['source_agreement'] = 'conflict'
                res['confidence_score'] = 0.8
        else:
            # Unmatched records retained with lower confidence
            res['source_agreement'] = 'cafef_only' if has_cf else 'vietstock_only'
            res['confidence_score'] = 0.6

        return pd.Series(res)

if __name__ == "__main__":
    BoardMerger().run()