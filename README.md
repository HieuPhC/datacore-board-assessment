# DataCore Board of Directors Assessment

This repository contains a robust data engineering pipeline designed to scrape, process, and merge leadership data from major Vietnamese financial portals (**CafeF** and **Vietstock**).

## Setup and Run

### Prerequisites
- Python 3.10+
- Ubuntu 22.04 (Recommended)

### Installation
```bash
git clone https://github.com/HieuPhC/datacore-board-assessment
cd datacore-board-assessment
pip install -r requirements.txt
python src/scrape_cafef.py     
python src/scrape_vietstock.py   
python src/merge.py    
```

### Verify Output

Check the `data/output/` directory for the final `golden_board_members.parquet` file. You can inspect it using:

```bash
import pandas as pd
df = pd.read_parquet("data/output/golden_board_members.parquet")
print(df.head())
```

## Approach

## Data Sources & Ticker Selection

To demonstrate the pipeline's robustness, the system scrapes data for 30 tickers as required by the assessment.

* Exchange Coverage: 15 HOSE tickers and 15 HNX tickers were selected to ensure a balanced cross-exchange validation.

* Selection Criteria: Tickers were curated from the VN30 and HNX30 indices. These represent the largest and most liquid stocks in the Vietnamese market, offering the most complete corporate filings for testing.

* Methodology: A static list (defined in config.yaml) was used to provide a consistent baseline for performance benchmarking and error handling across both CafeF and Vietstock sources.

### Task 1: CafeF Scraper

1. Strategy: API vs. HTML

    * Direct API Consumption: Instead of parsing the frontend DOM, the scraper targets CafeF’s internal Ajax endpoint (ListCeo.ashx).

    * Efficiency: Utilizing JSON payloads reduces bandwidth and increases execution speed compared to heavy HTML scraping.

    * Data Integrity: JSON provides structured key-value pairs (Name, Position), bypassing parsing errors common in irregular HTML table structures.

2. Operational Resilience

    * Safe-Sleep Mechanism: A custom safe_sleep function absorbs environmental "Ghost Interrupts" during delays, preventing the scraper from skipping tickers.

    * Content-Aware Retries: The logic validates the JSON body. If a response returns empty data (signaling temporary throttling), it triggers a recovery loop with exponential back off rather than proceeding with a partial dataset.

    * Anti-Bot Evasion: Combines configurable base delays (default 3 seconds) with randomized jitter (0.5s–1.5s) and exponential back-off (base 2) for 403 Forbidden errors.

### Task 2: Vietstock Scraper (Advanced Session Management)

1. Implementation: Stateful HTML Parsing

    * Rowspan Logic: Implemented a stateful parser using `BeautifulSoup` and `lxml` to handle complex HTML `rowspan` attributes. This ensures data from merged cells is correctly propagated across all related records.

    * Content Validation: The scraper explicitly validates the presence of `<table>` and `<tbody>` tags within the DOM. If the structure is missing due to a rendering glitch, it triggers a recovery retry.

2. Data Enrichment & Sanitization

    * Attribute Extraction: Focused on capturing supplemental fields missing from CafeF, including Year of Birth, Education, Tenure, and Shareholding.

    * Real-time Cleaning: Automatically sanitizes shareholding strings into numeric formats and filters out redacted entries marked with `***`.

3. Operational Stability

    * Network Resilience: Employs a robust fetch_with_retry architecture that handles transient network timeouts and environmental interrupts during active I/O.

    * Dual-Delay Strategy: Uses Exponential Back-off for infrastructure errors (403/Timeouts) and Fixed-Interval Retries for content-level validation failures.

    * Session Management: Utilizes requests.Session to maintain persistent cookies and connection states across multiple ticker requests.

### Task 3: Normalization & Merging

1. Silver Phase: Structural Cleaning & Persistence

    Raw records are standardized and aggregated into an intermediate layer before the final merge.

    * Structural Normalization:

        * Spacing & Hyphens: Replaces non-breaking spaces and hyphens (`-`, `–`, `—`) with standard spaces to unify names like "Trương-Công-Thắng" and "Trương Công Thắng".

        * Honorific Stripping: Removes prefixes (Ông, Bà, TS, etc.) using regex word boundaries to prevent accidental name corruption.

        * Unicode NFC: Normalizes all text to NFC format to ensure consistent character byte-codes for Vietnamese diacritics.

    * Symmetric Schema: Every record is forced into a standard 7-column schema. If a source lacks a field (e.g., CafeF lacking `education`), it is explicitly filled with `np.nan` to ensure perfect symmetry.

    * Entity Aggregation: Performs a `groupby` on `(ticker, person_name)` to collapse "multi-hat" individuals. Multiple roles are joined with a `" / "` separator and sorted to ensure deterministic comparisons.

    * Persistence: Standardized data is saved to `data/processed/` as Parquet files for auditability.

2. Gold Phase: Variation Handling & Conflict Resolution

    The system reads the Silver files and performs a "Union Join" to create the final record.

    * Match-Key Join (Requirement 3a): During the merge, a temporary, accent-free, and space-free "match key" is generated to bridge variations like "Nguyễn" vs. "Nguyen". The final join is executed as a Full Outer Join on `(ticker, match_key)`.

    * Best-Available Value Selection:

        * Name: Heuristically selects the most complete version (typically the one with accents).

        * Attributes: Prioritizes Vietstock for all attributes (Role, Independence, and Enriched Fields) due to better data availability.

    * Strict Conflict Resolution (Requirement 3b):

        * Discrepancies in Role or Independence Status between sources trigger a `conflict` flag.

        * Confidence Scoring:

            * `1.0` (both): Perfect semantic agreement across shared fields.

            * `0.8` (conflict): Identity matched, but source attributes disagreed.

            * `0.6` (single_source): Record present in only one source; cross-validation not possible.

## Known Challenges

* Silent Data Mismatches: Because the pipeline defaults to Vietstock Priority, if CafeF has more recent data (e.g., a very recent resignation), it will be overwritten by outdated Vietstock records without a freshness check.

* Positional Fragility (Vietstock): The scraper relies on fixed column indices. If Vietstock reorders their table (e.g., swapping "Tenure" and "Shares"), data will be mapped to incorrect fields silently.

* Heuristic Independence: Detection is strictly keyword-based ("độc lập"). Terminology variations or typos in the source data will lead to false negatives for board independence status.

* API Response Ghosting (CafeF): The internal CafeF endpoint occasionally returns empty Data arrays even for valid tickers. The scraper retries up to 3 times, but some tickers may still fail to return data during peak server load.

* Internal Path Volatility: Both scrapers rely on undocumented internal endpoints/classes. A "silent break" (e.g., CafeF changing ListCeo.ashx to a new path) will render the source inaccessible until the code is manually updated.

* Name Collisions: In rare cases where a person has the same name and ticker but different identities, the pipeline will erroneously merge them into a single record.