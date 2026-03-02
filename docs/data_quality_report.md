# Data Quality Report

## 1. Overview
* **Total Golden Records:** [Total Count]
* **CafeF Match Count:** 591
* **Vietstock Match Count:** [Count from Task 2]
* **Cross-Source Match Rate:** [Percentage]%

## 2. Source Agreement Analysis
| Agreement Type | Count | Percentage |
| :--- | :--- | :--- |
| **Both Sources** | [Count] | [XX]% |
| **CafeF Only** | [Count] | [XX]% |
| **Vietstock Only** | [Count] | [XX]% |

## 3. Technical Challenges & Observations

### Vietstock Anti-Bot Measures
During Task 2, the scraper encountered aggressive IP-based rate limiting from `finance.vietstock.vn`. 
* **Codec Errors:** Observed `latin-1` encoding failures. This was diagnosed as the server sending non-ASCII characters in the `__RequestVerificationToken` to crash standard automated HTTP clients.
* **Mitigation:** Implemented per-ticker session rotation and byte-level header handling. A cooling-down period was required to reset the IP reputation.

### Conflict Resolution Strategy
* **Precedence:** When sources disagreed on a role title, **Vietstock** was prioritized due to its higher granularity and alignment with official corporate filings.
* **Deduplication:** Join keys were constructed using `ticker` + `normalized_name` (lowercase, no honorifics) to ensure "Ông Nguyễn Văn A" (CafeF) matched "Nguyễn Văn A" (Vietstock).

## 4. Confidence Score Distribution
* **1.0 (High):** Matches found in both sources with identical roles.
* **0.8 (Medium-High):** Matches found in both sources but with slight role title variations.
* **0.6 (Medium):** Records found in only one source.