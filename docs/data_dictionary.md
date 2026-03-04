# Data Dictionary: Board Golden Dataset

This document describes the schema and data logic for the `board_golden.parquet` file, which represents the final "Gold" layer of the board of directors dataset.

| Field | Data Type | Description | Values / Range |
| :--- | :--- | :--- | :--- |
| `ticker` | String | Unique stock ticker symbol for the company. | E.g., `FPT`, `VNM`, `HPG` |
| `person_name` | String | Standardized full name of the board member or executive. | Title Case; Honorifics (Ông, Bà, etc.) removed. |
| `role` | String | Standardized and sorted job title(s) mapped from raw source strings. | E.g., `Chủ tịch HĐQT`, `Tổng Giám đốc` |
| `is_independent` | Boolean | Flag indicating if the member is an independent board member. | `True`, `False` |
| `year_of_birth` | Float | Year the member was born (Source: Vietstock). | E.g., `1960.0` |
| `education` | String | Academic background or professional certifications. | String description or `None` |
| `shares` | Float | Total number of shares held by the individual. | Numeric value or `NaN` |
| `source_agreement` | String | Indicates the level of consensus between CafeF and Vietstock. | `both`, `cafef_only`, `vietstock_only`, `conflict` |
| `confidence_score` | Float | Quantitative reliability metric based on source verification. | `0.6`, `0.8`, `1.0` |
| `merged_at` | String | ISO-8601 timestamp indicating when the record was processed. | E.g., `2026-03-03T15:42:50Z` |

---

## 🛠 Field Logic & Caveats

### 1. `person_name` & Identity Matching
* **Normalization:** All strings are converted to **Unicode NFC**.
* **Requirement 3a Handling:** To bridge variations (e.g., "Nguyễn" vs "Nguyen"), the system generates a temporary **Accent-free Slug** (lowercase, no spaces, no diacritics) for the merge operation.
* **Selection:** If both sources exist, the script selects the **longest string** as the display name to ensure accented Vietnamese characters are preserved over flat Latin text.

### 2. `role` (Multi-Hat Aggregation)
* **Aggregation:** During the Silver phase, individuals holding multiple seats in one company have their roles combined. 
* **Standardization:** Roles are deduplicated and sorted alphabetically. This ensures that a "Chairman / CEO" listing in CafeF matches a "CEO / Chairman" listing in Vietstock.
* **Resolution (3b):** If sources disagree on the role, the **Vietstock** version is prioritized for the final output.

### 3. `is_independent`
* **Conflict Logic:** This field is subject to the `conflict` flag. If CafeF lists a member as independent but Vietstock does not, the record is marked as `conflict` and **Vietstock's status takes priority**.

### 4. `source_agreement` & `confidence_score`
The pipeline assigns these based on the **Full Outer Join** results:

| Agreement | Criteria | Score |
| :--- | :--- | :--- |
| **both** | Identity matched AND shared attributes (**Role**, **Independence**) match perfectly. | **1.0** |
| **conflict** | Identity matched via slug, but **Role** or **Independence** differ. | **0.8** |
| **vietstock_only**| Record only found in Vietstock. | **0.6** |
| **cafef_only** | Record only found in CafeF. | **0.6** |



### 5. Enriched Fields (Vietstock Priority)
Fields like `year_of_birth`, `education`, `tenure`, and `shares` are mapped directly from the Vietstock source. If a record is `cafef_only`, these fields will naturally be `NaN`.