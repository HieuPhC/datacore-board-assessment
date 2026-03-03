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

### 1. `person_name`
* **Normalization:** Names are normalized to **Unicode NFC** format to ensure consistent character representation.
* **Cleaning:** Aggressive Regex stripping is applied to remove prefixes (Ông, Bà, TS, etc.) including those with non-breaking spaces (`\xa0`).
* **Join Logic:** A hidden `join_key` (slugified, accent-free, space-free) is used to bridge spelling variations (e.g., "Nguyễn" vs "Nguyen") across sources.

### 2. `role`
* **Standardization:** Raw strings are mapped to a "Golden Standard" using a dictionary. Multi-part roles (e.g., "KTT kiêm Phó TGĐ") are split, mapped, deduplicated, and sorted alphabetically to ensure `A / B` matches `B / A`.
* **Resolution:** In cases of conflict, **Vietstock** is used as the tie-breaker for the final display role due to its higher granularity for board-level data.

### 3. `is_independent`
* **Logic:** Extracted from the `role` field in CafeF and the `tenure` field in Vietstock. If *either* source flags a member as independent, the Golden record returns `True`.

### 4. `source_agreement`
* `both`: Record exists in both sources and standardized roles are identical.
* `conflict`: Record exists in both sources but roles differ (resolved via Vietstock preference).
* `cafef_only` / `vietstock_only`: Record exists in only one source.

### 5. `confidence_score`
* **1.0 (High):** Data is cross-verified and identical across both sources.
* **0.8 (Medium-High):** Sources match on identity, but a conflict in roles was resolved using the priority strategy.
* **0.6 (Medium):** Data exists in only one source; veracity cannot be cross-verified.