# Data Dictionary: Golden Board Dataset

This document describes the schema for the final merged dataset located at `data/final/board_golden.parquet`.

| Field | Type | Description | Values / Range |
| :--- | :--- | :--- | :--- |
| **ticker** | string | The stock symbol of the company. | e.g., FPT, VNM, VCB |
| **exchange** | string | The stock exchange where the company is listed. | HOSE, HNX |
| **person_name** | string | The official full name of the leader (preferred source: Vietstock). | e.g., Trương Gia Bình |
| **role** | string | The normalized job title or position within the board. | e.g., Chủ tịch HĐQT |
| **normalized_name**| string | Name stripped of honorifics and standardized for joining. | e.g., truong gia binh |
| **education** | string | Educational background (Vietstock exclusive). | e.g., Tiến sĩ Toán Lý |
| **birth_year** | string | Year of birth (Vietstock exclusive). | e.g., 1956 |
| **source_agreement**| string | Indicates which sources provided this specific record. | `both`, `cafef_only`, `vietstock_only` |
| **confidence_score** | float | A quality metric based on source agreement and role match. | 0.0 to 1.0 |
| **scraped_at** | timestamp| ISO 8601 timestamp of when the data was last fetched. | |

### Caveats
* **Role Normalization:** Roles are standardized (e.g., "Chủ tịch Hội đồng quản trị" is mapped to "Chủ tịch HĐQT").
* **Confidence Scoring:** Records found in both sources with matching roles receive a 1.0. Single-source records default to 0.6.