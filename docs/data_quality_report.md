# Data Quality Report - Board of Directors Merger

## 1. Executive Summary
- **Total Unique Entities**: 530
- **Overall Match Rate**: 77.36%
- **Weighted Confidence Score**: 0.87

## 2. Source Distribution
| Category | Count | Description |
| :--- | :---: | :--- |
| Both (Agreed) | 296 | Full consensus on name and role. |
| Both (Conflict) | 114 | Matched on identity; role resolved via Vietstock preference. |
| CafeF Only | 97 | Primarily Executive Managers not listed on board pages. |
| Vietstock Only | 23 | Board members missing from CafeF's API. |

## 3. Conflict Resolution Strategy
- **Identity Matching**: Implemented a Normalization Slug (lowercase, accent-free, space-free) to bridge the gap between "Nguyễn" and "Nguyen" (Requirement 3a).
- **Attribute Priority**: Preferred **Vietstock** for all shared attributes (Role, Independence) due to its specialized focus on board-level governance and higher update frequency (Requirement 3b).
- **Multi-Hat Aggregation**: Resolved individuals holding multiple roles by concatenating titles (e.g., "Chủ tịch / Tổng Giám đốc") and sorting them alphabetically to prevent false "conflicts" caused by string order.

## 4. Observed Patterns
- **Naming Integrity**: 100% of honorifics (Ông, Bà, TS, etc.) were successfully stripped. Display names were selected using a "Longest String" heuristic to preserve accented characters.
- **Enrichment Rate:**: Vietstock successfully enriched 81.13% of the total dataset with demographic data (Year of Birth, Education, Tenure) that was entirely absent in the CafeF API.
- **Governance Discrepancy**: A high conflict rate of 21.5% was observed stems from structural differences in how CafeF and Vietstock name executive roles. The pipeline successfully standardized these via Vietstock priority to ensure a unified governance metric."