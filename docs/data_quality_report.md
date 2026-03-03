# Data Quality Report - Board of Directors Merger

## 1. Executive Summary
- **Total Unique Entities**: 530
- **Overall Match Rate**: 77.36%
- **Weighted Confidence Score**: 0.87

## 2. Source Distribution
| Category | Count | Description |
| :--- | :---: | :--- |
| Both (Agreed) | 307 | Full consensus on name and role. |
| Both (Conflict) | 103 | Matched on identity; role resolved via Vietstock preference. |
| CafeF Only | 97 | Primarily Executive Managers not listed on board pages. |
| Vietstock Only | 23 | Board members missing from CafeF's API. |

## 3. Conflict Resolution Strategy
- **Identity Matching**: Used a "Slugified Join Key" (lowercase, no accents, no spaces) to bypass diacritic and honorific variations.
- **Role Preference**: Preferred **Vietstock** for role titles as it provides higher granularity for Board of Director classifications (e.g., distinguishing between Supervisory Board and Executive Management).
- **Independence**: Applied a logical OR (max); if either source flagged a member as independent, the Golden record reflects this.

## 4. Observed Patterns
- **Honorifics**: 100% of honorifics (Ông, Bà, etc.) were successfully stripped from display names.
- **Enrichment**: Vietstock successfully enriched 81.7% of the total dataset with Year of Birth and Education data.