import re
import unicodedata
import pandas as pd
import numpy as np

# Requirement 3b: Mapping for role title standardization
ROLE_MAP = {
    "CTHĐQT": "Chủ tịch HĐQT", "CHỦ TỊCH HĐQT": "Chủ tịch HĐQT",
    "PHÓ CTHĐQT": "Phó Chủ tịch HĐQT", "PHÓ CHỦ TỊCH HĐQT": "Phó Chủ tịch HĐQT",
    "TVHĐQT": "Thành viên HĐQT", "THÀNH VIÊN HĐQT": "Thành viên HĐQT",
    "TGĐ": "Tổng Giám đốc", "TỔNG GIÁM ĐỐC": "Tổng Giám đốc",
    "PHÓ TGĐ": "Phó Tổng Giám đốc", "PHÓ TỔNG GĐ": "Phó Tổng Giám đốc",
    "KTT": "Kế toán trưởng", "KẾ TOÁN TRƯỞNG": "Kế toán trưởng",
    "GĐ": "Giám đốc", "GIÁM ĐỐC": "Giám đốc",
    "GĐ ĐH": "Giám đốc Điều hành", "GĐ ĐIỀU HÀNH": "Giám đốc Điều hành",
    "BKS": "Ban Kiểm soát", "BAN KIỂM SOÁT": "Ban Kiểm soát",
    "UBKTNB": "Ủy ban Kiểm toán", "TRƯỞNG BKS": "Trưởng Ban Kiểm soát",
    "PHÓ BKS": "Phó Ban Kiểm soát", "THƯ KÝ CÔNG TY": "Thư ký Công ty",
    "THƯ KÝ HĐQT": "Thư ký Công ty", "NGƯỜI CBTT": "Người CBTT",
    "CÔNG BỐ THÔNG TIN": "Người CBTT", "NGƯỜI ĐƯỢC ỦY QUYỀN CÔNG BỐ THÔNG TIN": "Người CBTT",
    "PHỤ TRÁCH QUẢN TRỊ": "Quản trị Công ty", "QUẢN LÝ": "Quản lý"
}

def normalize_name(name: str) -> str:
    """Requirement 3a: Structural cleaning (Spacing, Hyphens, Honorifics)."""
    if not name or not isinstance(name, str): 
        return ""
    
    # 1. Structural Spacing: Replace non-breaking spaces and tabs
    name = name.replace('\xa0', ' ').replace('\t', ' ')
    
    # 2. Hyphen-to-Space: Standardize "Văn-A" to "Văn A"
    name = re.sub(r'[-–—]', ' ', name)
    
    # 3. Unicode Normalization: Ensure NFC (precomposed characters)
    # This prevents 'ế' (1 char) from being 'e + accent' (2 chars)
    name = unicodedata.normalize('NFC', name)
    
    # 4. Honorific Stripping
    prefixes = r'^(Ông|Bà|TS|T\.S|ThS|Th\.S|GS|G\.S|PGS|P\.G\.S|CN|Kỹ sư|Kỹ Sư|Trợ lý)\b\.?\s*'
    name = re.sub(prefixes, '', name, flags=re.IGNORECASE).strip()
    
    # 5. Collapse multiple spaces created by previous steps
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

def standardize_role(role_raw: str) -> str:
    """Requirement 3b: Normalize, deduplicate, and sort multi-part roles."""
    if not role_raw: return "Khác"
    parts = [p.strip() for p in re.split(r'[/,;]| kiêm | và ', role_raw, flags=re.IGNORECASE)]
    mapped = [ROLE_MAP.get(p.upper(), p.title()) for p in parts]
    unique_sorted = sorted(list(dict.fromkeys(mapped)))
    return " / ".join(unique_sorted)

def standardize_nan(value):
    """Unifies various null placeholders into true NaNs."""
    if value is None or pd.isna(value): return np.nan
    val_str = str(value).strip().lower()
    placeholders = {'-', 'n/a', 'na', 'n.a', '', 'nan', 'none'}
    return np.nan if val_str in placeholders else value

def process_silver_record(row):
    """
    Silver Transformation: Produces a clean record using ORIGINAL column names.
    This provides the symmetric schema required for the Gold Merge.
    """
    role_raw = str(row.get('role', ''))
    tenure_raw = str(row.get('tenure', ''))
    
    # Independence Logic
    is_independent = "độc lập" in role_raw.lower() or "độc lập" in tenure_raw.lower()
    clean_role_str = re.sub(r'\s+độc lập', '', role_raw, flags=re.IGNORECASE).strip()
    
    # Share Logic: Handle commas and convert to float
    shares = row.get('shares')
    if pd.notnull(shares):
        try: 
            shares = float(str(shares).replace(',', ''))
        except: 
            shares = np.nan

    # Return EVERY field using original names for symmetry
    return pd.Series({
        'person_name': normalize_name(row.get('person_name', '')),
        'role': standardize_role(clean_role_str),
        'is_independent': is_independent,
        'year_of_birth': standardize_nan(row.get('year_of_birth')),
        'education': standardize_nan(row.get('education')),
        'tenure': standardize_nan(row.get('tenure')),
        'shares': shares
    })