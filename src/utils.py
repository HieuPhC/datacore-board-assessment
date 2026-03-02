import re
import unicodedata

def normalize_name(name: str) -> str:
    """
    Standardizes names for cross-source matching.
    Requirement 3a: Handles diacritics, honorifics, and spacing.
    """
    if not name or not isinstance(name, str):
        return ""
    
    # 1. Unicode Normalization (NFC)
    # Essential for Vietnamese. Ensures 'Nguyễn' (one encoding) 
    # matches 'Nguyễn' (another encoding).
    name = unicodedata.normalize('NFC', name)
    
    # 2. Remove common honorifics and titles (case-insensitive)
    # We look for the prefix at the start of the string followed by a space
    prefixes = (
        r'^(Ông|Bà|TS|ThS|GS|PGS|Tiến sĩ|Thạc sĩ|Cử nhân|'
        r'Chuyên gia|Dược sĩ|Luật sư|Tiến sỹ|Thạc sỹ)\.?\s+'
    )
    name = re.sub(prefixes, '', name, flags=re.IGNORECASE)
    
    # 3. Clean up spacing and standardize casing
    # '  TRUONG GIA BINH  ' -> 'Truong Gia Binh'
    name = " ".join(name.split()).strip().title()
    
    return name

def normalize_role(role: str) -> str:
    """Standardizes common role titles to improve conflict resolution."""
    if not role:
        return "Thành viên"
    role = role.strip()
    # Example: 'Chủ tịch Hội đồng quản trị' -> 'Chủ tịch HĐQT'
    if re.search(r'Chủ tịch.*HĐQT|Chủ tịch.*Hội đồng', role, re.I):
        return "Chủ tịch HĐQT"
    return role