"""
📋 CODE VALIDATOR - STRICT CODE FILTER
Ưu tiên code thật, chặn chữ quảng cáo, link, hashtag, username, domain và text chat.
Hỗ trợ gom nhiều kênh vào một nhóm lọc để dễ kiểm soát.
"""

import math
import re
from config import Config


class CodeValidator:
    SITE_ROUTING_RULES = {
        "new88": ["NEW88", "N88", "NEW"],
        "mm88": ["MM88", "M88"],
        "llwin": ["LLWIN", "LLW", "LL"],
        "xx88": ["XX88", "XX"],
        "o8": ["O8"],
        "qq88": ["QQ88", "QQ"],
        "shbet": ["SHBET", "SH"],
        "jun88": ["JUN88", "JUN"],
        "789bet": ["789BET", "789B"],
        "hi88": ["HI88"],
        "f8bet": ["F8BET", "F8"],
        "mb66": ["MB66"],
    }

    COMMON_WORDS = [
        "CHUC", "MUNG", "HOM", "NAY", "TANG", "LIXI", "NHAN", "THUONG",
        "DANG", "NHAP", "THAM", "GIA", "LINK", "GAME", "RUT", "NAP",
        "TIEN", "TAI", "KHOAN", "KHUYEN", "MAI", "DANGKY", "THANHCONG",
        "NHOM", "KENH", "ADMIN", "HOTRO", "CSKH", "ZALO", "TELE",
        "DANGNHAP", "MATKHAU", "LIENHE", "TRANGCHU", "NHACAI", "UYTIN",
        "BAOTRI", "NOHU", "BANCA", "THETHAO", "CASINO", "LODE", "XOSO",
        "KHONG", "DUOC", "HAY", "THOI", "GIAI", "TRI", "MUC", "VIP",
        "THEO", "DOI", "CHIA", "LIKE", "SHARE", "YOUTUBE",
        "MINIGAME", "O8THETHAO", "BONGDA", "TRUYCAP", "GIFTCODE", "EVENT",
        "FACEBOOK", "TELEGRAM", "TIKTOK", "WEBSITE", "OFFICIAL", "CHANNEL",
        "QUATANG", "BOT", "CHECK", "FREE", "ONLINE", "DAILY", "CLIP",
        "NHANH", "NHANHTAY", "ANH", "EM", "DUNG", "BO", "LO", "JACKPOT",
        "CANHBAO", "GIAMAO", "THONGBAO", "DANGNHAP", "BAOMAT", "KIEMTRA",
        "TAIDAY", "THONGTIN", "HOTLINE", "SUPPORT", "CHAT", "POST",
        "VIEW", "COMMENT", "PINNED", "SUBSCRIBE", "JOIN", "GROUP",
        "DANHSACH", "DIEUKIEN", "HUONGDAN", "KETQUA", "CHUCDANH", "PHAT",
        "XEMNGAY", "CLICK", "LOGIN", "PASSWORD", "TAIKHOAN", "KHUYENMAI",
    ]

    VIETNAMESE_TEXT_WORDS = [
        "khong", "dung", "nhanh", "tang", "code", "free", "clip", "vui",
        "dang", "nhap", "dangky", "truycap", "chinh", "thuc", "kenh",
        "thong", "bao", "canh", "gia", "mao", "kiem", "tra", "duong",
        "link", "facebook", "tiktok", "telegram", "zalo", "website",
        "hom", "nay", "anh", "em", "nhan", "qua", "thuong", "jackpot",
        "may", "man", "don", "cho", "chat", "bot", "cskh", "hotro",
        "lienhe", "taiday", "dangnhap", "matkhau", "taikhoan",
    ]

    FAKE_CODE_PATTERNS = [
        r"^(TEST|DEMO|EXAMPLE|FAKE|SAMPLE)",
        r"^(ABC|DEF|GHI|JKL|MNO|PQR|STU|VWX|YZ)$",
        r"^(123|456|789|000|111|222|333|444|555|666|777|888|999)$",
        r"^(AAAA|BBBB|CCCC|DDDD|EEEE|FFFF|GGGG|HHHH|IIII|JJJJ)$",
    ]

    SOFT_BLACKLIST = {"CODE", "GAME", "FREE", "VIP", "NAP", "RUT"}
    HARD_BLACKLIST = {
        "HTTP", "HTTPS", "WWW", "FACEBOOK", "TELEGRAM", "TIKTOK", "ZALO",
        "CHECK", "CLIP", "DAILY", "TRUYCAP", "BANCA", "NOHU", "ONLINE",
        "GIFTCODE", "MINIGAME", "THETHAO", "O8THETHAO", "BONGDA", "TROLL",
        "SUPPORT", "HOTLINE", "CSKH",
    }

    @staticmethod
    def clean_code(code):
        if not code:
            return ""
        return re.sub(r"[^a-zA-Z0-9]", "", str(code)).strip()

    @staticmethod
    def calculate_entropy(code):
        if not code:
            return 0.0

        char_freq = {}
        for char in code:
            char_freq[char] = char_freq.get(char, 0) + 1

        entropy = 0.0
        code_len = len(code)

        for freq in char_freq.values():
            p = freq / code_len
            if p > 0:
                entropy -= p * math.log2(p)

        return entropy

    @classmethod
    def get_filter_group(cls, target_url="", filter_group_name=None):
        groups = getattr(Config, "CODE_FILTER_GROUPS", {}) or {}

        if filter_group_name and filter_group_name in groups:
            return filter_group_name, groups[filter_group_name]

        target_lower = (target_url or "").lower()
        for group_name, group_config in groups.items():
            if group_name == "default":
                continue
            keywords = group_config.get("url_keywords", [])
            if any(str(keyword).lower() in target_lower for keyword in keywords):
                return group_name, group_config

        return "default", groups.get("default", {})

    @staticmethod
    def get_special_chars(group_config=None):
        group_config = group_config or {}
        return str(group_config.get("special_chars") or getattr(Config, "SPECIAL_CODE_CHARS_30", ""))

    @classmethod
    def count_special_chars(cls, raw_code, group_config=None):
        special_chars = set(cls.get_special_chars(group_config))
        return sum(1 for char in str(raw_code or "") if char in special_chars)

    @staticmethod
    def is_sequential_code(code):
        if not code:
            return True

        upper = code.upper()

        if len(set(upper)) <= 2 and len(upper) >= 6:
            return True

        if len(code) >= 4:
            pattern_1 = code[:1]
            if pattern_1 and pattern_1 * len(code) == code:
                return True

            pattern_2 = code[:2]
            if len(code) % 2 == 0 and pattern_2 * (len(code) // 2) == code:
                return True

            pattern_3 = code[:3]
            if len(code) % 3 == 0 and pattern_3 * (len(code) // 3) == code:
                return True

        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        if re.fullmatch(r"[A-Z]+", upper) and len(upper) >= 4:
            if upper in alphabet or upper in alphabet[::-1]:
                return True

        digits = "0123456789"
        if code.isdigit() and len(code) >= 4:
            if code in digits or code in digits[::-1]:
                return True

        return False

    @staticmethod
    def looks_like_domain_or_link(code):
        upper = code.upper()

        if upper.startswith(("HTTP", "HTTPS", "WWW", "TME", "TELEGRAM", "FACEBOOK", "TIKTOK")):
            return True

        if upper.endswith(("COM", "NET", "ORG", "VN", "APP", "INFO")) and len(upper) <= 14:
            return True

        if any(fragment in upper for fragment in ("DOTCOM", "CHAMCOM", "COMVN", "NETVN")):
            return True

        return False

    @classmethod
    def detect_site_identity(cls, clean_code):
        clean_upper = clean_code.upper()

        for site_key, prefixes in cls.SITE_ROUTING_RULES.items():
            if any(clean_upper.startswith(prefix) for prefix in prefixes):
                return site_key

        return None

    @classmethod
    def has_code_shape(cls, code, group_config=None):
        if not code:
            return False

        group_config = group_config or {}
        length = len(code)

        min_len = int(group_config.get("min_clean_length", getattr(Config, "CODE_MIN_LENGTH", 6)) or 6)
        max_len = int(group_config.get("max_clean_length", getattr(Config, "CODE_MAX_LENGTH", 15)) or 15)

        if length < min_len:
            return False

        if length > max_len:
            return False

        if bool(group_config.get("require_uppercase", False)) and code != code.upper():
            return False

        has_lower = any(c.islower() for c in code)
        has_upper = any(c.isupper() for c in code)
        has_digit = any(c.isdigit() for c in code)
        has_letter = any(c.isalpha() for c in code)
        entropy = cls.calculate_entropy(code)
        min_entropy = float(group_config.get("min_entropy", 2.3))
        uppercase_min_entropy = float(group_config.get("uppercase_min_entropy", 2.9))
        allow_numeric = bool(group_config.get("allow_numeric", True))
        allow_random_mix = bool(group_config.get("allow_random_mix", True))

        if cls.detect_site_identity(code):
            return entropy >= min_entropy and not cls.is_sequential_code(code)

        if code.isdigit():
            return allow_numeric and 8 <= length <= 12 and entropy >= 2.6 and not cls.is_sequential_code(code)

        if not has_letter:
            return False

        if has_upper and has_lower:
            return allow_random_mix and entropy >= min_entropy

        if has_letter and has_digit:
            return entropy >= min_entropy

        if code.isupper() and length >= min_len and entropy >= uppercase_min_entropy:
            return True

        return False

    @classmethod
    def is_text_word(cls, code):
        lower = code.lower()
        upper = code.upper()

        if lower in cls.VIETNAMESE_TEXT_WORDS:
            return True

        if upper in cls.COMMON_WORDS:
            return True

        if code.islower() and not any(c.isdigit() for c in code):
            return True

        if code.isupper() and not any(c.isdigit() for c in code):
            if upper in cls.COMMON_WORDS:
                return True
            # Chỉ chặn nếu entropy thấp (trông như chữ thật, không phải code ngẫu nhiên)
            # Trước đây chặn toàn bộ uppercase <= 7 ký tự -> miss nhiều code hợp lệ
            if len(code) <= 5:
                return True
            if len(code) <= 7 and cls.calculate_entropy(code) < 2.4:
                return True

        return False

    @classmethod
    def contains_blacklisted_fragment(cls, code, group_config=None):
        group_config = group_config or {}
        code_upper = code.upper()
        config_blacklist = {str(item).upper() for item in getattr(Config, "CODE_BLACKLIST", []) if str(item).strip()}
        group_soft_blacklist = {str(item).upper() for item in group_config.get("soft_blacklist", []) if str(item).strip()}

        soft_blacklist = cls.SOFT_BLACKLIST | group_soft_blacklist | (config_blacklist & cls.SOFT_BLACKLIST)
        hard_blacklist = cls.HARD_BLACKLIST | (config_blacklist - soft_blacklist)

        for word in hard_blacklist:
            if not word:
                continue
            if word == code_upper:
                return True
            if len(word) >= 5 and word in code_upper and len(code_upper) <= len(word) + 4:
                return True

        has_digit = any(c.isdigit() for c in code)
        has_lower = any(c.islower() for c in code)
        has_upper = any(c.isupper() for c in code)

        for word in soft_blacklist:
            if not word:
                continue
            if word == code_upper:
                return True
            if word in code_upper and len(code_upper) <= len(word) + 2 and not (has_digit or (has_lower and has_upper)):
                return True

        return False

    @classmethod
    def is_site_allowed_for_group(cls, site_identity, group_config):
        if not site_identity:
            return True

        allowed_sites = group_config.get("allowed_sites", []) if group_config else []
        if not allowed_sites:
            return True

        return site_identity in {str(site).lower() for site in allowed_sites}

    @classmethod
    def is_likely_fake(cls, code, group_config=None):
        code_upper = code.upper()
        group_config = group_config or {}

        if not cls.has_code_shape(code, group_config):
            return True

        for fake_pattern in cls.FAKE_CODE_PATTERNS:
            if re.match(fake_pattern, code_upper):
                return True

        if cls.is_sequential_code(code):
            return True

        if cls.looks_like_domain_or_link(code):
            return True

        if cls.is_text_word(code):
            return True

        if cls.contains_blacklisted_fragment(code, group_config):
            return True

        for word in cls.COMMON_WORDS:
            # Chỉ chặn nếu code gần như chính là word đó và không có digit thêm vào
            # Trước đây buffer +3 quá nhỏ -> chặn nhầm code như "NAPXYZ", "VIP8K9L"
            if word in code_upper and len(code_upper) <= len(word) + 2:
                has_digit_in_code = any(c.isdigit() for c in code_upper)
                if not has_digit_in_code:
                    return True

        entropy = cls.calculate_entropy(code)
        min_entropy = float(group_config.get("min_entropy", 2.3))

        return entropy < min_entropy

    @classmethod
    def validate_code(cls, code, target_url="", filter_group_name=None, source="normal"):
        raw_code = str(code or "").strip()
        clean_code = cls.clean_code(raw_code)
        group_name, group_config = cls.get_filter_group(target_url, filter_group_name)
        special_count = cls.count_special_chars(raw_code, group_config)

        result = {
            "valid": False,
            "confidence": 0.0,
            "reason": "",
            "is_fake": False,
            "entropy": 0.0,
            "recommendation": "SKIP",
            "clean_code": clean_code,
            "raw_code": raw_code,
            "filter_group": group_name,
            "special_count": special_count,
            "source": source,
        }

        min_len = int(group_config.get("min_clean_length", getattr(Config, "CODE_MIN_LENGTH", 6)) or 6)
        max_len = int(group_config.get("max_clean_length", getattr(Config, "CODE_MAX_LENGTH", 15)) or 15)

        if len(clean_code) < min_len or len(clean_code) > max_len:
            result["reason"] = f"❌ Độ dài không hợp lệ: {len(clean_code)}"
            return result

        # Không bắt buộc dấu đặc biệt với spoiler/marker.
        min_special_chars = 0
        if source not in ("spoiler", "marker"):
            min_special_chars = int(group_config.get("min_special_chars", 0) or 0)

        if min_special_chars > 0 and special_count < min_special_chars:
            result["reason"] = f"🚫 Không đủ dấu đặc biệt: {special_count}/{min_special_chars} ({group_name})"
            return result

        if bool(group_config.get("require_uppercase", False)) and clean_code != clean_code.upper():
            result["reason"] = f"🚫 Mã không viết hoa đúng chuẩn ({group_name})"
            return result

        target_lower = target_url.lower() if target_url else ""
        site_identity = cls.detect_site_identity(clean_code)
        clean_upper = clean_code.upper()

        if group_name == "new88":
            if clean_upper.startswith(("NEW88", "N88", "NEW")):
                result["reason"] = f"🚫 NEW88 prefix giống chữ quảng cáo, không nhận là code ({group_name})"
                return result

            promo_fragments = [
                "TROLAI",
                "TRLAI",
                "XTL",
                "NOHU",
                "BANCA",
                "MIENPHI",
                "KHUYENMAI",
                "PHATCODE",
                "GIFTCODE",
                "EVENT",
                "FREECODE",
            ]

            if any(fragment in clean_upper for fragment in promo_fragments):
                result["reason"] = f"🚫 Dính chữ quảng cáo NEW88: {clean_upper}"
                return result


        if site_identity and not cls.is_site_allowed_for_group(site_identity, group_config):
            result["reason"] = f"🛡️ Code [{site_identity.upper()}] không thuộc nhóm lọc [{group_name}]"
            return result

        if site_identity and target_lower and site_identity not in target_lower:
            result["reason"] = (
                f"🛡️ CHỐNG NHẬP SAI: Code [{site_identity.upper()}] "
                f"không thuộc trang [{target_url}]"
            )
            return result

        entropy = cls.calculate_entropy(clean_code)
        result["entropy"] = round(entropy, 2)

        if cls.is_likely_fake(clean_code, group_config):
            result["is_fake"] = True
            result["reason"] = f"🚫 Nhận diện là chữ quảng cáo / link / text rác ({group_name})"
            return result

        min_entropy = float(group_config.get("min_entropy", 2.3))
        has_lower = any(c.islower() for c in clean_code)
        has_upper = any(c.isupper() for c in clean_code)
        has_digit = any(c.isdigit() for c in clean_code)

        if site_identity:
            result["valid"] = True
            result["confidence"] = 1.0
            result["reason"] = f"🌟 Code có định danh {site_identity.upper()} hợp lệ ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if clean_code.isdigit() and bool(group_config.get("allow_numeric", True)) and entropy >= 2.6:
            result["valid"] = True
            result["confidence"] = 0.9
            result["reason"] = f"✅ Code hợp lệ dạng toàn số ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if has_lower and has_upper and entropy >= min_entropy:
            result["valid"] = True
            result["confidence"] = 0.95
            result["reason"] = f"✅ Code hợp lệ dạng mix chữ hoa/thường ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if has_digit and entropy >= min_entropy:
            result["valid"] = True
            result["confidence"] = 0.92
            result["reason"] = f"✅ Code hợp lệ dạng có số ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        if entropy >= float(group_config.get("uppercase_min_entropy", 2.9)):
            result["valid"] = True
            result["confidence"] = 0.85
            result["reason"] = f"✅ Code hợp lệ độ ngẫu nhiên cao ({group_name})"
            result["recommendation"] = "SUBMIT"
            return result

        result["reason"] = f"🚫 Không đủ đặc điểm code thật ({group_name})"
        return result