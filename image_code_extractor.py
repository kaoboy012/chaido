"""
🖼️ IMAGE CODE EXTRACTOR - OCR MODULE (LIGHT & FAST)
Trích xuất mã code từ hình ảnh - Cấu hình nhẹ, nhanh, chuẩn
- OCR trực tiếp, không tiền xử lý phức tạp
- Tối ưu cho speed, không cần GPU
- Hỗ trợ đa ngôn ngữ
"""

import os
import pytesseract
from pathlib import Path
from logger_setup import logger


class ImageCodeExtractor:
    """Trích xuất code từ hình ảnh - Phiên bản nhẹ & nhanh"""
    
    def __init__(self):
        """Khởi tạo OCR extractor"""
        self.supported_formats = ['.png', '.jpg', '.jpeg', '.bmp', '.gif', '.tiff', '.webp']
        self._check_tesseract()
        logger.info("✅ ImageCodeExtractor ready (lightweight version)")
    
    def _check_tesseract(self):
        """Kiểm tra Tesseract đã cài hay chưa"""
        try:
            pytesseract.get_tesseract_version()
            logger.debug("✅ Tesseract OCR available")
        except Exception as e:
            logger.error(
                f"❌ Tesseract not installed!\n"
                f"   Windows: https://github.com/UB-Mannheim/tesseract/wiki\n"
                f"   Linux: sudo apt-get install tesseract-ocr\n"
                f"   macOS: brew install tesseract"
            )
            raise
    
    def extract_code_from_image(self, image_path: str, lang: str = "eng") -> str:
        """
        Trích xuất code từ ảnh — có tiền xử lý để tăng độ chính xác.

        Args:
            image_path: Đường dẫn file ảnh
            lang: Ngôn ngữ OCR ('eng', 'vie', v.v.)

        Returns:
            Text đã OCR (chuỗi rỗng nếu không có)
        """
        try:
            image_path = Path(image_path)

            if not image_path.exists():
                logger.warning(f"⚠️ File not found: {image_path}")
                return ""

            if image_path.suffix.lower() not in self.supported_formats:
                logger.warning(f"⚠️ Format not supported: {image_path.suffix}")
                return ""

            logger.info(f"📸 OCR image: {image_path.name}")

            # ── Thử với tiền xử lý ảnh (Pillow) ──────────────────────────────
            try:
                from PIL import Image, ImageFilter, ImageEnhance
                img = Image.open(str(image_path)).convert("L")  # grayscale
                # Tăng độ tương phản
                img = ImageEnhance.Contrast(img).enhance(2.0)
                # Sharpen nhẹ
                img = img.filter(ImageFilter.SHARPEN)

                # Thử OCR trên ảnh đã xử lý
                custom_config = r"--oem 3 --psm 6"
                text = pytesseract.image_to_string(img, lang=lang, config=custom_config)
                cleaned = self._clean_text(text)

                if not cleaned and lang == "eng":
                    # Fallback: thử psm 11 (sparse text — tốt cho code lẻ tẻ)
                    custom_config2 = r"--oem 3 --psm 11"
                    text2 = pytesseract.image_to_string(img, lang=lang, config=custom_config2)
                    cleaned = self._clean_text(text2)

            except ImportError:
                # Pillow không có → OCR thẳng
                custom_config = r"--oem 3 --psm 6"
                text = pytesseract.image_to_string(str(image_path), lang=lang, config=custom_config)
                cleaned = self._clean_text(text)

            if cleaned:
                logger.info(f"✅ Extracted: {len(cleaned)} chars")
            else:
                logger.warning("⚠️ No text detected in image")

            return cleaned

        except Exception as e:
            logger.error(f"❌ OCR error: {e}")
            return ""
    
    def _clean_text(self, text: str) -> str:
        """Làm sạch text: xóa dòng trống, khoảng trắng thừa"""
        if not text:
            return ""
        
        # Xóa dòng trống
        lines = [line.strip() for line in text.split('\n') if line.strip()]
        
        # Nối lại
        return '\n'.join(lines)
    
    def extract_codes_from_image(self, image_path: str, lang: str = "eng") -> list:
        """
        Trích xuất nhiều code từ ảnh (mỗi dòng là 1 code)
        
        Returns:
            List của các codes
        """
        text = self.extract_code_from_image(image_path, lang)
        
        if not text:
            return []
        
        # Chia theo dòng
        codes = [line.strip() for line in text.split('\n') if line.strip()]
        return codes
    
    def extract_code_from_multiple(self, image_paths: list, lang: str = "eng") -> dict:
        """
        Trích xuất từ nhiều ảnh
        
        Args:
            image_paths: List đường dẫn ảnh
            lang: Ngôn ngữ OCR
        
        Returns:
            Dict {tên_file: code}
        """
        results = {}
        for path in image_paths:
            code = self.extract_code_from_image(path, lang)
            results[Path(path).name] = code
        return results
    
    def extract_and_validate(self, image_path: str, min_length: int = 4, max_length: int = 50, lang: str = "eng") -> dict:
        """
        Trích xuất + validate code từ ảnh
        
        Args:
            image_path: Đường dẫn ảnh
            min_length: Độ dài tối thiểu code
            max_length: Độ dài tối đa code
            lang: Ngôn ngữ OCR
        
        Returns:
            {
                'success': bool,
                'code': str,
                'confidence': float (0-1),
                'message': str
            }
        """
        code = self.extract_code_from_image(image_path, lang)
        
        if not code:
            return {
                'success': False,
                'code': '',
                'confidence': 0.0,
                'message': '❌ Không OCR được code'
            }
        
        # Nếu quá ngắn
        if len(code) < min_length:
            return {
                'success': False,
                'code': code,
                'confidence': 0.3,
                'message': f'❌ Code quá ngắn: {len(code)} ký tự (cần {min_length})'
            }
        
        # Nếu quá dài (khả năng là text rác)
        if len(code) > max_length:
            return {
                'success': False,
                'code': code,
                'confidence': 0.2,
                'message': f'❌ Quá dài ({len(code)} ký tự): có thể là quảng cáo'
            }
        
        return {
            'success': True,
            'code': code,
            'confidence': 0.95,
            'message': f'✅ OK: {len(code)} ký tự'
        }
    
    def extract_with_confidence(self, image_path: str, lang: str = "eng") -> dict:
        """
        Trích xuất code với mức độ tin cậy
        
        Returns:
            {
                'code': str,
                'confidence': float (0-1),
                'char_count': int,
                'line_count': int
            }
        """
        code = self.extract_code_from_image(image_path, lang)
        
        if not code:
            return {
                'code': '',
                'confidence': 0.0,
                'char_count': 0,
                'line_count': 0
            }
        
        lines = code.split('\n')
        char_count = len(code)
        line_count = len(lines)
        
        # Tính mức độ tin cậy dựa trên độ dài
        if 4 <= char_count <= 50:
            confidence = 0.95
        elif 50 < char_count <= 100:
            confidence = 0.80
        elif char_count > 100:
            confidence = 0.60
        else:
            confidence = 0.30
        
        return {
            'code': code,
            'confidence': confidence,
            'char_count': char_count,
            'line_count': line_count
        }


# Global instance
_image_extractor = None

def init_image_extractor() -> ImageCodeExtractor:
    """Khởi tạo image extractor"""
    global _image_extractor
    if _image_extractor is None:
        try:
            _image_extractor = ImageCodeExtractor()
        except Exception as e:
            logger.error(f"❌ Cannot init image extractor: {e}")
            return None
    return _image_extractor

def get_image_extractor() -> ImageCodeExtractor:
    """Lấy image extractor instance"""
    global _image_extractor
    if _image_extractor is None:
        _image_extractor = init_image_extractor()
    return _image_extractor
