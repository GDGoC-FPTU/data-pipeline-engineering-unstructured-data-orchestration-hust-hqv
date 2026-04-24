import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================

def process_pdf_data(raw_json: dict) -> dict:
    # Bước 1: Làm sạch nhiễu (Header/Footer) khỏi văn bản
    raw_text = raw_json.get("extractedText", "") or ""
    cleaned_content = re.sub(r"HEADER_PAGE_\d+", "", raw_text)
    cleaned_content = re.sub(r"FOOTER_PAGE_\d+", "", cleaned_content)
    cleaned_content = cleaned_content.strip()

    # Bước 2: Map dữ liệu thô sang định dạng chuẩn của UnifiedDocument
    return {
        "document_id": raw_json.get("docId", "") or "",
        "source_type": "PDF",
        "author": (raw_json.get("authorName") or "").strip(),
        "category": (raw_json.get("docCategory") or "").strip(),
        "content": cleaned_content,
        "timestamp": raw_json.get("createdAt", "") or "",
    }

def process_video_data(raw_json: dict) -> dict:
    # Map dữ liệu thô từ Video sang định dạng chuẩn (giống PDF)
    # Các key của Video: video_id, creator_name, transcript, category, published_timestamp
    return {
        "document_id": raw_json.get("video_id", "") or "",
        "source_type": "VIDEO",
        "author": (raw_json.get("creator_name") or "").strip(),
        "category": (raw_json.get("category") or "").strip(),
        "content": (raw_json.get("transcript") or "").strip(),
        "timestamp": raw_json.get("published_timestamp", "") or "",
    }
