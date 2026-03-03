from fastapi import APIRouter
from backend.core.stt_service import transcribe_audio
from backend.core.mapping_service import map_raw_words
from backend.core.ai_enrich_service import enrich_words_with_ai

router = APIRouter()

@router.post("/process")
def process_audio(file_path: str):

    raw_words = transcribe_audio(file_path)
    mapped_words = map_raw_words(raw_words)
    enriched_words = enrich_words_with_ai(mapped_words)

    return {
        "total_words": len(enriched_words),
        "words": enriched_words
    }