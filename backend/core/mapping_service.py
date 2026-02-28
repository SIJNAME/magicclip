def map_raw_words(raw_words, pc_id: int = 101, speaker: str = "Narrator"):

    mapped = []

    for idx, w in enumerate(raw_words):
        mapped.append({
            "id": idx + 1,
            "pcId": pc_id,
            "word": w["word"],
            "startTime": w["start"],
            "endTime": w["end"],
            "type": "word" if w["word"].isalnum() else "punctuation",
            "speaker": speaker,
            "important": False,
            "emoji": None,
            "clipDisabled": False
        })

    return mapped


def map_words_to_segments(raw_words, pc_id: int = 101, speaker: str = "Narrator"):
    return map_raw_words(raw_words, pc_id=pc_id, speaker=speaker)
