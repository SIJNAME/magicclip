import os
import subprocess
from pathlib import Path

from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def transcribe_audio(file_path: str):
    with open(file_path, "rb") as file:
        transcription = client.audio.transcriptions.create(
            file=(file_path, file.read()),
            model="whisper-large-v3",
            temperature=0,
            response_format="verbose_json",
            timestamp_granularities=["word"]
        )

    return transcription.words  # raw word-level data

def extract_audio(video_path: str, output_path: str | None = None) -> str:
    if output_path:
        audio_path = output_path
    else:
        source = Path(video_path)
        audio_path = str(source.with_suffix(".mp3"))

    subprocess.run([
        "ffmpeg",
        "-i", video_path,
        "-q:a", "0",
        "-map", "a",
        audio_path,
        "-y"
    ], check=True)

    return audio_path
