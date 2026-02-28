import os
import subprocess
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

def extract_audio(video_path: str) -> str:
    audio_path = "extracted_audio.mp3"

    subprocess.run([
        "ffmpeg",
        "-i", video_path,
        "-q:a", "0",
        "-map", "a",
        audio_path,
        "-y"
    ])

    return audio_path