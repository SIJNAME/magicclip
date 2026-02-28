import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from utils.prompt_builder import build_clip_prompt

load_dotenv()

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def generate_clip_suggestions(transcript_segments):

    prompt = build_clip_prompt(transcript_segments)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a professional short-form video strategist."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7,
        response_format={"type": "json_object"}
    )

    content = response.choices[0].message.content

    try:
        parsed = json.loads(content)

        if "clips" not in parsed:
            raise ValueError("Missing 'clips' key")

        return parsed["clips"]

    except Exception as e:
        print("AI JSON ERROR:", e)
        print("RAW RESPONSE:", content)
        return []