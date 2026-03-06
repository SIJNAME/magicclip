import subprocess


def render_clip(input_path: str, output_path: str, start: float, end: float) -> None:
    if end <= start:
        raise ValueError("Invalid render time range")
    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(start),
        "-to",
        str(end),
        "-i",
        input_path,
        "-vf",
        "scale=1080:1920:flags=lanczos,setsar=1",
        "-c:v",
        "libx264",
        "-preset",
        "medium",
        "-crf",
        "21",
        "-c:a",
        "aac",
        "-movflags",
        "+faststart",
        output_path,
    ]
    subprocess.run(cmd, check=True, capture_output=True, text=True)

