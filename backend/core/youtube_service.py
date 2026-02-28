from pathlib import Path

import yt_dlp


def download_youtube_video(url: str, output_dir: str = "storage/videos", audio_only: bool = True) -> str:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if audio_only:
        ydl_opts = {
            "format": "bestaudio/best",
            "outtmpl": str(out_dir / "%(id)s_%(title).120B.%(ext)s"),
            "noplaylist": True,
            "quiet": True,
            "postprocessors": [
                {
                    "key": "FFmpegExtractAudio",
                    "preferredcodec": "mp3",
                    "preferredquality": "128",
                }
            ],
        }
    else:
        ydl_opts = {
            "format": "bestvideo+bestaudio/best",
            "outtmpl": str(out_dir / "%(id)s_%(title).120B.%(ext)s"),
            "merge_output_format": "mp4",
            "noplaylist": True,
            "quiet": True,
        }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=True)
        file_path = Path(ydl.prepare_filename(info))

        if audio_only:
            mp3_path = file_path.with_suffix(".mp3")
            if mp3_path.exists():
                return str(mp3_path)

            fallback = sorted(out_dir.glob(f"{info.get('id', '')}_*.mp3"))
            if fallback:
                return str(fallback[-1])
        else:
            # If yt-dlp merged streams, the final file is usually mp4.
            merged_mp4 = file_path.with_suffix(".mp4")
            if merged_mp4.exists():
                return str(merged_mp4)

        return str(file_path)
