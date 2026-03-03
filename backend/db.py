import sqlite3
from pathlib import Path
from typing import Iterable

BASE_DIR = Path(__file__).resolve().parent
STORAGE_DIR = BASE_DIR / "storage"
DB_PATH = STORAGE_DIR / "magicclip.db"


def get_connection() -> sqlite3.Connection:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _run_statements(statements: Iterable[str]) -> None:
    with get_connection() as conn:
        for statement in statements:
            conn.execute(statement)
        conn.commit()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def init_db() -> None:
    _run_statements(
        [
            """
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_url TEXT,
                input_file TEXT,
                video_file TEXT,
                audio_file TEXT,
                status TEXT NOT NULL,
                transcript_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS clips (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                start_time REAL NOT NULL,
                end_time REAL NOT NULL,
                score INTEGER NOT NULL,
                title TEXT NOT NULL,
                summary TEXT NOT NULL,
                scoring_breakdown_json TEXT,
                retention_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS exports (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                clip_id TEXT,
                status TEXT NOT NULL,
                output_path TEXT,
                format TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY (clip_id) REFERENCES clips(id) ON DELETE SET NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS clip_performance (
                id TEXT PRIMARY KEY,
                clip_id TEXT NOT NULL,
                project_id TEXT NOT NULL,
                avg_watch_time REAL,
                completion_rate REAL,
                rewatch_rate REAL,
                engagement_score REAL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (clip_id) REFERENCES clips(id) ON DELETE CASCADE,
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE
            )
            """,

            """
            CREATE TABLE IF NOT EXISTS video_render_jobs (
                id TEXT PRIMARY KEY,
                project_id TEXT NOT NULL,
                clip_id TEXT NOT NULL,
                input_file TEXT NOT NULL,
                output_file TEXT NOT NULL,
                start REAL NOT NULL,
                end REAL NOT NULL,
                mp4_options_json TEXT,
                status TEXT NOT NULL,
                logs TEXT,
                progress REAL DEFAULT 0,
                eta_seconds REAL DEFAULT 0,
                retries INTEGER DEFAULT 0,
                max_retries INTEGER DEFAULT 2,
                render_time_sec REAL,
                output_file_size INTEGER,
                encoding_params_json TEXT,
                callback_url TEXT,
                output_url TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY (clip_id) REFERENCES clips(id) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS scoring_weight_history (
                id TEXT PRIMARY KEY,
                weights_json TEXT NOT NULL,
                mse REAL,
                created_at TEXT NOT NULL
            )
            """,
        ]
    )

    with get_connection() as conn:
        _ensure_column(conn, "clips", "scoring_breakdown_json", "scoring_breakdown_json TEXT")
        _ensure_column(conn, "clips", "retention_json", "retention_json TEXT")
        _ensure_column(conn, "video_render_jobs", "progress", "progress REAL DEFAULT 0")
        _ensure_column(conn, "video_render_jobs", "eta_seconds", "eta_seconds REAL DEFAULT 0")
        _ensure_column(conn, "video_render_jobs", "retries", "retries INTEGER DEFAULT 0")
        _ensure_column(conn, "video_render_jobs", "max_retries", "max_retries INTEGER DEFAULT 2")
        _ensure_column(conn, "video_render_jobs", "render_time_sec", "render_time_sec REAL")
        _ensure_column(conn, "video_render_jobs", "output_file_size", "output_file_size INTEGER")
        _ensure_column(conn, "video_render_jobs", "encoding_params_json", "encoding_params_json TEXT")
        _ensure_column(conn, "video_render_jobs", "callback_url", "callback_url TEXT")
        _ensure_column(conn, "video_render_jobs", "output_url", "output_url TEXT")
        conn.commit()
