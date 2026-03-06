from src.pipeline.service import run_pipeline


def process_pipeline_job(job_id: str) -> None:
    run_pipeline(job_id)

